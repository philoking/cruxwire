#!/usr/bin/env python3
"""Ingestion pipeline for Cruxwire.

Ported from the old n8n Code nodes into pure Python stdlib. One run_once():
fetch feeds -> parse RSS/Atom -> filter -> score+summarise+embed via Ollama ->
cluster same-story coverage -> atomically write digest.json.

A background scheduler thread runs it on a cron-like schedule. No external
dependencies; the only network calls are to the feeds, the article pages, and
the Ollama host.
"""
import concurrent.futures
import json
import math
import os
import re
import tempfile
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import corpus_archive
import settings

# ──────────────────────────────────────────────────────────────────────
# CONFIG. Deploy-time wiring (Ollama, ports, paths) stays env-only. The
# tuning knobs below (similarity, boosts, lookback, schedule, blocklist) are
# read from settings.py at runtime so the UI can change them without a
# restart; the env values here are the defaults settings.py falls back to.
# ──────────────────────────────────────────────────────────────────────
OLLAMA_HOST   = os.environ.get('OLLAMA_HOST', 'http://localhost:11434').rstrip('/')
OLLAMA_MODEL  = os.environ.get('OLLAMA_MODEL', 'qwen3:8b')
EMBED_MODEL   = os.environ.get('EMBED_MODEL', 'nomic-embed-text')

SIM_THRESHOLD = float(os.environ.get('SIM_THRESHOLD', '0.82'))
BOOST_CAP     = float(os.environ.get('BOOST_CAP', '1.5'))
BOOST_K       = float(os.environ.get('BOOST_K', '0.6'))

LOOKBACK_HOURS    = int(os.environ.get('LOOKBACK_HOURS', '36'))
MAX_ARTICLES      = int(os.environ.get('MAX_ARTICLES', '1024'))
SCORE_CONCURRENCY = int(os.environ.get('SCORE_CONCURRENCY', '4'))

ACTIVE_START_HOUR = int(os.environ.get('ACTIVE_START_HOUR', '6'))
ACTIVE_END_HOUR   = int(os.environ.get('ACTIVE_END_HOUR', '22'))
INTERVAL_HOURS    = max(1, int(os.environ.get('INTERVAL_HOURS', '2')))

FEEDS_FILE  = os.environ.get('FEEDS_FILE', '/data/feeds.json')
DIGEST_FILE = os.environ.get('DIGEST_FILE', '/data/digest.json')
STATE_FILE  = os.environ.get('STATE_FILE', '/data/state.json')
RUNS_FILE   = os.environ.get('RUNS_FILE', '/data/runs.json')
MAX_RUNS    = int(os.environ.get('MAX_RUNS', '200'))  # bounded run-history log
# Article embeddings for the current digest, kept out of digest.json (which is
# served to the browser) so semantic search can rank without shipping vectors.
EMBEDDINGS_FILE  = os.environ.get('EMBEDDINGS_FILE', '/data/embeddings.json')
SEARCH_THRESHOLD = float(os.environ.get('SEARCH_THRESHOLD', '0.5'))  # min cosine to match

# Taste vector: how many of the most-recent saved/opened items to learn from,
# and how much more a "saved" item counts than an "opened" one.
TASTE_MAX_ITEMS  = int(os.environ.get('TASTE_MAX_ITEMS', '40'))
TASTE_SAVE_WEIGHT = float(os.environ.get('TASTE_SAVE_WEIGHT', '2.0'))

FEED_TIMEOUT   = int(os.environ.get('FEED_TIMEOUT', '12'))
PAGE_TIMEOUT   = int(os.environ.get('PAGE_TIMEOUT', '8'))
OLLAMA_TIMEOUT = int(os.environ.get('OLLAMA_TIMEOUT', '120'))

# Categories (keys + the interest descriptions the scorer's prompt is built
# from) are configured in settings.load_categories(); the run loads them fresh.

USER_AGENT = 'Mozilla/5.0 Cruxwire (news aggregator)'

# Titles matching any of these are dropped (deal/ad spam).
TITLE_BLOCKLIST = re.compile('|'.join([
    r'save \$?\d+',
    r'\d+%\s*off',
    r'\$\d+\s*off',
    r'\bdeal(s)?\b.{0,40}\b(today|now|day|week|month|under|below)\b',
    r'\bbest\b.{0,30}\bdeals?\b',
    r'\bcoupon(s)?\b',
    r'\bpromo\s*code\b',
    r'\bprime day\b',
    r'\bblack friday\b',
    r'\bcyber monday\b',
    r'\bsale\b.{0,20}(ends|now|today)',
    r'\bgiveaway\b',
    r'\bsponsored\b',
]), re.IGNORECASE)

_run_lock = threading.Lock()
_runs_lock = threading.Lock()
_stop = threading.Event()

# Live run status, polled by the UI via GET /status. Updated as run_once moves
# through its phases so the frontend can show an "Updating…" indicator and
# auto-surface a fresh digest when one lands.
_status = {
    'running': False,
    'phase': None,          # fetching | scoring | clustering | None
    'started_at': None,     # ISO, when the current/last run began
    'last_run_at': None,    # ISO, when the last run finished
    'last_ok': None,        # bool, did the last run write a digest
    'article_count': None,  # articles in the last successful digest
    'feed_count': None,     # feeds fetched in the current/last run
}
_status_lock = threading.Lock()


def _set_status(**kw):
    with _status_lock:
        _status.update(kw)


def get_status():
    with _status_lock:
        return dict(_status)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def log(msg):
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f'[pipeline {ts}] {msg}', flush=True)


# ──────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────
def http_get_text(url, timeout, limit=None):
    """GET a URL and return decoded text, or '' on any failure."""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(limit) if limit else resp.read()
        return raw.decode('utf-8', errors='replace')
    except Exception:
        return ''


def ollama_post(path, payload, timeout):
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        OLLAMA_HOST + path, data=data,
        headers={'Content-Type': 'application/json'}, method='POST')
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))


def ollama_ps():
    """Models currently loaded in Ollama (GET /api/ps), or [] on failure. Used
    to tell whether the chat model is resident in VRAM (GPU) or pushed to CPU."""
    try:
        with urllib.request.urlopen(OLLAMA_HOST + '/api/ps', timeout=8) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        models = data.get('models') if isinstance(data, dict) else None
        return models if isinstance(models, list) else []
    except Exception:
        return []


# ──────────────────────────────────────────────────────────────────────
# Feed parsing (hand-rolled, CDATA-aware — same approach as v1)
# ──────────────────────────────────────────────────────────────────────
def clean_text(s):
    s = s.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>') \
         .replace('&quot;', '"').replace('&#39;', "'").replace('&apos;', "'")
    s = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), s)
    return s.strip()


def extract_field(xml, tag):
    for pat in (
        rf'<{tag}[^>]*><!\[CDATA\[([\s\S]*?)\]\]></{tag}>',
        rf'<{tag}[^>]*>([\s\S]*?)</{tag}>',
    ):
        m = re.search(pat, xml, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ''


def parse_date(s):
    """Best-effort parse of RSS (RFC822) or Atom (ISO) dates -> aware datetime."""
    if not s:
        return None
    s = s.strip()
    try:
        dt = parsedate_to_datetime(s)
        if dt is not None:
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def parse_feed(xml, source, category, cutoff):
    out = []

    # RSS <item>
    for m in re.finditer(r'<item>([\s\S]*?)</item>', xml):
        block = m.group(1)
        title = clean_text(extract_field(block, 'title'))
        url = (extract_field(block, 'link') or extract_field(block, 'guid')).strip()
        pub = parse_date(extract_field(block, 'pubDate')
                         or extract_field(block, 'dc:date')) or datetime.now(timezone.utc)
        if not title or not url.startswith('http'):
            continue
        if pub < cutoff:
            continue
        out.append({'title': title, 'url': url, 'source': source,
                    'category': category, 'published_at': pub.isoformat()})

    # Atom <entry>
    for m in re.finditer(r'<entry>([\s\S]*?)</entry>', xml):
        block = m.group(1)
        title = clean_text(extract_field(block, 'title'))
        lm = re.search(r'<link[^>]*href=["\']([^"\']+)["\']', block, re.IGNORECASE)
        url = (lm.group(1) if lm else extract_field(block, 'id')).strip()
        pub = parse_date(extract_field(block, 'published')
                         or extract_field(block, 'updated')) or datetime.now(timezone.utc)
        if not title or not url.startswith('http'):
            continue
        if pub < cutoff:
            continue
        out.append({'title': title, 'url': url, 'source': source,
                    'category': category, 'published_at': pub.isoformat()})

    return out


def stable_id(url):
    """Deterministic 8-char hex id from the URL (stable within a digest)."""
    h = 0
    for ch in url:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return format(h, '08x')[:8]


# ──────────────────────────────────────────────────────────────────────
# Scoring + embedding (Ollama)
# ──────────────────────────────────────────────────────────────────────
def build_system_prompt(categories):
    """The scorer's system prompt, built from the configured categories — the
    `interest` lines (in priority order) are what tune relevance to the reader."""
    interests = "\n".join(f"{i + 1}. {c['interest']}"
                          for i, c in enumerate(categories))
    return ("You are a relevance scorer for a personal news digest.\n\n"
            "Reader interests, roughly in priority order:\n"
            f"{interests}\n\n"
            "Scoring:\n"
            "8-10 = high relevance, reader would likely read in full\n"
            "5-7  = moderate relevance, worth a skim\n"
            "0-4  = low relevance or off-topic\n\n"
            "Rules:\n"
            "- Respond ONLY with valid JSON. No markdown fences, no commentary.\n"
            "- summary must be 1-2 sentences, max 180 characters, no em dashes.\n"
            "- category must be one of the exact strings listed in the schema.")


def _fetch_page(url):
    """Return (og_image_or_None, body_excerpt) for an article URL."""
    html = http_get_text(url, PAGE_TIMEOUT, limit=60000)
    if not html:
        return None, ''
    img_m = re.search(
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        html, re.IGNORECASE) or re.search(
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        html, re.IGNORECASE)
    image = img_m.group(1) if img_m else None
    body = re.sub(r'<script[\s\S]*?</script>', ' ', html, flags=re.IGNORECASE)
    body = re.sub(r'<style[\s\S]*?</style>', ' ', body, flags=re.IGNORECASE)
    body = re.sub(r'<[^>]+>', ' ', body)
    body = re.sub(r'\s+', ' ', body).strip()[:900]
    return image, body


def _score(article, excerpt, chat_model, system_prompt, valid_cats):
    """Ask Ollama for {score, summary, category}; fall back gracefully.

    `system_prompt` and `valid_cats` come from the configured categories.
    Returns (score, summary, category, ok, ms). `ok` is whether Ollama answered
    (False ⇒ the call errored/timed out and the score is a fallback); `ms` is
    the chat-call latency. Both feed the run-history health signals so a silent
    Ollama outage (many fallbacks) or a CPU fallback (high latency) is visible.
    """
    user_prompt = (
        "Score this article and return JSON only.\n\n"
        f"Title:    {article['title']}\n"
        f"Source:   {article['source']}\n"
        f"Hint cat: {article['category']}\n"
        f"Excerpt:  {excerpt or '(not available)'}\n\n"
        "Return this exact schema:\n"
        '{\n  "score": <number 0.0-10.0, one decimal place>,\n'
        '  "summary": "<1-2 sentences, max 180 chars, no em dashes>",\n'
        f'  "category": "<one of: {", ".join(valid_cats)}>"\n}}'
    )
    score, summary, category = 5.0, '', article['category']
    ok = False
    t0 = time.monotonic()
    try:
        resp = ollama_post('/api/chat', {
            'model': chat_model,
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': user_prompt},
            ],
            'stream': False, 'format': 'json',
            # Disable reasoning models' "thinking" (e.g. qwen3): with format=json
            # it burns the num_predict budget on hidden reasoning before the JSON,
            # breaking the output and slowing runs. Harmless no-op on plain models.
            'think': False,
            'options': {'temperature': 0.1, 'num_predict': 250},
        }, OLLAMA_TIMEOUT)
        ok = True  # Ollama answered (parsing may still default the score)
        raw = (resp.get('message') or {}).get('content') or resp.get('response') or ''
        parsed = json.loads(raw if isinstance(raw, str) else json.dumps(raw))
        try:
            score = round(min(10.0, max(0.0, float(parsed.get('score')))), 1)
        except (TypeError, ValueError):
            pass
        if isinstance(parsed.get('summary'), str):
            summary = parsed['summary'].replace('—', '--').strip()[:200]
        if parsed.get('category') in valid_cats:
            category = parsed['category']
    except Exception:
        pass
    ms = (time.monotonic() - t0) * 1000.0
    if not summary:
        summary = article['title'][:160]
    return score, summary, category, ok, ms


def _embed(text, embed_model=EMBED_MODEL):
    """Return an embedding vector for text, or None on failure."""
    try:
        resp = ollama_post('/api/embeddings',
                           {'model': embed_model, 'prompt': text}, OLLAMA_TIMEOUT)
        vec = resp.get('embedding')
        if isinstance(vec, list) and vec:
            return [float(x) for x in vec]
    except Exception:
        pass
    return None


def enrich(article, chat_model, embed_model, system_prompt, valid_cats):
    """Fetch page, score/summarise, and embed one article."""
    image, excerpt = _fetch_page(article['url'])
    score, summary, category, score_ok, score_ms = _score(
        article, excerpt, chat_model, system_prompt, valid_cats)
    embedding = _embed(f"{article['title']}\n{summary}", embed_model)
    return {
        'id': article['id'],
        'title': article['title'],
        'url': article['url'],
        'source': article['source'],
        'category': category,
        'score': score,
        'summary': summary,
        'image': image,
        'published_at': article['published_at'],
        'embedding': embedding,
        '_score_ok': score_ok,           # transient: dropped before the digest is written
        '_score_ms': round(score_ms, 1),
    }


TLDR_SYSTEM = """You write a TL;DR that helps a reader decide whether to open the \
full article. Be specific to THIS article — concrete facts, names, numbers, and \
claims, never generic filler. Each bullet is one short idea (max ~110 chars). The \
bottom line is one sentence on whether it's worth reading and for whom. Respond \
ONLY with valid JSON, no markdown."""


def _article_text(url, limit=6000):
    """Cleaned body text of an article page (more than _fetch_page's excerpt)."""
    html = http_get_text(url, PAGE_TIMEOUT, limit=150000)
    if not html:
        return ''
    body = re.sub(r'<script[\s\S]*?</script>', ' ', html, flags=re.IGNORECASE)
    body = re.sub(r'<style[\s\S]*?</style>', ' ', body, flags=re.IGNORECASE)
    body = re.sub(r'<[^>]+>', ' ', body)
    return re.sub(r'\s+', ' ', body).strip()[:limit]


def generate_tldr(url, title, chat_model=None):
    """Ollama TL;DR for a saved article: {tldr: [bullets], bottom_line: str}.
    Returns None on failure (Ollama down / nothing usable) so the caller can
    surface a retry."""
    chat_model = chat_model or settings.load()['ollama_model']
    text = _article_text(url)
    user_prompt = (
        "Write a TL;DR for someone deciding whether to read this article.\n\n"
        f"Title: {title}\n"
        f"Article text: {text or '(could not fetch the article body — use the title)'}\n\n"
        "Return this exact schema:\n"
        '{\n  "tldr": ["3-5 short bullet points — the key takeaways"],\n'
        '  "bottom_line": "<one sentence: is it worth reading, and for whom>"\n}'
    )
    try:
        resp = ollama_post('/api/chat', {
            'model': chat_model,
            'messages': [
                {'role': 'system', 'content': TLDR_SYSTEM},
                {'role': 'user', 'content': user_prompt},
            ],
            'stream': False, 'format': 'json',
            'think': False,   # see _score: keep reasoning models from eating the budget
            'options': {'temperature': 0.2, 'num_predict': 400},
        }, OLLAMA_TIMEOUT)
        raw = (resp.get('message') or {}).get('content') or resp.get('response') or ''
        parsed = json.loads(raw if isinstance(raw, str) else json.dumps(raw))
    except Exception as exc:
        log(f'tldr failed for {url}: {exc}')
        return None
    bullets = parsed.get('tldr')
    bullets = ([str(b).strip()[:200] for b in bullets if str(b).strip()][:6]
               if isinstance(bullets, list) else [])
    bottom = parsed.get('bottom_line')
    bottom = str(bottom).strip()[:300] if isinstance(bottom, str) else ''
    if not bullets and not bottom:
        return None
    return {'tldr': bullets, 'bottom_line': bottom}


# ──────────────────────────────────────────────────────────────────────
# Clustering
# ──────────────────────────────────────────────────────────────────────
def _normalize(vec):
    norm = math.sqrt(sum(x * x for x in vec))
    if norm == 0:
        return None
    return [x / norm for x in vec]


# ──────────────────────────────────────────────────────────────────────
# Personalization + semantic blocking (both reuse article embeddings)
# ──────────────────────────────────────────────────────────────────────
def _embed_many(texts, embed_model, workers=4):
    """Embed a list of texts concurrently; returns vectors (or None per item)."""
    if not texts:
        return []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(workers, len(texts))) as ex:
        return list(ex.map(lambda t: _embed(t, embed_model), texts))


def _load_user_state():
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as fh:
            d = json.load(fh)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _load_embeddings_index():
    """{article_id: vector} for the current digest, or {} if none yet."""
    try:
        with open(EMBEDDINGS_FILE, 'r', encoding='utf-8') as fh:
            d = json.load(fh)
        vecs = d.get('vectors') if isinstance(d, dict) else None
        return vecs if isinstance(vecs, dict) else {}
    except Exception:
        return {}


def semantic_search(query, threshold=SEARCH_THRESHOLD, limit=80):
    """Rank the current digest's articles by cosine similarity of their stored
    embedding to the query's embedding. Returns [{'id', 'score'}] descending.

    Embeds the query with the same model and the same plain title+summary
    convention the pipeline used, so the query lands in the same vector space.
    """
    query = (query or '').strip()
    if not query:
        return []
    model = settings.load().get('embed_model') or EMBED_MODEL
    qn = _normalize(_embed(query, model) or [])
    if not qn:
        return []
    results = []
    for aid, vec in _load_embeddings_index().items():
        n = _normalize(vec) if vec else None
        if not n:
            continue
        sim = sum(x * y for x, y in zip(qn, n))
        if sim >= threshold:
            results.append({'id': aid, 'score': round(sim, 4)})
    results.sort(key=lambda r: r['score'], reverse=True)
    return results[:limit]


def semantic_block(articles, topics, threshold, embed_model):
    """Drop articles whose embedding is too close to any 'topic to avoid'
    phrase. Keyword/regex blocking already ran earlier; this is the semantic
    layer. Articles without an embedding are kept (can't judge them)."""
    topics = [t for t in (topics or []) if isinstance(t, str) and t.strip()]
    if not topics:
        return articles
    topic_norms = [n for n in (_normalize(v) for v in _embed_many(topics, embed_model)) if n]
    if not topic_norms:
        return articles
    kept, dropped = [], 0
    for a in articles:
        n = _normalize(a['embedding']) if a.get('embedding') else None
        if n is not None and max(sum(x * y for x, y in zip(n, tn)) for tn in topic_norms) > threshold:
            dropped += 1
            continue
        kept.append(a)
    if dropped:
        log(f'semantic blocklist dropped {dropped} article(s)')
    return kept


def apply_taste(articles, embed_model, weight):
    """Annotate each article with a `taste_boost` from how close it is to the
    user's taste — the weighted centroid of embeddings of titles they've saved
    (weighted higher) and opened. weight is the max points an on-taste article
    can gain. No history yet (or weight 0) → every boost is 0."""
    for a in articles:
        a['taste_boost'] = 0.0
    if weight <= 0:
        return articles

    state = _load_user_state()
    later = state.get('later') if isinstance(state.get('later'), list) else []
    history = state.get('history') if isinstance(state.get('history'), list) else []
    samples = []  # (title, weight, recency_key)
    for it in later:
        if isinstance(it, dict) and it.get('title'):
            samples.append((it['title'], TASTE_SAVE_WEIGHT, it.get('savedAt') or ''))
    for it in history:
        if isinstance(it, dict) and it.get('title'):
            samples.append((it['title'], 1.0, it.get('viewedAt') or ''))
    if not samples:
        return articles
    samples.sort(key=lambda s: s[2], reverse=True)   # most recent first
    samples = samples[:TASTE_MAX_ITEMS]

    embs = _embed_many([s[0] for s in samples], embed_model)
    acc, wsum = None, 0.0
    for (_, w, _), emb in zip(samples, embs):
        n = _normalize(emb) if emb else None
        if n is None:
            continue
        if acc is None:
            acc = [0.0] * len(n)
        for i in range(len(n)):
            acc[i] += w * n[i]
        wsum += w
    centroid = _normalize(acc) if acc and wsum else None
    if centroid is None:
        return articles

    n_boosted = 0
    for a in articles:
        n = _normalize(a['embedding']) if a.get('embedding') else None
        if n is None:
            continue
        sim = sum(x * y for x, y in zip(n, centroid))
        if sim > 0:
            a['taste_boost'] = round(weight * sim, 3)
            n_boosted += 1
    log(f'taste vector from {len(samples)} items; boosted {n_boosted} article(s)')
    return articles


def cluster(articles, sim_threshold=SIM_THRESHOLD, boost_cap=BOOST_CAP, boost_k=BOOST_K):
    """Assign cluster_id/size/boost/rep.

    Each story is anchored on its highest-scoring article; another article joins
    that story only if its cosine similarity to the anchor exceeds sim_threshold
    (and, if it is close to several anchors, the nearest one wins). Anchoring on
    the representative -- rather than single-link union-find -- avoids transitive
    "chaining", where A~B and B~C silently merge A and C even when A and C are
    unrelated. That chaining is what collapses a pile of loosely-related articles
    into one bogus mega-cluster. Articles without an embedding stay singletons."""
    n = len(articles)
    norms = [_normalize(a['embedding']) if a.get('embedding') else None
             for a in articles]

    def cosine(u, v):
        return sum(x * y for x, y in zip(u, v))

    # Seed clusters from the highest-scoring articles first, so the strongest
    # article anchors each story; weaker articles attach to the best-matching
    # existing anchor or start their own.
    order = sorted((i for i in range(n) if norms[i] is not None),
                   key=lambda i: articles[i]['score'], reverse=True)
    anchors = []          # anchor indices, in creation (descending-score) order
    members = {}          # anchor index -> list of member indices
    for i in order:
        best_anchor, best_sim = None, sim_threshold
        for a in anchors:
            s = cosine(norms[i], norms[a])
            if s > best_sim:
                best_anchor, best_sim = a, s
        if best_anchor is None:
            anchors.append(i)
            members[i] = [i]
        else:
            members[best_anchor].append(i)

    groups = list(members.values())
    # Articles without an embedding cannot be matched; each is its own story.
    groups.extend([i] for i in range(n) if norms[i] is None)

    for grp in groups:
        size = len(grp)
        # Representative: highest score, tie-break has-image.
        rep = max(grp, key=lambda i: (
            articles[i]['score'], 1 if articles[i].get('image') else 0))
        cid = articles[rep]['id']
        boost = min(boost_cap, boost_k * math.log2(size)) if size > 1 else 0.0
        for i in grp:
            articles[i]['cluster_id'] = cid
            articles[i]['cluster_size'] = size
            articles[i]['cluster_rep'] = (i == rep)
            articles[i]['cluster_boost'] = round(boost, 3) if i == rep else 0.0
    return articles


# ──────────────────────────────────────────────────────────────────────
# Carry-forward + retention
#
# The digest is no longer a fresh 36h snapshot. Unread stories are carried
# forward from the previous digest so a story you never got to doesn't vanish
# just because its source feed rotated it out. The pool is then pruned by a
# rank-weighted lifespan held inside a [floor, ceiling] band: weak stories die
# fast, strong ones linger, the inbox never goes dry or overflows, and a hard
# age cap overrides everything so nothing stale lingers. Read stories are
# dropped (vacated) — History keeps its own self-contained copy, untouched.
# ──────────────────────────────────────────────────────────────────────
def _age_hours(published_at, now=None):
    """Hours since an ISO `published_at`, or None if unparseable."""
    if not isinstance(published_at, str) or not published_at:
        return None
    try:
        dt = datetime.fromisoformat(published_at)
    except ValueError:
        dt = parse_date(published_at)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return (now - dt).total_seconds() / 3600.0


def _load_prev_digest_articles():
    """Articles from the currently-stored digest (for carry-forward), or []."""
    try:
        with open(DIGEST_FILE, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
    except Exception:
        return []
    arts = data.get('articles') if isinstance(data, dict) else None
    return arts if isinstance(arts, list) else []


def _load_read_ids():
    """The user's read article ids — so carry-forward can vacate read stories."""
    raw = _load_user_state().get('readIds')
    return {r for r in raw if isinstance(r, str)} if isinstance(raw, list) else set()


def _reembed(article, embed_model):
    """Refresh a carried article's embedding so it can re-cluster against fresh
    coverage. Its Ollama score/summary are reused (the expensive, less stable
    part); only the embedding — which clustering and taste need — is recomputed."""
    article['embedding'] = _embed(
        f"{article.get('title', '')}\n{article.get('summary', '')}", embed_model)
    return article


def apply_retention(scored, cfg, now=None):
    """Prune the scored pool to a rank-weighted, volume-banded keep set.

    Operates on *stories* (clusters), not raw articles: each story lives for a
    TTL that scales with its rank between [base_ttl, max_ttl], clamped into a
    [floor, ceiling] band so the inbox never goes dry or overflows, with a hard
    age cap that overrides everything. A story's age is its freshest coverage.
    Returns the kept articles, preserving input order.
    """
    now = now or datetime.now(timezone.utc)
    ceiling = max(1, cfg['retain_ceiling'])
    floor = min(cfg['retain_floor'], ceiling)
    base_ttl = cfg['retain_base_ttl_hours']
    max_ttl = max(base_ttl, cfg['retain_max_ttl_hours'])
    hard_max = cfg['retain_hard_max_age_hours']

    def boosted(a):
        return a['score'] + a.get('cluster_boost', 0.0) + a.get('taste_boost', 0.0)

    # Group articles into stories.
    groups = {}
    for a in scored:
        groups.setdefault(a.get('cluster_id', a['id']), []).append(a)

    clusters = []
    for cid, members in groups.items():
        ages = [h for h in (_age_hours(m.get('published_at'), now) for m in members)
                if h is not None]
        clusters.append({
            'cid': cid,
            'age': min(ages) if ages else None,        # freshest coverage = story age
            'rank': max(boosted(m) for m in members),
        })

    # Hard age cap first: drop outright (None age = can't judge, so keep).
    live = [c for c in clusters if c['age'] is None or c['age'] < hard_max]
    live.sort(key=lambda c: c['rank'], reverse=True)   # best first
    m = len(live)

    kept, expired = set(), []
    for i, c in enumerate(live):
        pct = (m - 1 - i) / (m - 1) if m > 1 else 1.0  # 1.0 best … 0.0 worst
        ttl = base_ttl + (max_ttl - base_ttl) * pct
        if c['age'] is None or c['age'] <= ttl:
            kept.add(c['cid'])
        else:
            expired.append(c)                          # stays best-first

    # Ceiling: keep only the top-ranked stories (live is best-first).
    if len(kept) > ceiling:
        kept = set([c['cid'] for c in live if c['cid'] in kept][:ceiling])

    # Floor: backfill the best expired-but-in-window stories until we hit it.
    for c in expired:
        if len(kept) >= floor:
            break
        kept.add(c['cid'])

    return [a for a in scored if a.get('cluster_id', a['id']) in kept]


# ──────────────────────────────────────────────────────────────────────
# Digest assembly
# ──────────────────────────────────────────────────────────────────────
def _atomic_write_json(path, obj):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    d = os.path.dirname(path) or '.'
    fd, tmp = tempfile.mkstemp(prefix='.digest-', suffix='.json', dir=d)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ──────────────────────────────────────────────────────────────────────
# Run history — a bounded log of completed runs, served at GET /runs so the
# UI can show recent executions and surface failures.
# ──────────────────────────────────────────────────────────────────────
def get_runs():
    """The recorded run log (oldest first), or [] if none/unreadable."""
    try:
        with open(RUNS_FILE, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _record_run(rec):
    """Append one completed run to the log, capped to MAX_RUNS (newest kept)."""
    with _runs_lock:
        runs = get_runs()
        runs.append(rec)
        try:
            _atomic_write_json(RUNS_FILE, runs[-MAX_RUNS:])
        except Exception as exc:
            log(f'failed to record run: {exc}')


def load_feeds():
    try:
        with open(FEEDS_FILE, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def run_once():
    """One full pipeline pass. No-op (returns False) if one is already running."""
    if not _run_lock.acquire(blocking=False):
        log('run already in progress; skipping')
        return False
    started = _now_iso()
    _set_status(running=True, phase='fetching', started_at=started, feed_count=None)
    ok = False
    feed_count = new_count = article_count = clusters = 0
    scored_count = score_fallbacks = embed_fallbacks = 0
    avg_score_ms = None
    gpu_ratio = None
    error = None
    try:
        cfg = settings.load()
        user_blocklist = [k.lower() for k in cfg['block_keywords']]
        feeds = load_feeds()
        if not feeds:
            log('no feeds configured; nothing to do')
            return False
        feed_count = len(feeds)
        log(f'fetching {feed_count} feeds')
        _set_status(feed_count=feed_count)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=cfg['lookback_hours'])

        # Fetch feeds concurrently.
        def fetch(feed):
            return http_get_text(feed.get('url', ''), FEED_TIMEOUT), feed

        bodies = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(feeds))) as ex:
            for body, feed in ex.map(fetch, feeds):
                if body:
                    bodies.append((body, feed))

        # Parse + dedup + blocklist.
        seen, fresh = set(), []
        for body, feed in bodies:
            for art in parse_feed(body, feed.get('source', ''),
                                  feed.get('category', 'tech'), cutoff):
                if (art['url'] in seen
                        or TITLE_BLOCKLIST.search(art['title'])
                        or settings.is_blocked(art['title'], user_blocklist)):
                    continue
                seen.add(art['url'])
                art['id'] = stable_id(art['url'])
                fresh.append(art)

        fresh.sort(key=lambda a: a['published_at'], reverse=True)
        fresh = fresh[:cfg['max_articles']]

        # Carry-forward: bring unread stories from the last digest back into the
        # pool so a story you never read doesn't vanish just because its source
        # feed rotated it out. Read stories are vacated; truly stale ones are
        # cut by the hard age cap. Re-fetched stories supersede their carried
        # copy (same id), so each story is enriched at most once.
        read_ids = _load_read_ids()
        fresh = [a for a in fresh if a['id'] not in read_ids]
        fresh_ids = {a['id'] for a in fresh}
        hard_max = cfg['retain_hard_max_age_hours']
        carried = []
        for a in _load_prev_digest_articles():
            if not isinstance(a, dict):
                continue
            aid = a.get('id')
            if not aid or aid in read_ids or aid in fresh_ids:
                continue
            # Re-apply the blocklist to carried stories so a keyword you add
            # later removes a story already in the digest on the next run —
            # otherwise carry-forward would keep showing it indefinitely.
            title = a.get('title', '')
            if TITLE_BLOCKLIST.search(title) or settings.is_blocked(title, user_blocklist):
                continue
            age = _age_hours(a.get('published_at'))
            if age is not None and age >= hard_max:
                continue
            carried.append(a)
        if carried:
            log(f'carrying forward {len(carried)} unread '
                f'stor{"y" if len(carried) == 1 else "ies"}')

        log(f'parsed {len(fresh)} fresh articles; scoring')
        if not fresh and not carried:
            return False
        new_count = len(fresh)

        # Score + embed concurrently, using the UI-selected models. Fresh
        # articles get a full enrich; carried ones only re-embed (their score
        # and summary are reused) so they can re-cluster with fresh coverage.
        _set_status(phase='scoring')
        chat_model, embed_model = cfg['ollama_model'], cfg['embed_model']
        categories = settings.load_categories()
        system_prompt = build_system_prompt(categories)
        valid_cats = [c['key'] for c in categories]

        def _enrich(article):
            return enrich(article, chat_model, embed_model, system_prompt, valid_cats)

        scored = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=cfg['score_concurrency']) as ex:
            scored = list(ex.map(_enrich, fresh))
        if carried:
            with concurrent.futures.ThreadPoolExecutor(max_workers=cfg['score_concurrency']) as ex:
                scored.extend(ex.map(lambda a: _reembed(a, embed_model), carried))

        # Ollama health for this run: fallbacks (Ollama errored → default score),
        # average chat latency (high ≈ running on CPU), and whether the chat
        # model is resident in VRAM. Surfaced in Settings → Runs.
        fresh_scored = [a for a in scored if '_score_ok' in a]
        scored_count = len(fresh_scored)
        score_fallbacks = sum(1 for a in fresh_scored if not a.get('_score_ok'))
        lat = [a['_score_ms'] for a in fresh_scored if a.get('_score_ms') is not None]
        avg_score_ms = round(sum(lat) / len(lat), 1) if lat else None
        embed_fallbacks = sum(1 for a in scored if a.get('embedding') is None)
        for m in ollama_ps():
            if m.get('name') == chat_model or m.get('model') == chat_model:
                size, vram = m.get('size') or 0, m.get('size_vram') or 0
                gpu_ratio = round(vram / size, 2) if size else None
                break
        log(f'ollama: {scored_count - score_fallbacks}/{scored_count} scored, '
            f'avg {avg_score_ms}ms/story, gpu_ratio={gpu_ratio}')

        # Semantic blocklist (drop articles close to a "topic to avoid").
        scored = semantic_block(scored, cfg['block_topics'],
                                cfg['block_topic_threshold'], embed_model)
        # Personalization: boost articles that match the user's taste.
        apply_taste(scored, embed_model, cfg['taste_weight'])

        log('clustering')
        _set_status(phase='clustering')
        scored = cluster(scored, cfg['sim_threshold'], cfg['boost_cap'], cfg['boost_k'])

        # Keep a copy of each article's embedding (keyed by id) before they're
        # stripped from the digest — semantic search ranks against these.
        emb_index = {a['id']: a['embedding'] for a in scored if a.get('embedding')}

        # Drop transient fields; sort by boosted score (matches client order).
        for a in scored:
            a.pop('embedding', None)
            a.pop('_score_ok', None)
            a.pop('_score_ms', None)
        scored.sort(key=lambda a: a['score'] + a.get('cluster_boost', 0.0)
                    + a.get('taste_boost', 0.0), reverse=True)

        # Retention: rank-weighted lifespan inside the [floor, ceiling] band.
        pool_size = len(scored)
        scored = apply_retention(scored, cfg)
        if len(scored) != pool_size:
            log(f'retention kept {len(scored)}/{pool_size} articles')

        clusters = len({a['cluster_id'] for a in scored})
        article_count = len(scored)
        _atomic_write_json(DIGEST_FILE, {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'article_count': article_count,
            'articles': scored,
        })
        # Embeddings index, scoped to exactly the retained digest, for /search.
        kept_ids = {a['id'] for a in scored}
        _atomic_write_json(EMBEDDINGS_FILE, {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'model': embed_model,
            'vectors': {i: v for i, v in emb_index.items() if i in kept_ids},
        })
        # Best-effort copy to the corpus archive for the offline Clustering
        # Bench (off unless BENCH_CAPTURE_ENABLED). Never raises; the digest
        # and the feed are unaffected if it fails. See corpus_archive.py.
        corpus_archive.archive_articles(scored, emb_index, embed_model, cfg, log=log)
        log(f'wrote {article_count} articles in {clusters} clusters -> {DIGEST_FILE}')
        _set_status(article_count=article_count)
        ok = True
        return True
    except Exception as exc:
        error = str(exc)
        log(f'run failed: {exc}')
        return False
    finally:
        finished = _now_iso()
        try:
            dur = (datetime.fromisoformat(finished)
                   - datetime.fromisoformat(started)).total_seconds()
        except Exception:
            dur = None
        _record_run({
            'started_at': started,
            'finished_at': finished,
            'duration_s': round(dur, 1) if dur is not None else None,
            'status': 'ok' if ok else ('failed' if error else 'empty'),
            'feed_count': feed_count,
            'new_count': new_count,
            'article_count': article_count,
            'clusters': clusters,
            'scored_count': scored_count,        # fresh articles sent to Ollama
            'score_fallbacks': score_fallbacks,  # of those, how many Ollama failed to score
            'embed_fallbacks': embed_fallbacks,
            'avg_score_ms': avg_score_ms,        # avg chat latency (high ⇒ likely CPU)
            'gpu_ratio': gpu_ratio,              # chat model VRAM/size (1.0 ⇒ fully on GPU)
            'error': error,
        })
        _set_status(running=False, phase=None, last_run_at=finished, last_ok=ok)
        _run_lock.release()


# ──────────────────────────────────────────────────────────────────────
# Scheduler
# ──────────────────────────────────────────────────────────────────────
def _scheduler_loop():
    last_slot = None
    while not _stop.is_set():
        # Re-read each tick so schedule edits from the UI take effect live.
        cfg = settings.load()
        start, end, interval = (cfg['active_start_hour'],
                                cfg['active_end_hour'],
                                max(1, cfg['interval_hours']))
        now = datetime.now().astimezone()
        in_window = start <= now.hour <= end
        on_interval = (now.hour - start) % interval == 0
        if in_window and on_interval and now.minute == 0:
            slot = (now.date(), now.hour)
            if slot != last_slot:
                last_slot = slot
                run_once()
        _stop.wait(30)


def start_background(run_now=True):
    """Start the scheduler thread, and optionally kick an immediate run."""
    threading.Thread(target=_scheduler_loop, name='scheduler', daemon=True).start()
    cfg = settings.load()
    log(f"scheduler started ({cfg['active_start_hour']}-{cfg['active_end_hour']} "
        f"every {cfg['interval_hours']}h)")
    if run_now:
        threading.Thread(target=run_once, name='initial-run', daemon=True).start()


def refresh_async():
    """Trigger a one-off run in the background (used by POST /refresh)."""
    threading.Thread(target=run_once, name='refresh', daemon=True).start()


if __name__ == '__main__':
    run_once()
