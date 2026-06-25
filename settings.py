#!/usr/bin/env python3
"""Runtime-editable settings shared by the server and the pipeline.

Tunable knobs live in settings.json on the data volume (SETTINGS_FILE). The UI
edits them via GET/PUT /settings; the pipeline re-reads them every run and the
server reads them when pruning, so changes take effect without a restart.

Env vars still provide the *defaults* (so existing .env deployments behave
exactly as before); the file only stores user overrides. Deploy-time wiring
(Ollama host/model, port, file paths) is deliberately NOT here — changing those
needs a container restart, so the UI shows them read-only instead.
"""
import json
import os
import re
import tempfile
import threading

SETTINGS_FILE = os.environ.get('SETTINGS_FILE', '/data/settings.json')
CATEGORIES_FILE = os.environ.get('CATEGORIES_FILE', '/data/categories.json')

# Guards read-modify-write in save(); reads rely on the atomic os.replace.
_lock = threading.Lock()

MAX_KEYWORDS = 200
MAX_KEYWORD_LEN = 80

# ── Categories ──────────────────────────────────────────────────────────
# A category is data, not code, so a self-hoster can make the app about THEIR
# interests by editing categories.json (key/label/color + an `interest` line
# that the scorer's prompt is built from — the interest text matters most).
# These defaults are the ultimate fallback if the file is missing/malformed.
DEFAULT_CATEGORIES = [
    {'key': 'ai', 'label': 'AI', 'color': '#7b5cff',
     'interest': 'AI / machine learning -- models, research, tools, industry news'},
    {'key': 'devtools', 'label': 'Dev Tools', 'color': '#3fae46',
     'interest': 'Developer tools -- editors, CLIs, platforms, DevOps, cloud-native'},
    {'key': 'tech', 'label': 'Tech', 'color': '#2b7fff',
     'interest': 'Technology -- hardware, software, industry moves'},
    {'key': 'gaming', 'label': 'Gaming', 'color': '#ff5a3c',
     'interest': 'Gaming -- PC and console games, industry, releases'},
    {'key': 'music', 'label': 'Music', 'color': '#10b981',
     'interest': 'Music production -- DAWs, plugins, hardware, sound design, technique'},
    {'key': 'woodworking', 'label': 'Woodworking', 'color': '#d98a3b',
     'interest': 'Woodworking -- hand tools, power tools, joinery, projects, reviews'},
    {'key': 'photography', 'label': 'Photography', 'color': '#e84d9c',
     'interest': 'Photography -- cameras, lenses, software, technique, light'},
    {'key': 'pm', 'label': 'Product', 'color': '#8a90a0',
     'interest': 'Product management -- strategy, frameworks, discovery, craft'},
    {'key': 'productivity', 'label': 'Productivity', 'color': '#06b6d4',
     'interest': 'Productivity / PKM -- Obsidian, PARA, note-taking, workflows, tools'},
]

_CAT_KEY_RE = re.compile(r'^[a-z0-9]{1,20}$')
_HEX_RE = re.compile(r'^#[0-9a-fA-F]{6}$')


def _coerce_color(v):
    v = str(v or '').strip()
    return v if _HEX_RE.match(v) else '#8a90a0'


def _coerce_categories(items):
    """Validate/normalise a category list: drop non-dicts, bad/duplicate keys;
    clamp label/interest; coerce colour. Same rules for file reads and UI saves."""
    out, seen = [], set()
    for c in (items if isinstance(items, list) else []):
        if not isinstance(c, dict):
            continue
        key = str(c.get('key', '')).strip().lower()
        if not _CAT_KEY_RE.match(key) or key in seen:
            continue
        seen.add(key)
        label = str(c.get('label') or key).strip()[:40] or key
        out.append({
            'key': key,
            'label': label,
            'color': _coerce_color(c.get('color')),
            'interest': str(c.get('interest') or label).strip()[:300],
        })
    return out


def load_categories():
    """The configured categories (key/label/color/interest), validated and
    de-duplicated. Falls back to DEFAULT_CATEGORIES so a missing or malformed
    file can never break a run. Order = scoring priority."""
    try:
        with open(CATEGORIES_FILE, 'r', encoding='utf-8') as fh:
            raw = json.load(fh)
    except Exception:
        raw = None
    cats = _coerce_categories(raw if isinstance(raw, list) else DEFAULT_CATEGORIES)
    return cats or list(DEFAULT_CATEGORIES)


def save_categories(items):
    """Validate and persist a category list (atomic). Raises ValueError if
    nothing valid remains — the app must always have at least one category."""
    cats = _coerce_categories(items)
    if not cats:
        raise ValueError('At least one valid category is required.')
    with _lock:
        _atomic_write(CATEGORIES_FILE, cats)
    return cats


def category_keys():
    return [c['key'] for c in load_categories()]


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


# Schema: key -> spec. Defaults come from env so .env still drives them.
# group/label/help/min/max/step are returned to the client (GET /settings) so
# the Settings form is generated from this schema, not hand-maintained twice.
def _schema():
    return {
        # ── Models (which Ollama models to use; host/port stay in compose) ─
        'ollama_model': {
            'type': 'str', 'default': os.environ.get('OLLAMA_MODEL', 'qwen2.5:latest'),
            'group': 'models', 'label': 'Chat model',
            'help': 'Ollama model used to score, summarise, and categorise articles.'},
        'embed_model': {
            'type': 'str', 'default': os.environ.get('EMBED_MODEL', 'nomic-embed-text'),
            'group': 'models', 'label': 'Embedding model',
            'help': 'Ollama model used to embed articles for same-story clustering.'},
        # ── Clustering & ranking ──────────────────────────────────────
        'sim_threshold': {
            'type': 'float', 'default': _env_float('SIM_THRESHOLD', 0.74),
            'min': 0.5, 'max': 0.99, 'step': 0.01,
            'group': 'cluster', 'label': 'Merge similarity',
            'help': 'Cosine similarity above which two articles are collapsed as the same topic (any source). Lower = more aggressive merging; too low over-merges unrelated stories.'},
        'boost_cap': {
            'type': 'float', 'default': _env_float('BOOST_CAP', 1.5),
            'min': 0.0, 'max': 5.0, 'step': 0.1,
            'group': 'cluster', 'label': 'Boost cap',
            'help': 'Maximum cross-source rank boost a story can receive.'},
        'boost_k': {
            'type': 'float', 'default': _env_float('BOOST_K', 0.6),
            'min': 0.0, 'max': 5.0, 'step': 0.1,
            'group': 'cluster', 'label': 'Boost strength',
            'help': 'Boost = min(cap, k x log2(sources)). Higher = coverage counts for more.'},
        'taste_weight': {
            'type': 'float', 'default': _env_float('TASTE_WEIGHT', 1.0),
            'min': 0.0, 'max': 3.0, 'step': 0.1,
            'group': 'cluster', 'label': 'Personalization strength',
            'help': 'How much your taste (embeddings of what you save and open) boosts a story\'s rank. Max points it can add to a perfectly on-taste article. 0 disables personalization.'},
        # ── Ingestion ─────────────────────────────────────────────────
        'lookback_hours': {
            'type': 'int', 'default': _env_int('LOOKBACK_HOURS', 36),
            'min': 1, 'max': 168,
            'group': 'ingestion', 'label': 'Lookback (hours)',
            'help': 'Ignore feed items older than this.'},
        'max_articles': {
            'type': 'int', 'default': _env_int('MAX_ARTICLES', 1024),
            'min': 16, 'max': 5000,
            'group': 'ingestion', 'label': 'Max articles / run',
            'help': 'Cap on the number of articles scored per run.'},
        'score_concurrency': {
            'type': 'int', 'default': _env_int('SCORE_CONCURRENCY', 4),
            'min': 1, 'max': 32,
            'group': 'ingestion', 'label': 'Scoring workers',
            'help': 'Parallel Ollama scoring requests. Raise to speed up; lower if Ollama serialises.'},
        # ── Schedule ──────────────────────────────────────────────────
        'active_start_hour': {
            'type': 'int', 'default': _env_int('ACTIVE_START_HOUR', 6),
            'min': 0, 'max': 23,
            'group': 'schedule', 'label': 'Active start hour',
            'help': 'Earliest hour (local time) a scheduled run fires.'},
        'active_end_hour': {
            'type': 'int', 'default': _env_int('ACTIVE_END_HOUR', 22),
            'min': 0, 'max': 23,
            'group': 'schedule', 'label': 'Active end hour',
            'help': 'Latest hour (local time) a scheduled run fires.'},
        'interval_hours': {
            'type': 'int', 'default': _env_int('INTERVAL_HOURS', 2),
            'min': 1, 'max': 24,
            'group': 'schedule', 'label': 'Interval (hours)',
            'help': 'Hours between runs inside the active window.'},
        # ── Retention (History only — Read Later is curated, never aged) ──
        'history_retention_days': {
            'type': 'int', 'default': _env_int('HISTORY_RETENTION_DAYS', 3),
            'min': 1, 'max': 60,
            'group': 'retention', 'label': 'History retention (days)',
            'help': 'Keep today plus this many prior calendar days of viewed articles. Read Later is never aged out.'},
        # Inbox retention: unread stories are carried forward across runs and
        # pruned by a rank-weighted lifespan held inside a [floor, ceiling] band.
        'retain_floor': {
            'type': 'int', 'default': _env_int('RETAIN_FLOOR', 25),
            'min': 0, 'max': 500,
            'group': 'retention', 'label': 'Keep at least (stories)',
            'help': 'Floor: while recent unread stories still exist, never let the inbox drop below this many — even past their normal lifespan. Stops the app going dry on quiet days.'},
        'retain_ceiling': {
            'type': 'int', 'default': _env_int('RETAIN_CEILING', 60),
            'min': 1, 'max': 1000,
            'group': 'retention', 'label': 'Keep at most (stories)',
            'help': 'Ceiling: cap on unread stories carried at once. Over this, the lowest-ranked fall off first. Stops the inbox flooding.'},
        'retain_base_ttl_hours': {
            'type': 'int', 'default': _env_int('RETAIN_BASE_TTL_HOURS', 24),
            'min': 1, 'max': 720,
            'group': 'retention', 'label': 'Min story lifespan (hours)',
            'help': 'Lifespan of the lowest-ranked story before it can fall off. Higher-ranked stories live longer, up to the max below.'},
        'retain_max_ttl_hours': {
            'type': 'int', 'default': _env_int('RETAIN_MAX_TTL_HOURS', 72),
            'min': 1, 'max': 720,
            'group': 'retention', 'label': 'Max story lifespan (hours)',
            'help': 'Lifespan of the highest-ranked story. Lifespan scales with rank between the min and this.'},
        'retain_hard_max_age_hours': {
            'type': 'int', 'default': _env_int('RETAIN_HARD_MAX_AGE_HOURS', 120),
            'min': 1, 'max': 1440,
            'group': 'retention', 'label': 'Absolute max age (hours)',
            'help': 'Hard cutoff: nothing older than this is ever carried, even to meet the floor. Stops stale news lingering.'},
        # ── Blocking ──────────────────────────────────────────────────
        'block_keywords': {
            'type': 'list', 'default': [],
            'group': 'blocklist', 'label': 'Blocked keywords',
            'help': 'Article titles containing any of these (case-insensitive substring) are dropped, on top of the built-in spam/deal filter.'},
        'block_topics': {
            'type': 'list', 'default': [],
            'group': 'blocklist', 'label': 'Blocked topics (semantic)',
            'help': 'Short phrases describing topics to avoid (e.g. "celebrity gossip"). Articles whose meaning is close to any of these are dropped — matches the concept, not just the words. Keep phrases specific; broad terms can over-block.'},
        'block_topic_threshold': {
            'type': 'float', 'default': _env_float('BLOCK_TOPIC_THRESHOLD', 0.50),
            'min': 0.42, 'max': 0.7, 'step': 0.01,
            'group': 'blocklist', 'label': 'Topic block sensitivity',
            'help': 'Cosine similarity above which an article counts as matching a blocked topic (~0.50 suits the default embedding model). Lower = blocks more aggressively (risks false blocks). Specific phrases separate far better than broad categories.'},
    }


def clean_keywords(value):
    """Trim, drop blanks, case-insensitively dedupe, and cap a keyword list."""
    if not isinstance(value, list):
        return []
    out, seen = [], set()
    for term in value:
        if not isinstance(term, str):
            continue
        t = term.strip()[:MAX_KEYWORD_LEN].strip()
        if not t:
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
        if len(out) >= MAX_KEYWORDS:
            break
    return out


MAX_STR_LEN = 120


def _coerce(spec, value):
    t = spec['type']
    if t == 'str':
        if not isinstance(value, str):
            return spec['default']
        v = value.strip()[:MAX_STR_LEN].strip()
        return v if v else spec['default']
    if t == 'float':
        try:
            v = float(value)
        except (TypeError, ValueError):
            return spec['default']
        return max(spec['min'], min(spec['max'], v))
    if t == 'int':
        try:
            v = int(round(float(value)))
        except (TypeError, ValueError):
            return spec['default']
        return max(spec['min'], min(spec['max'], v))
    if t == 'list':
        return clean_keywords(value)
    return spec['default']


def defaults():
    return {k: s['default'] for k, s in _schema().items()}


def schema():
    """Schema for the client (labels, bounds, defaults, groups)."""
    return _schema()


def _read_file():
    try:
        with open(SETTINGS_FILE, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        # A malformed settings.json must never crash a run — fall back to env.
        return {}


def load():
    """Effective settings: env defaults overlaid with validated file overrides."""
    sch = _schema()
    raw = _read_file()
    out = {}
    for key, spec in sch.items():
        out[key] = _coerce(spec, raw[key]) if key in raw else spec['default']
    return out


def _atomic_write(path, obj):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    d = os.path.dirname(path) or '.'
    fd, tmp = tempfile.mkstemp(prefix='.settings-', suffix='.json', dir=d)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            json.dump(obj, fh, indent=2)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def save(patch):
    """Validate a partial update, merge over current overrides, write atomically.

    Unknown keys are ignored; out-of-range values are clamped. Returns the new
    effective settings.
    """
    sch = _schema()
    with _lock:
        current = _read_file()
        if isinstance(patch, dict):
            for key, spec in sch.items():
                if key in patch:
                    current[key] = _coerce(spec, patch[key])
        # Drop anything no longer in the schema so the file stays clean.
        current = {k: v for k, v in current.items() if k in sch}
        _atomic_write(SETTINGS_FILE, current)
    return load()


def is_blocked(title, keywords):
    """True if title contains any user keyword (case-insensitive substring).
    The built-in regex blocklist is applied separately in the pipeline."""
    if not title or not keywords:
        return False
    low = title.lower()
    return any(k in low for k in keywords)
