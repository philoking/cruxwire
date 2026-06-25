#!/usr/bin/env python3
"""Cruxwire HTTP server.

Single-process app: serves the static dashboard, the per-user state API, and
in-app feed management, and starts the ingestion pipeline (pipeline.py) on a
background scheduler thread. No nginx, no n8n, no NAS share.

Endpoints:
  GET  /                      digest.html
  GET  /favicon.ico  + favicon-16x16.png / favicon-32x32.png / apple-touch-icon.png   icons
  GET  /digest.json           current digest (from DIGEST_FILE; empty if none yet)
  GET  /state    PUT /state   per-user read/later/history/learning state
  GET  /feeds    POST /feeds  DELETE /feeds   GET /feeds/check   feed management
  GET  /feeds.opml            export feeds as OPML
  POST /feeds/import          batch-add mapped feeds (OPML import); returns a summary
  GET  /settings PUT /settings  runtime-editable tuning knobs + models + blocklist
  GET  /models                installed Ollama models (for the model pickers)
  GET  /status                live pipeline status (running/phase/last run)
  POST /refresh               trigger a pipeline run now
"""
import concurrent.futures
import json
import os
import re
import tempfile
import threading
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import pipeline
import settings

STATE_FILE  = os.environ.get('STATE_FILE', '/data/state.json')
DIGEST_FILE = os.environ.get('DIGEST_FILE', '/data/digest.json')
FEEDS_FILE  = os.environ.get('FEEDS_FILE', '/data/feeds.json')
STATIC_DIR  = os.environ.get('STATIC_DIR', '.')
SEED_FEEDS  = os.environ.get('SEED_FEEDS', '')
SEED_CATEGORIES = os.environ.get('SEED_CATEGORIES', '')
PORT = int(os.environ.get('PORT', '8090'))

# History retention is a runtime-editable setting now (see settings.py); the env
# var there remains the default. Read Later is curated and never aged out.

# readIds are kept (not pruned to the digest) so "done" sticks across runs — see
# prune(). Capped only as a runaway guard; far more than any lookback window
# needs (a human won't read this many stories before the feed drops them).
MAX_READ_IDS = 20000

# Valid feed categories come from the configured category list (categories.json,
# served at GET /categories). POST /feeds enforces them so the UI can't insert a
# category the client has no label/color for.
def allowed_categories():
    return {c['key'] for c in settings.load_categories()}

STATIC_TYPES = {
    '.html': 'text/html; charset=utf-8',
    '.ico': 'image/x-icon',
    '.png': 'image/png',
    '.json': 'application/json',
}

# Icon files served at the root, by exact name (no path traversal).
ICON_FILES = ('favicon.ico', 'favicon-16x16.png', 'favicon-32x32.png',
              'apple-touch-icon.png', 'logo-mark.png')

DEFAULT_STATE = {
    'updatedAt': None,
    'readIds': [],
    'later': [],
    'history': [],
    'viewState': {
        'currentView': 'digest',
        'currentCat': 'all',
        'digestCat': 'all',
        # Client display preference: show per-story ranking score chips. Opt-in.
        'showScores': False,
    },
    # Per-source interaction counters used by client-side ranking. Persists
    # independently of readIds/later/history so the user's inbox hygiene
    # (clearing Later, history aging out) doesn't wipe their preferences.
    'sourceStats': {},
    'learning': {'lastDecayAt': None},
}

# Serialize read-modify-write so concurrent PUTs don't trample each other.
_state_lock = threading.Lock()
_feeds_lock = threading.Lock()


def _parse_iso(value):
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _now_iso():
    now = datetime.now(timezone.utc)
    return now.strftime('%Y-%m-%dT%H:%M:%S.') + f'{now.microsecond // 1000:03d}Z'


def load_state():
    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
            if not isinstance(data, dict):
                return DEFAULT_STATE.copy()
            return {**DEFAULT_STATE, **data}
    except FileNotFoundError:
        return DEFAULT_STATE.copy()
    except Exception:
        return DEFAULT_STATE.copy()


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE) or '.', exist_ok=True)
    # Atomic write so a crash mid-write can't truncate state.json.
    dir_ = os.path.dirname(STATE_FILE) or '.'
    fd, tmp_path = tempfile.mkstemp(prefix='.state-', suffix='.json', dir=dir_)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            json.dump(state, fh, indent=2)
        os.replace(tmp_path, STATE_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def prune(state):
    """Drop entries that can no longer affect any view.

    readIds: kept (deduped, capped) — NOT pruned to the current digest. Under
             the carry-forward model the pipeline vacates a read story from the
             digest immediately, but its source feed can still serve it for up
             to LOOKBACK_HOURS — so the id must persist or the story gets
             re-ingested as "fresh" and reappears. Pruning to the digest is
             exactly what made dismissed stories come back.
    later:   never aged out (manually curated); only de-duped/validated.
    history: viewedAt older than HISTORY_RETENTION_DAYS calendar days.
    """
    cfg = settings.load()
    history_retention_days = cfg['history_retention_days']

    seen = set()
    kept = []
    for rid in (state.get('readIds') or []):
        if isinstance(rid, str) and rid not in seen:
            seen.add(rid)
            kept.append(rid)
    state['readIds'] = kept[-MAX_READ_IDS:]   # newest-last; cap is a runaway guard only

    # Read Later is a manually-curated list — it is never aged out. Only drop
    # malformed entries and de-dupe by id; items persist until the user removes
    # them. (Retention applies to History, which is auto-tracked; see below.)
    later = []
    seen_later = set()
    for item in (state.get('later') or []):
        if not isinstance(item, dict) or not item.get('id'):
            continue
        if item['id'] in seen_later:
            continue
        seen_later.add(item['id'])
        later.append(item)
    state['later'] = later

    history_cutoff = datetime.now().astimezone().date() - timedelta(days=history_retention_days - 1)
    history = []
    for item in (state.get('history') or []):
        if not isinstance(item, dict) or not item.get('id'):
            continue
        ts = _parse_iso(item.get('viewedAt'))
        if ts is None or ts.astimezone().date() >= history_cutoff:
            history.append(item)
    state['history'] = history

    # Drop sourceStats entries for sources no longer in feeds.json. User intent
    # is "once removed, gone for good". Guard against a transient empty/unreadable
    # feeds list so a disk hiccup can't nuke every counter.
    try:
        feeds = load_feeds()
    except Exception:
        feeds = []
    if feeds:
        feed_sources = {
            f.get('source') for f in feeds
            if isinstance(f, dict) and isinstance(f.get('source'), str)
        }
        stats = state.get('sourceStats') or {}
        state['sourceStats'] = {s: v for s, v in stats.items() if s in feed_sources}

    return state


def _normalize_source_stats(raw):
    """Coerce sourceStats into {source: {opens, saves, dismisses}} with
    numeric counters. Counters can be fractional because of decay."""
    if not isinstance(raw, dict):
        return {}
    out = {}
    for source, counters in raw.items():
        if not isinstance(source, str) or not isinstance(counters, dict):
            continue
        coerced = {}
        for key in ('opens', 'saves', 'dismisses'):
            try:
                coerced[key] = float(counters.get(key, 0) or 0)
            except (TypeError, ValueError):
                coerced[key] = 0.0
        out[source] = coerced
    return out


def _normalize_learning(raw):
    if not isinstance(raw, dict):
        return {'lastDecayAt': None}
    last = raw.get('lastDecayAt')
    return {'lastDecayAt': last if isinstance(last, str) else None}


def normalize_payload(payload):
    """Coerce incoming PUT into the expected shape; ignore unknown fields."""
    vs_default = DEFAULT_STATE['viewState']
    raw_vs = payload.get('viewState') if isinstance(payload.get('viewState'), dict) else {}
    view_state = {**vs_default, **{k: raw_vs[k] for k in vs_default if k in raw_vs}}

    return {
        'updatedAt': payload.get('updatedAt') or None,
        'readIds': list(payload.get('readIds') or []),
        'later': list(payload.get('later') or []),
        'history': list(payload.get('history') or []),
        'viewState': view_state,
        'sourceStats': _normalize_source_stats(payload.get('sourceStats')),
        'learning': _normalize_learning(payload.get('learning')),
    }


def load_feeds():
    try:
        with open(FEEDS_FILE, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
            return data if isinstance(data, list) else []
    except FileNotFoundError:
        return []
    except (json.JSONDecodeError, OSError):
        # A malformed feeds.json shouldn't crash the server — surface an empty
        # list so the UI can at least show the add form.
        return []


def save_feeds(feeds):
    """Atomic write — the pipeline reads this file too, so it must never see
    partial bytes."""
    os.makedirs(os.path.dirname(FEEDS_FILE) or '.', exist_ok=True)
    dir_ = os.path.dirname(FEEDS_FILE) or '.'
    fd, tmp_path = tempfile.mkstemp(prefix='.feeds-', suffix='.json', dir=dir_)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            json.dump(feeds, fh, indent=2)
        os.replace(tmp_path, FEEDS_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


_TITLE_RE = re.compile(
    r'<title[^>]*>\s*(?:<!\[CDATA\[\s*)?([^<\]]+?)(?:\s*\]\]>)?\s*</title>',
    re.IGNORECASE,
)


def validate_feed_url(url, timeout=10):
    """Fetch the URL and confirm it actually returns an RSS or Atom feed.

    Returns {'ok': bool, 'error': str?, 'title': str?, 'kind': 'rss'|'atom'?}.
    """
    if not url or not isinstance(url, str):
        return {'ok': False, 'error': 'URL is required'}
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        return {'ok': False, 'error': 'URL must be http(s):// with a host'}

    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 Cruxwire (feed validator)',
        'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*',
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.status if hasattr(resp, 'status') else resp.getcode()
            if not (200 <= status < 300):
                return {'ok': False, 'error': f'HTTP {status}'}
            body_bytes = resp.read(8192)
    except urllib.error.HTTPError as e:
        return {'ok': False, 'error': f'HTTP {e.code}'}
    except urllib.error.URLError as e:
        return {'ok': False, 'error': f'Network error: {e.reason}'}
    except Exception as e:
        return {'ok': False, 'error': f'Fetch failed: {e}'}

    body = body_bytes.decode('utf-8', errors='replace')
    low = body.lower()
    kind = None
    if '<rss' in low or '<rdf:rdf' in low:
        kind = 'rss'
    elif '<feed' in low and 'xmlns' in low:
        kind = 'atom'
    if not kind:
        return {'ok': False, 'error': 'Response does not look like RSS or Atom'}

    m = _TITLE_RE.search(body)
    title = m.group(1).strip() if m else None
    return {'ok': True, 'kind': kind, 'title': title}


def normalize_feed_payload(payload):
    if not isinstance(payload, dict):
        return None, 'Payload must be an object'
    url = (payload.get('url') or '').strip()
    source = (payload.get('source') or '').strip()
    category = (payload.get('category') or '').strip()
    if not url:
        return None, 'url is required'
    if not source:
        return None, 'source is required'
    if not category:
        return None, 'category is required'
    if category not in allowed_categories():
        return None, f'Unknown category: {category}'
    return {'url': url, 'source': source, 'category': category}, None


def feeds_to_opml(feeds, categories):
    """Serialize the feed list to OPML 2.0, grouping feeds into <outline>
    folders by category (in category/priority order). Pure stdlib."""
    cats = {c['key']: c for c in categories}
    order = [c['key'] for c in categories]
    groups = {}
    for f in feeds:
        if isinstance(f, dict) and f.get('url'):
            groups.setdefault(f.get('category') or '', []).append(f)

    opml = ET.Element('opml', version='2.0')
    head = ET.SubElement(opml, 'head')
    ET.SubElement(head, 'title').text = 'Cruxwire feeds'
    body = ET.SubElement(opml, 'body')

    def add_folder(key):
        label = (cats.get(key) or {}).get('label') or (key or 'Uncategorized')
        folder = ET.SubElement(body, 'outline', text=label, title=label)
        for f in groups[key]:
            src = f.get('source') or f['url']
            ET.SubElement(folder, 'outline', type='rss', text=src, title=src, xmlUrl=f['url'])

    for key in order:                       # configured categories first, in order
        if key in groups:
            add_folder(key)
    for key in groups:                      # any leftover (unknown) categories
        if key not in order:
            add_folder(key)

    xml = ET.tostring(opml, encoding='unicode')
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml


def import_feeds(items, do_validate=True):
    """Batch-add mapped feeds: drop bad payloads / unknown categories, skip
    duplicates (existing + within the batch), optionally validate each URL
    concurrently, then append the survivors. Returns a summary."""
    with _feeds_lock:
        existing_urls = {f.get('url') for f in load_feeds() if isinstance(f, dict)}

    seen, candidates, invalid, dupes = set(), [], [], 0
    for it in (items or []):
        feed, err = normalize_feed_payload(it if isinstance(it, dict) else {})
        if err:
            src = (it or {}).get('source', '') if isinstance(it, dict) else ''
            invalid.append({'url': (it or {}).get('url', '') if isinstance(it, dict) else '',
                            'source': src, 'error': err})
            continue
        if feed['url'] in existing_urls or feed['url'] in seen:
            dupes += 1
            continue
        seen.add(feed['url'])
        candidates.append(feed)

    # Validate outside the lock (each call hits the network).
    failed, valid = [], candidates
    if do_validate and candidates:
        valid = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(16, len(candidates))) as ex:
            checked = list(ex.map(lambda f: (f, validate_feed_url(f['url'])), candidates))
        for f, res in checked:
            (valid if res.get('ok') else failed).append(
                f if res.get('ok') else {'url': f['url'], 'source': f['source'],
                                         'error': res.get('error') or 'Invalid feed'})

    added = 0
    with _feeds_lock:
        feeds = load_feeds()
        cur = {f.get('url') for f in feeds if isinstance(f, dict)}
        for f in valid:
            if f['url'] in cur:
                dupes += 1
                continue
            cur.add(f['url'])
            feeds.append(f)
            added += 1
        if added:
            save_feeds(feeds)

    return {'added': added, 'skipped_dupes': dupes, 'failed': failed, 'invalid': invalid}


def seed_feeds_if_missing():
    """First-run convenience: copy the bundled sample feed list into the data
    volume so a fresh deploy starts with feeds instead of an empty UI."""
    if os.path.exists(FEEDS_FILE) or not SEED_FEEDS:
        return
    try:
        with open(SEED_FEEDS, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        if isinstance(data, list) and data:
            save_feeds(data)
            print(f'seeded {len(data)} feeds from {SEED_FEEDS}', flush=True)
    except Exception as exc:
        print(f'feed seed failed: {exc}', flush=True)


def seed_categories_if_missing():
    """First-run convenience: copy the bundled sample categories onto the data
    volume so a self-hoster has an editable starting point (and the app loads
    them from one place). Until edited, load_categories() falls back to the
    built-in defaults anyway, so this is purely for discoverability."""
    if os.path.exists(settings.CATEGORIES_FILE) or not SEED_CATEGORIES:
        return
    try:
        with open(SEED_CATEGORIES, 'r', encoding='utf-8') as fh:
            data = json.load(fh)
        if isinstance(data, list) and data:
            os.makedirs(os.path.dirname(settings.CATEGORIES_FILE) or '.', exist_ok=True)
            with open(settings.CATEGORIES_FILE, 'w', encoding='utf-8') as fh:
                json.dump(data, fh, indent=2)
            print(f'seeded {len(data)} categories from {SEED_CATEGORIES}', flush=True)
    except Exception as exc:
        print(f'category seed failed: {exc}', flush=True)


def deploy_info():
    """Deploy-time wiring shown read-only in the UI — set via docker-compose /
    env at container start, so editing needs a restart (not a writable setting).
    Model selection is NOT here; it's a runtime setting."""
    return {
        'ollama_host': pipeline.OLLAMA_HOST,
        'port': PORT,
        'state_file': STATE_FILE,
        'digest_file': DIGEST_FILE,
        'feeds_file': FEEDS_FILE,
        'settings_file': settings.SETTINGS_FILE,
    }


def list_ollama_models():
    """Model names installed on the configured Ollama host, for the model
    pickers in Settings. Returns {'ok': bool, 'models': [...], 'error': str?}."""
    url = pipeline.OLLAMA_HOST + '/api/tags'
    try:
        req = urllib.request.Request(url, headers={'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode('utf-8', errors='replace'))
        names = sorted(
            m.get('name') for m in (data.get('models') or [])
            if isinstance(m, dict) and m.get('name')
        )
        return {'ok': True, 'models': names}
    except Exception as exc:
        return {'ok': False, 'models': [], 'error': f'{exc}'}


def settings_payload():
    """Full Settings response: effective values, the schema that drives the
    form, and read-only deploy info."""
    return {
        'settings': settings.load(),
        'schema': settings.schema(),
        'deploy': deploy_info(),
    }


class StateHandler(BaseHTTPRequestHandler):
    def _set_headers(self, status=200, content_type='application/json'):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def _json(self, status, payload):
        self._set_headers(status)
        self.wfile.write(json.dumps(payload).encode('utf-8'))

    def _serve_file(self, fs_path, content_type):
        try:
            with open(fs_path, 'rb') as fh:
                data = fh.read()
        except FileNotFoundError:
            self.send_error(404, 'Not Found')
            return
        except Exception:
            self.send_error(500, 'Read error')
            return
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        # Always revalidate static assets so a deploy shows up on a normal
        # reload — no hard-refresh needed after the UI changes.
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(data)

    def _serve_digest(self):
        """Serve the current digest, or an empty one if the pipeline hasn't
        produced its first run yet (nicer than a 404 in the UI)."""
        try:
            with open(DIGEST_FILE, 'rb') as fh:
                data = fh.read()
        except FileNotFoundError:
            self._json(200, {'generated_at': None, 'article_count': 0, 'articles': []})
            return
        except Exception:
            self.send_error(500, 'Read error')
            return
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(data)

    def _read_json_body(self, allow_list=False):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length) if length else b''
        try:
            payload = json.loads(body.decode('utf-8') if body else '{}')
        except Exception as exc:
            return None, f'Invalid JSON: {exc}'
        if allow_list and isinstance(payload, list):
            return payload, None
        if not isinstance(payload, dict):
            return None, 'Payload must be an object'
        return payload, None

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path
        if path == '/state':
            with _state_lock:
                state = prune(load_state())
            self._json(200, state)
            return
        if path == '/feeds':
            self._json(200, load_feeds())
            return
        if path == '/feeds.opml':
            data = feeds_to_opml(load_feeds(), settings.load_categories()).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/x-opml; charset=utf-8')
            self.send_header('Content-Disposition', 'attachment; filename="cruxwire-feeds.opml"')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(data)
            return
        if path == '/settings':
            self._json(200, settings_payload())
            return
        if path == '/models':
            self._json(200, list_ollama_models())
            return
        if path == '/status':
            self._json(200, pipeline.get_status())
            return
        if path == '/runs':
            self._json(200, pipeline.get_runs())
            return
        if path == '/categories':
            self._json(200, settings.load_categories())
            return
        if path == '/search':
            qs = parse_qs(urlparse(self.path).query)
            q = (qs.get('q') or [''])[0]
            try:
                thr = float((qs.get('threshold') or [''])[0])
            except ValueError:
                thr = pipeline.SEARCH_THRESHOLD
            results = pipeline.semantic_search(q, threshold=thr)
            self._json(200, {'query': q, 'results': results})
            return
        if path == '/feeds/check':
            qs = parse_qs(urlparse(self.path).query)
            url = (qs.get('url') or [''])[0]
            result = validate_feed_url(url)
            self._json(200 if result['ok'] else 400, result)
            return
        if path == '/digest.json':
            self._serve_digest()
            return
        # Static assets (explicit whitelist — no path traversal).
        if path in ('/', '/index.html', '/digest.html'):
            self._serve_file(os.path.join(STATIC_DIR, 'digest.html'), STATIC_TYPES['.html'])
            return
        if path.lstrip('/') in ICON_FILES:
            name = path.lstrip('/')
            self._serve_file(os.path.join(STATIC_DIR, name), STATIC_TYPES[os.path.splitext(name)[1]])
            return
        self.send_error(404, 'Not Found')

    def do_PUT(self):
        path = urlparse(self.path).path
        if path == '/settings':
            payload, err = self._read_json_body()
            if err:
                self._json(400, {'error': err})
                return
            # settings.save validates/clamps and ignores unknown keys.
            settings.save(payload)
            self._json(200, settings_payload())
            return
        if path == '/categories':
            payload, err = self._read_json_body(allow_list=True)
            if err:
                self._json(400, {'error': err})
                return
            try:
                cats = settings.save_categories(payload)
            except ValueError as exc:
                self._json(400, {'error': str(exc)})
                return
            self._json(200, cats)
            return
        if path != '/state':
            self.send_error(404, 'Not Found')
            return
        payload, err = self._read_json_body()
        if err:
            self.send_error(400, err)
            return

        with _state_lock:
            state = normalize_payload(payload)
            # Old clients that don't send sourceStats / learning would silently
            # wipe them. Preserve whatever's on disk when not included explicitly.
            if 'sourceStats' not in payload:
                state['sourceStats'] = load_state().get('sourceStats', {})
            if 'learning' not in payload:
                state['learning'] = load_state().get('learning', {'lastDecayAt': None})
            state = prune(state)
            state['updatedAt'] = _now_iso()
            save_state(state)
        self._json(200, state)

    def do_POST(self):
        path = urlparse(self.path).path
        if path == '/refresh':
            pipeline.refresh_async()
            self._json(202, {'status': 'refresh triggered'})
            return
        if path == '/tldr':
            payload, err = self._read_json_body()
            if err:
                self._json(400, {'error': err})
                return
            url = (payload or {}).get('url')
            title = (payload or {}).get('title') or ''
            if not isinstance(url, str) or not url.startswith('http'):
                self._json(400, {'error': 'valid url required'})
                return
            result = pipeline.generate_tldr(url, str(title))
            if result is None:
                self._json(502, {'error': 'Could not summarise (Ollama may be down)'})
                return
            self._json(200, result)
            return
        if path == '/feeds/import':
            payload, err = self._read_json_body()
            if err:
                self._json(400, {'error': err})
                return
            items = payload.get('feeds')
            if not isinstance(items, list):
                self._json(400, {'error': 'feeds[] is required'})
                return
            do_validate = payload.get('validate', True) is not False
            self._json(200, import_feeds(items, do_validate))
            return
        if path != '/feeds':
            self.send_error(404, 'Not Found')
            return
        payload, err = self._read_json_body()
        if err:
            self._json(400, {'error': err})
            return
        feed, err = normalize_feed_payload(payload)
        if err:
            self._json(400, {'error': err})
            return

        # Skip validation when the client is restoring a feed it just removed —
        # the URL was confirmed at original-add time and revalidating could fail
        # flakily for a feed we already know is good.
        qs = parse_qs(urlparse(self.path).query)
        skip_validate = (qs.get('skip_validate') or ['0'])[0] in ('1', 'true')

        if not skip_validate:
            validation = validate_feed_url(feed['url'])
            if not validation['ok']:
                self._json(400, {'error': validation.get('error') or 'Invalid feed'})
                return

        with _feeds_lock:
            feeds = load_feeds()
            if any(isinstance(f, dict) and f.get('url') == feed['url'] for f in feeds):
                self._json(409, {'error': 'Feed already exists'})
                return
            feeds.append(feed)
            save_feeds(feeds)
        self._json(201, feed)

    def do_DELETE(self):
        if urlparse(self.path).path != '/feeds':
            self.send_error(404, 'Not Found')
            return
        qs = parse_qs(urlparse(self.path).query)
        url = (qs.get('url') or [''])[0].strip()
        if not url:
            self._json(400, {'error': 'url query param is required'})
            return

        with _feeds_lock:
            feeds = load_feeds()
            removed = next((f for f in feeds if isinstance(f, dict) and f.get('url') == url), None)
            if removed is None:
                self._json(404, {'error': 'Feed not found'})
                return
            new_feeds = [f for f in feeds if not (isinstance(f, dict) and f.get('url') == url)]
            save_feeds(new_feeds)

        # Wipe the source's stats too so the ghost row disappears immediately.
        # The client stashes the returned stats for in-session undo.
        removed_stats = None
        source = removed.get('source')
        if source:
            with _state_lock:
                state = load_state()
                stats = state.get('sourceStats') or {}
                if source in stats:
                    removed_stats = stats.pop(source)
                    state['sourceStats'] = stats
                    state['updatedAt'] = _now_iso()
                    save_state(state)
        self._json(200, {'removed': removed, 'stats': removed_stats})

    def log_message(self, format, *args):
        return


if __name__ == '__main__':
    seed_feeds_if_missing()
    seed_categories_if_missing()
    pipeline.start_background(run_now=True)
    server = ThreadingHTTPServer(('0.0.0.0', PORT), StateHandler)
    print(f'Cruxwire server listening on http://0.0.0.0:{PORT}/', flush=True)
    server.serve_forever()
