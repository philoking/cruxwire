# Cruxwire

**The crux of the news, none of the noise.**
A self-hosted, local-AI news reader · **[cruxwire.app](https://cruxwire.app)**

[![CI](https://github.com/philoking/cruxwire/actions/workflows/ci.yml/badge.svg)](https://github.com/philoking/cruxwire/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A personal news dashboard in a **single container**. A background pipeline fetches RSS/Atom feeds,
uses a local LLM (via [Ollama](https://ollama.com)) to score, summarise, and de-duplicate articles,
clusters stories covered by multiple sources, learns what you actually read, and renders a ranked,
magazine-style digest. Read state (read / later / history) is shared across your devices.

<img width="1355" height="1125" alt="hero-screenshot" src="https://github.com/user-attachments/assets/c2c3cf38-5f7d-4631-b40d-0a699496e571" />

No build step, **no Python dependencies** (pure stdlib), no external services other than Ollama.

---

## Features

- **LLM-ranked digest** - every article gets a 0-10 relevance score, a 1-2 sentence summary, and a
  category from your local Ollama model.
- **Your own categories** - categories aren't hardcoded. Each carries an *interest description* you
  write (e.g. "Cooking - recipes, technique, equipment, restaurants"), and that sentence is woven into
  the scoring prompt - so "relevant" means what *you* actually care about. Editable in-app.
- **Source-agnostic de-duplication** - articles about the same story (across outlets) are clustered
  by embedding similarity and shown as one card, with the other coverage collapsed under it.
- **Personalization** - a per-source affinity multiplier and an embedding-based "taste" boost float
  the sources and topics you open/save upward, and sink ones you dismiss. Learned automatically.
- **Sticky retention** - unread stories are carried forward across runs and pruned by a rank-weighted
  lifespan inside a floor/ceiling band, so the inbox never goes dry or floods.
- **Balanced magazine layout** - a hero + "Latest" grid with an "Earlier this week" rail that stays
  height-matched; dismissing promotes the next-ranked story so sections never sit empty.
- **On-demand TL;DR** - save an article to Read Later and the local model reads the full page and
  distills it to a few bullet points plus a one-line bottom line, generated in the background so it's
  ready when you come back. Regenerate anytime; the TL;DR text is searchable too.
- **Semantic search** - filter the Home view to stories *about* a topic by meaning, not just keyword
  match ("did you hear about X?"), powered by the same local embeddings. It's a filter, like the
  category chips - no cloud, no generated answers.
- **Blocklists** - literal title keywords plus a semantic "topics to avoid" filter, on top of a
  built-in deal/spam regex.
- **Runtime settings** - ranking, ingestion, schedule, retention, blocklist, and model choices are
  all editable in-app and apply on the next run, no restart.
- **Multi-device state** - read / Read Later / History sync through the server; the app is fully
  usable offline from local cache.

---

## How it works

```
┌──────────────────── single container ─────────────────────┐
│  server.py    HTTP: UI, /digest.json, /state, /settings,  │
│               /feeds, /status, /refresh                   │
│  pipeline.py  background scheduler (cron-like):           │
│     fetch feeds → carry forward unread → score + embed    │
│     (Ollama) → cluster → retain → write digest.json       │
└───────────────────────────┬───────────────────────────────┘
                            │ HTTP
                      Ollama (OLLAMA_HOST)
       local volume /data → state.json, feeds.json,
                            digest.json, settings.json
```

**Pipeline** ([pipeline.py](pipeline.py)) runs on a schedule (default every 2h, 06:00-22:00) and on
start. Each run:

1. Fetches every feed, parses RSS/Atom, drops items older than `lookback_hours` and anything matching
   the blocklists, and de-dupes by URL.
2. **Carries forward** unread stories from the previous digest (so a story you didn't read doesn't
   vanish when its feed rotates it out); read stories are vacated, stale ones are cut.
3. Scores + summarises + embeds each fresh article via Ollama (carried stories reuse their score and
   only re-embed).
4. **Clusters** same-story coverage by cosine similarity and boosts a story by how many sources cover
   it.
5. **Retains** the pool to a rank-weighted, floor/ceiling-banded keep set.
6. Atomically writes `digest.json`.

**Frontend** ([digest.html](digest.html)) is vanilla JS - Home (magazine), Read Later, History, and
Settings views; category filters; light/dark; in-app feed management. It applies the per-device
source-affinity multiplier when ordering, and handles the layout balance, backfill, and promotion.

**State** ([server.py](server.py)) is served by the same process and pruned/retained server-side:
`readIds` persist (capped) so a dismissed story stays dismissed even after it's vacated from the
digest, History ages out after `history_retention_days`, Read Later is never aged, and `sourceStats`
persist independently so inbox hygiene never wipes your learned preferences.

See [TUNING.md](TUNING.md) for every adjustable knob.

---

## Quick start

```bash
cp .env.example .env          # then set OLLAMA_HOST to your Ollama server
docker compose up -d --build
```

Open `http://<host>:8090/`.

> ⚠️ **There is no login.** Keep this on a trusted private network and don't expose port 8090 to the
> internet - see [Security & deployment](#security--deployment).

On first run the app seeds its feed list from [feeds.sample.json](feeds.sample.json) and the pipeline
generates the first digest (give it a minute). Manage feeds in the UI's **Feeds** screen, or trigger
a refresh with `curl -X POST http://<host>:8090/refresh`.

### Requirements

- Docker + Docker Compose
- An [Ollama](https://ollama.com) server reachable over HTTP, with the models pulled:
  ```bash
  ollama pull qwen3:8b      # chat: score / summarise / categorise
  ollama pull nomic-embed-text    # embeddings: clustering / taste / semantic block
  ```

### Running on a laptop with Docker Desktop

Cruxwire runs fine on a personal Mac/Windows machine via Docker Desktop. One gotcha: if Ollama runs
**natively on the same machine**, set `OLLAMA_HOST` to **`http://host.docker.internal:11434`**, not
`localhost` - inside the container, `localhost` is the container itself, and `host.docker.internal` is
how Docker Desktop reaches your host. (On Linux that name isn't automatic: add
`extra_hosts: ["host.docker.internal:host-gateway"]` to the service, or point `OLLAMA_HOST` at the
host's LAN IP.)

> On macOS, run the **native Ollama app** - Docker Desktop's Linux VM can't use the Mac GPU, so
> running Ollama *inside* Docker would fall back to slow CPU. Cruxwire-in-Docker talking to a native
> Ollama is the right setup.

---

## Security & deployment

Cruxwire has **no authentication**. Every endpoint - including the ones that change settings, feeds,
and categories and trigger pipeline runs - is reachable by anyone who can reach the port. It is built
for a **single trusted user on a private network**: a homelab LAN, a Tailscale/WireGuard tailnet, or
`localhost`.

**Do not expose port 8090 directly to the internet.** If you want remote access, put it behind a
reverse proxy that adds authentication - Caddy/nginx with basic auth, [Authelia](https://www.authelia.com),
[Tailscale](https://tailscale.com), Cloudflare Access, etc.

Also worth knowing:

- **The pipeline fetches URLs you give it.** It requests every feed you add and, for TL;DRs, fetches
  the linked article pages. Treat your feed list as trusted input, and don't give the container
  network access to internal services it has no reason to reach.
- **No rate limiting or CSRF protection** on the mutating endpoints - the trust model is "the network
  is trusted," nothing more.
- **Runtime data is stored unencrypted** on the Docker volume (read history, Read Later, learned
  source preferences). Back up - and protect - the volume accordingly.

---

## Configuration & tuning

Two tiers:

- **Settings UI** - ranking, ingestion, schedule, retention, blocklist, and model choices are edited
  live in the **Settings** view, persisted to `settings.json` on the volume, and applied on the next
  run **without a restart**.
- **Deploy env** - infrastructure wiring (`OLLAMA_HOST`, `PORT`, file paths, timeouts) is set in
  `docker-compose.yaml` / `.env` and needs a container restart. Env vars also seed the **defaults**
  for every Settings knob.

A few common knobs:

| Var / setting | Default | Purpose |
|-----|---------|---------|
| `OLLAMA_HOST` | `http://localhost:11434` | Ollama base URL (chat + embeddings) |
| Chat / Embedding model | `qwen3:8b` / `nomic-embed-text` | Ollama models |
| Merge similarity (`SIM_THRESHOLD`) | `0.74` | Cosine threshold to collapse same-topic articles |
| Lookback (`LOOKBACK_HOURS`) | `36` | How far back fresh items are first discovered |
| Retention floor / ceiling (`RETAIN_FLOOR`/`RETAIN_CEILING`) | `25` / `60` | Keep at least / at most this many unread stories |
| Schedule (`ACTIVE_START_HOUR`/`ACTIVE_END_HOUR`/`INTERVAL_HOURS`) | `6` / `22` / `2` | When runs fire |
| `PORT` | `8090` | Container HTTP port |

**→ [TUNING.md](TUNING.md) documents every knob**, what it does, how the knobs interact, and recipes
("it goes dry on weekends", "I'm seeing duplicate cards", "surface more of what I read", …). Full env
list in [.env.example](.env.example).

### Make it about *your* interests - categories

The categories (their labels, colors, and the **interest descriptions the scorer ranks against**) are
data, not code. Edit `categories.json` on the data volume - each entry is
`{ "key", "label", "color", "interest" }`, in priority order:

```json
[ { "key": "cooking", "label": "Cooking", "color": "#ff8800",
    "interest": "Cooking -- recipes, technique, equipment, restaurants" } ]
```

The `interest` line is the important part: it's woven into the LLM's scoring prompt, so write a real
sentence about what you actually want in that bucket - that's what makes relevance scoring good. Keys
must be lowercase alphanumeric. A fresh deploy seeds the file from
[categories.sample.json](categories.sample.json) (the maintainer's defaults); changes apply on the
next run. The pipeline, feed validation, and UI all read from this one place.

---

## HTTP API

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/` | The dashboard (`digest.html`) |
| `GET` | `/digest.json` | Current ranked digest |
| `GET` / `PUT` | `/state` | Read / save user state (read, later, history, sourceStats) |
| `GET` / `PUT` | `/settings` | Read schema + values / save runtime settings |
| `GET` / `POST` / `DELETE` | `/feeds` | List / add / remove feeds |
| `GET` | `/feeds/check?url=` | Validate a feed URL before adding |
| `GET` | `/models` | Ollama models available on the host |
| `GET` | `/status` | Live pipeline run status (for the "updating…" pill) |
| `GET` | `/runs` | Recent pipeline run log (powers Settings → Runs) |
| `GET` | `/categories` | Configured categories (key / label / color / interest) |
| `GET` | `/search?q=&threshold=` | Semantic search - ranked digest matches by embedding similarity |
| `POST` | `/refresh` | Trigger a pipeline run now |

---

## Data & persistence

Mutable data lives on the `cruxwire-data` Docker volume (`/data` in the container):
`state.json`, `feeds.json`, `digest.json`, `settings.json`, `runs.json`, `categories.json`,
`embeddings.json` (article vectors for semantic search, rewritten each run). The app code is baked into the image, so
rebuilds preserve your data; new settings keys fall back to their defaults automatically. Back up the
volume to preserve your feed list, read history, and learned preferences.

---

## Development

No dependencies - run the server directly against local files:

```bash
OLLAMA_HOST=http://localhost:11434 \
DIGEST_FILE=./digest.json STATE_FILE=./state.json FEEDS_FILE=./feeds.json \
SETTINGS_FILE=./settings.json \
STATIC_DIR=. SEED_FEEDS=./feeds.sample.json \
python server.py
```

Then open `http://localhost:8090/`. To work on the layout without a live Ollama/feed setup, point
`FEEDS_FILE` at a nonexistent path (the scheduled run no-ops) and drop a real `digest.json` in place.

---

## Project layout

| File | Role |
|------|------|
| [server.py](server.py) | HTTP server: UI, state/settings/feeds API, digest serving, state pruning |
| [pipeline.py](pipeline.py) | Ingestion pipeline + scheduler: fetch → carry-forward → score → cluster → retain |
| [settings.py](settings.py) | Runtime-settings schema (drives the Settings form), validation, persistence |
| [digest.html](digest.html) | Single-file frontend: views, ranking display, affinity learning, layout |
| [TUNING.md](TUNING.md) | Every adjustable knob, interactions, and recipes |

---

## Contributing & security

It's a personal project, but issues and pull requests are welcome - see
[CONTRIBUTING.md](CONTRIBUTING.md). To report a security issue, see [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE).
