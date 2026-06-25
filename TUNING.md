# Tuning guide

Everything you can adjust to change what Cruxwire shows you and how it behaves, what each knob
does, and how the knobs interact. Skip to [Recipes](#recipes) if you just want "I want X, change Y".

## How a story gets ranked and kept

Each run produces a ranked digest; your device renders it. Five stages decide where a story lands -
and whether it survives to the next run:

```
 1. base score      Ollama reads the article → 0.0-10.0 relevance
 2. + cluster boost  more sources covering the same story → higher (rep only)
 3. + taste boost    closeness to what you save/open → higher
 4. × source affinity sources you engage with float up, dismissed ones sink   (per device)
 5. retention        which stories carry to the next run, and for how long
```

The **pipeline** (server side) computes stages 1-3 and stores them in `digest.json`. Stage 4 is
applied **in the browser** (it's per-device and learned locally). The **final on-screen order** is:

```
effectiveScore = (base_score + cluster_boost + taste_boost) × source_affinity
```

**Retention** (stage 5) decides which stories exist in the digest at all. It ranks by the
pipeline-side number only - `base_score + cluster_boost + taste_boost`, *without* source affinity,
since affinity lives on each device. So a story you'd never see ranked highly because you dismiss its
source can still be *retained* server-side; affinity only reorders/sinks it in your view.

---

## Where knobs live (four tiers)

| Tier | Where | Takes effect | Examples |
|------|-------|--------------|----------|
| **Settings UI** | Settings view → `settings.json` on the volume | next pipeline run, **no restart** | ranking, ingestion, schedule, retention, blocklist, models |
| **Deploy env** | `docker-compose.yaml` / `.env` | **container restart** | `OLLAMA_HOST`, `PORT`, file paths, timeouts |
| **Frontend constants** | edit [`digest.html`](digest.html), rebuild image | rebuild + reload | source-affinity weights, layout balance |
| **Pipeline constants** | env or edit [`pipeline.py`](pipeline.py), restart | restart | taste-vector size/weight |

Env vars only seed the **defaults**; the Settings UI stores overrides in `settings.json`. Changing a
Settings-tier value in the UI wins over its env default until you clear it.

---

## 1. Settings UI knobs

Editable live in the **Settings** view. Each takes effect on the next pipeline run.

### Models

| Knob | Default | Notes |
|------|---------|-------|
| **Chat model** (`ollama_model`) | `qwen2.5:latest` | Scores, summarises, categorises each article. Must be pulled on your Ollama host. |
| **Embedding model** (`embed_model`) | `nomic-embed-text` | Embeds titles+summaries for clustering, taste, and semantic blocking. **Changing this changes the vector space** - `sim_threshold` and `block_topic_threshold` are calibrated to the model; re-tune them if you switch. |

### Clustering & ranking

| Knob | Default | Range | What it does |
|------|---------|-------|--------------|
| **Merge similarity** (`sim_threshold`) | `0.74` | 0.5-0.99 | Cosine similarity above which two articles collapse into one story (any source). **Lower = merges more** (fewer duplicate cards, risk of merging unrelated stories); higher = stricter. |
| **Boost cap** (`boost_cap`) | `1.5` | 0-5 | Maximum rank points a story gains from cross-source coverage. |
| **Boost strength** (`boost_k`) | `0.6` | 0-5 | `boost = min(cap, k · log2(sources))`. Higher = breadth of coverage counts for more. With defaults: 2 sources → +0.6, 4 → +1.2, 8+ → capped at 1.5. |
| **Personalization strength** (`taste_weight`) | `1.0` | 0-3 | Max points a perfectly on-taste article gains. **0 disables personalization.** Higher = your save/open history pulls matching stories up harder. |

> Scale context: base scores run 0-10, so a boost cap of 1.5 + taste 1.0 can move a story up to ~2.5
> points - enough to lift a well-covered, on-taste 7 above a lone 9, but not to bury relevance.

### Ingestion

| Knob | Default | Range | What it does |
|------|---------|-------|--------------|
| **Lookback (hours)** (`lookback_hours`) | `36` | 1-168 | How far back a **fresh** feed item is first discovered. Does **not** control how long stories stay - that's [retention](#4-retention). Items older than this are never pulled in the first place. |
| **Max articles / run** (`max_articles`) | `1024` | 16-5000 | Cap on fresh articles scored per run (newest first). A throughput/cost guard, rarely hit. |
| **Scoring workers** (`score_concurrency`) | `4` | 1-32 | Parallel Ollama requests. Raise to speed up runs if your Ollama host has headroom; lower if it serialises or OOMs. |

### Schedule

| Knob | Default | Range | What it does |
|------|---------|-------|--------------|
| **Active start hour** (`active_start_hour`) | `6` | 0-23 | Earliest local hour a scheduled run fires. |
| **Active end hour** (`active_end_hour`) | `22` | 0-23 | Latest local hour a scheduled run fires. |
| **Interval (hours)** (`interval_hours`) | `2` | 1-24 | Hours between runs inside the active window. |

Runs fire at minute 0 of every `interval` hours within `[start, end]` (default ≈ `0 6-22/2 * * *`),
plus once on container start. A run is skipped if one is already in progress.

### 4. Retention

This is the **inbox-stickiness** model: unread stories are carried forward across runs and pruned by
a rank-weighted lifespan held inside a `[floor, ceiling]` band.

| Knob | Default | Range | What it does |
|------|---------|-------|--------------|
| **Keep at least (stories)** (`retain_floor`) | `25` | 0-500 | Floor. While recent unread stories still exist, never let the inbox drop below this - even past their normal lifespan. **Stops the app going dry** on quiet days/weekends. |
| **Keep at most (stories)** (`retain_ceiling`) | `60` | 1-1000 | Ceiling. Never carry more than this many unread stories; over it, the lowest-ranked fall off first. **Stops the inbox flooding.** |
| **Min story lifespan (hours)** (`retain_base_ttl_hours`) | `24` | 1-720 | Lifespan of the **lowest-ranked** story before it can fall off. |
| **Max story lifespan (hours)** (`retain_max_ttl_hours`) | `72` | 1-720 | Lifespan of the **highest-ranked** story. Lifespan scales with rank between min and max. |
| **Absolute max age (hours)** (`retain_hard_max_age_hours`) | `120` | 1-1440 | Hard cutoff. Nothing older than this is ever kept, even to meet the floor. **Stops stale news lingering.** |
| **History retention (days)** (`history_retention_days`) | `3` | 1-60 | Keep today + this many prior calendar days of **viewed** articles in History. (Read Later is curated and never aged out.) |

**How a story's lifespan is computed.** Each run, every unread story gets a TTL scaled by its rank in
the current pool:

```
ttl = base_ttl + (max_ttl - base_ttl) × rank_percentile
```

The best story lives `max_ttl` (72h), the worst `base_ttl` (24h), everyone in between. A story's
**age** is its *freshest* coverage, so ongoing coverage of a developing story keeps it alive. Then
the band clamps the survivor count: over the ceiling, drop the lowest-ranked; under the floor, add
back the best *expired-but-not-yet-stale* stories; the hard cap overrides everything.

Precedence (highest wins): **hard max age → ceiling → TTL → floor**.

> Read stories are **vacated** (removed from the carried pool) so they don't consume the band. They
> survive only in History, which keeps its own self-contained copy and is unaffected by retention.

### Blocklist

| Knob | Default | Range | What it does |
|------|---------|-------|--------------|
| **Blocked keywords** (`block_keywords`) | empty | - | Drop any article whose **title** contains one of these (case-insensitive substring), on top of the built-in spam/deal filter. Fast, literal. |
| **Blocked topics (semantic)** (`block_topics`) | empty | - | Short phrases ("celebrity gossip"). Drop articles whose **meaning** is close to any phrase - matches the concept, not just the words. Keep phrases specific; broad terms over-block. |
| **Topic block sensitivity** (`block_topic_threshold`) | `0.50` | 0.42-0.70 | Cosine similarity above which an article counts as matching a blocked topic. **Lower = blocks more aggressively** (more false blocks). Calibrated for `nomic-embed-text`. |

A built-in title regex always runs first (drops coupon/deal/sale/giveaway/
sponsored spam, etc.) - edit `TITLE_BLOCKLIST` in [`pipeline.py`](pipeline.py) to change it.

---

## 2. Source-affinity learning (frontend constants)

Your clicks teach the app which **sources** you trust. Counters live per device in `sourceStats`
(persisted independently of read/later/history, so inbox hygiene never wipes preferences). Affinity
is a multiplier on `effectiveScore`:

```
affinity(source) = clamp(1 + 0.10·opens + 0.20·saves - 0.05·dismisses,  0.5, 2.0)
```

So a source you open and save floats toward 2× rank; one you repeatedly dismiss sinks toward 0.5×.
Counters **decay** so stale habits fade. Constants in [`digest.html`](digest.html):

| Constant | Default | Effect |
|----------|---------|--------|
| `WEIGHT_OPEN` | `0.10` | Rank lift per open. |
| `WEIGHT_SAVE` | `0.20` | Rank lift per Read-Later save (counts double an open). |
| `WEIGHT_DISMISS` | `0.05` | Rank penalty per dismiss (gentler than positive signals). |
| `AFFINITY_MIN` / `AFFINITY_MAX` | `0.5` / `2.0` | Clamp - a source can never fully vanish or dominate. |
| `LEARNING_DECAY_DAYS` | `7` | Apply decay at most once per this many days. |
| `LEARNING_DECAY_FACTOR` | `0.97` | Multiply every counter by this on decay (slow forgetting). |

These are **per device** and **not** in the Settings UI - change them by editing `digest.html` and
rebuilding. "Reset learning" in the UI zeroes the counters.

---

## 3. Layout balance (frontend constants)

How the magazine front page and the "Earlier this week" rail stay balanced. Constants at the top of
`render()` in [`digest.html`](digest.html):

| Constant | Default | Effect |
|----------|---------|--------|
| `FRONT_MIN` | `11` | Fewest stories that make a proper front page (hero of 5 + ~2 grid rows). If *today* has fewer, the best **earlier** stories are promoted up by ranking so no section sits empty. |
| `RAIL_MIN` | `6` | Floor on rail length when the grid is short. |

The rail is capped to `max(RAIL_MIN, gridCards.length)` so the single-column "Earlier this week"
tracks the 3-wide "Latest" grid's height instead of overrunning it; the overflow hides behind
**Show more / Show less**. Dismissing a card re-renders the view - vacated hero/grid slots promote
the next-ranked story and the rail pulls its reserve up at the end.

- Raise `FRONT_MIN` for a fuller front page (promotes earlier stories sooner on thin days).
- Raise `RAIL_MIN` if the rail feels too short next to a small grid.

---

## 4. Taste vector (pipeline constants)

How your taste centroid (the target `taste_boost` aims at) is built, in [`pipeline.py`](pipeline.py):

| Constant | Default | Effect |
|----------|---------|--------|
| `TASTE_MAX_ITEMS` | `40` | How many of your most-recent saved/opened items form the taste vector. More = smoother, slower-moving taste; fewer = snappier, more recency-driven. |
| `TASTE_SAVE_WEIGHT` | `2.0` | How much a **saved** item counts vs an **opened** one when averaging the taste centroid. |

`taste_weight` (Settings) sets *how hard* the centroid pulls; these set *what the centroid is*.

---

## 5. Deploy-time wiring (env only - restart required)

Infrastructure, deliberately **not** in the Settings UI (shown read-only there). Set in
`docker-compose.yaml` or `.env`; changes need a container restart.

| Var | Default | Purpose |
|-----|---------|---------|
| `OLLAMA_HOST` | `http://localhost:11434` | Ollama base URL (chat + embeddings). |
| `PORT` | `8090` | Container HTTP port. |
| `STATE_FILE` / `DIGEST_FILE` / `FEEDS_FILE` / `SETTINGS_FILE` | `/data/*.json` | Data paths (on the volume). |
| `STATIC_DIR` | `/app` | Where `digest.html`/`favicon.ico` are served from. |
| `SEED_FEEDS` | `/app/feeds.sample.json` | Feed list to seed on first run if `feeds.json` is missing. |
| `FEED_TIMEOUT` | `12` | Per-feed fetch timeout (s). |
| `PAGE_TIMEOUT` | `8` | Per-article page fetch timeout (s) for `og:image` + excerpt. |
| `OLLAMA_TIMEOUT` | `120` | Per-request Ollama timeout (s). |

Every Settings-tier knob *also* has an env default (`SIM_THRESHOLD`, `LOOKBACK_HOURS`, `RETAIN_*`,
etc.) - see [.env.example](.env.example). Those just seed the default; the UI overrides them.

---

## Recipes

**"It goes dry on weekends - I want more stories sitting around."**
Raise `retain_floor` (25 → 35) and/or `retain_max_ttl_hours` (72 → 96-120). The floor holds your
best unread stories alive even past their lifespan when little new is coming in.

**"Too cluttered - too much sitting in the inbox."**
Lower `retain_ceiling` (60 → 40) and/or `retain_base_ttl_hours` (24 → 12) so weak stories die faster.

**"I'm seeing the same story as separate cards."**
Lower `sim_threshold` (0.74 → 0.70) so near-duplicates merge. If unrelated stories start merging,
nudge it back up.

**"Surface more from sources/topics I actually read."**
Raise `taste_weight` (1.0 → 1.5-2.0). Affinity also handles this automatically as you open/save -
give it a few days. For a hard preference, the affinity weights in `digest.html` are the lever.

**"Cross-source coverage should matter more (or less)."**
Raise/lower `boost_k` and `boost_cap`. Higher = a story carried by many outlets jumps higher.

**"Stop a topic I never want to see."**
Add a specific phrase to **Blocked topics** (semantic) for concepts, or a literal string to **Blocked
keywords** for exact title matches. If too much slips through, lower `block_topic_threshold` slightly.

**"Refresh more often / only during work hours."**
Set `interval_hours` (e.g. 1) and `active_start_hour` / `active_end_hour`.

**"The 'Earlier this week' rail feels off."**
Tune `RAIL_MIN` (length floor) and `FRONT_MIN` (when earlier stories get promoted onto the front
page) in `digest.html`.

**"Runs are slow."**
Raise `score_concurrency` if your Ollama host can take it; or use a smaller `ollama_model`. Note
page-fetch + scoring per fresh article dominates run time; carried stories only re-embed.
