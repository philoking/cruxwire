#!/usr/bin/env python3
"""Best-effort corpus archive writer for the offline Clustering Bench.

Behind an off-by-default flag, this appends a copy of each run's retained,
clustered articles -- title + summary + the production embedding + metadata --
to an append-only JSONL archive on a shared volume. A separate, private tool
(the Clustering Bench) reads that archive read-only to recreate and re-cluster
past windows for tuning. This module is the *only* touch point: it imports
nothing app-specific beyond stdlib, has one call site (in pipeline.run_once),
and is trivially disabled (flag off) or removed (delete this file + the call).

Pure stdlib. Best effort: any failure here is logged and swallowed -- it can
never raise into the ingestion path or drop a real article.

The record format is owned by the Clustering Bench repo (its schema/ package);
this writer conforms to schema_version 3. Evolve additively: add nullable
fields, never repurpose existing ones, and bump SCHEMA_VERSION in lockstep with
the bench's definition.
"""
import json
import os
import tempfile
from datetime import datetime, timezone

# Conforms to the bench's schema/corpus_schema.py SCHEMA_VERSION.
SCHEMA_VERSION = 3

# Deploy-time wiring (env only, like the other paths in pipeline.py). Capture is
# a deploy setting, not a per-session toggle: a window with missing stories
# cannot be faithfully recreated, so it is effectively all-or-nothing per day.
CAPTURE_ENABLED = os.environ.get('BENCH_CAPTURE_ENABLED', '').strip().lower() \
    in ('1', 'true', 'yes', 'on')
# The shared volume, mounted read-write here and read-only by the bench.
CORPUS_DIR = os.environ.get('BENCH_CORPUS_DIR', '/shared/corpus')


def _block_id(dt):
    """The 2-hour block an instant falls in, e.g. 08:00-09:59 -> '0800'."""
    return '%02d00' % (dt.hour // 2 * 2)


def _record(article, embedding, now, embed_model, prod_params):
    """One corpus row from an enriched, clustered article. Embedding comes from
    the run's emb_index (it is stripped off the article before the digest is
    written), so we pass it in rather than read article['embedding']."""
    return {
        'schema_version': SCHEMA_VERSION,
        'article_id': article.get('id'),
        'day': now.date().isoformat(),
        'block_id': _block_id(now),
        'source': article.get('source'),
        'title': article.get('title'),
        # title + "\n" + summary is exactly the text production embedded, so the
        # bench can re-embed candidate models on the same input. Body is not kept.
        'summary': article.get('summary'),
        'url': article.get('url'),
        'published_at': article.get('published_at'),
        # cruxwire keeps no per-article ingest time; the run timestamp is it.
        'ingested_at': now.isoformat(),
        'score': article.get('score'),
        'has_image': bool(article.get('image')),
        'body_text': None,
        'embedding': embedding,
        'embedding_model': embed_model,
        'embedding_model_version': '',
        'entities': None,
        # The baseline recorded, not inferred: the cluster cruxwire assigned this
        # article in this run (its cluster_id == the representative article id).
        'prod_cluster_id': article.get('cluster_id'),
        'prod_params': prod_params,
    }


def archive_articles(articles, emb_index, embed_model, cfg, log=print):
    """Append this run's retained articles to the corpus archive. Best-effort:
    no-op when the flag is off, and never raises (the caller need not guard).

    articles: the post-retention `scored` list (what the reader sees).
    emb_index: {article_id: embedding} captured before embeddings are stripped.
    cfg:       the settings dict, for the clustering-param snapshot.
    """
    if not CAPTURE_ENABLED:
        return
    try:
        now = datetime.now(timezone.utc)
        prod_params = {
            'sim_threshold': cfg.get('sim_threshold'),
            'boost_cap': cfg.get('boost_cap'),
            'boost_k': cfg.get('boost_k'),
        }
        rows = [
            _record(a, emb_index.get(a.get('id')), now, embed_model, prod_params)
            for a in articles if a.get('id')
        ]
        if not rows:
            return

        # Layout: corpus/day=YYYYMMDD/block=HHMM/part-HHMMSS.jsonl. One file per
        # run (timestamped), so appends never interleave and adding a file takes
        # no lock the always-on reader holds.
        part_dir = os.path.join(
            CORPUS_DIR, 'day=%s' % now.strftime('%Y%m%d'),
            'block=%s' % _block_id(now))
        os.makedirs(part_dir, exist_ok=True)
        path = os.path.join(part_dir, 'part-%s.jsonl' % now.strftime('%H%M%S'))

        # Write to a temp file in the same dir, then atomically replace, so the
        # bench never sees a half-written part.
        fd, tmp = tempfile.mkstemp(dir=part_dir, suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as fh:
                for r in rows:
                    fh.write(json.dumps(r) + '\n')
            os.replace(tmp, path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        log('corpus archive: wrote %d rows -> %s' % (len(rows), path))
    except Exception as exc:
        # Never let archiving affect the run.
        try:
            log('corpus archive write failed (ignored): %s' % exc)
        except Exception:
            pass
