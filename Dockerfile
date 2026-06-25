FROM python:3.12-slim

# tzdata so the scheduler honours TZ (set in compose). Without it the slim
# image has no zoneinfo and the container runs in UTC, firing runs in the
# wrong local window.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# App code + bundled assets. Mutable data lives on the /data volume, not here.
COPY server.py pipeline.py settings.py corpus_archive.py digest.html \
     favicon.ico favicon-16x16.png favicon-32x32.png apple-touch-icon.png logo-mark.png \
     feeds.sample.json categories.sample.json ./

ENV STATE_FILE=/data/state.json \
    DIGEST_FILE=/data/digest.json \
    FEEDS_FILE=/data/feeds.json \
    SETTINGS_FILE=/data/settings.json \
    RUNS_FILE=/data/runs.json \
    EMBEDDINGS_FILE=/data/embeddings.json \
    CATEGORIES_FILE=/data/categories.json \
    STATIC_DIR=/app \
    SEED_FEEDS=/app/feeds.sample.json \
    SEED_CATEGORIES=/app/categories.sample.json \
    PORT=8090

VOLUME /data
EXPOSE 8090

# No build step, no dependencies — pure stdlib.
CMD ["python", "server.py"]
