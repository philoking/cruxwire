# Contributing to Cruxwire

Thanks for your interest! First, expectations: **Cruxwire is a personal project** I build for my own
use and share in case it's useful to others. Issues and pull requests are welcome, but there is **no
SLA** - I may be slow, and I may decline changes that don't fit the project's direction. No hard
feelings either way.

## Project principles (please keep these intact)

- **Zero runtime dependencies.** The app is pure Python **standard library** plus a single static
  `digest.html` (vanilla JS - no framework, no build step). Please don't add pip/npm dependencies;
  "no dependencies" is a feature, not an oversight.
- **Single container, local-first.** Everything runs in one process against a local Ollama - no cloud
  services. Keep it that way.
- **Simple over clever.** Readable, boring code that matches the surrounding style beats abstraction.

## Before you start

- For anything non-trivial, **open an issue first** to discuss the idea - it saves us both wasted
  effort if it's not a fit.
- Small, obvious fixes (typos, clear bugs) can go straight to a PR.

## Dev setup

No dependencies - run the server directly against local files (see the README's
[Development](README.md#development) section):

```bash
OLLAMA_HOST=http://localhost:11434 \
DIGEST_FILE=./digest.json STATE_FILE=./state.json FEEDS_FILE=./feeds.json \
SETTINGS_FILE=./settings.json STATIC_DIR=. SEED_FEEDS=./feeds.sample.json \
python server.py
```

Then open `http://localhost:8090/`.

## Style

- **Python:** standard library only; target 3.11+. Match the existing modules - small functions, clear
  names, and comments that explain *why* (not *what*).
- **Frontend:** everything lives in `digest.html` - vanilla JS, no framework. Follow the existing
  patterns (render functions, the schema-driven Settings UI, etc.).
- **Commits:** clear, present-tense subject; explain the *why* in the body for non-obvious changes.

## Testing

There's no automated test suite - verify changes by running the app (Docker, or the direct-run command
above) and exercising the affected flow. Say what you tested in the PR.

## Design docs

- [TUNING.md](TUNING.md) - every adjustable knob and how they interact.
- [SECURITY.md](SECURITY.md) - the trust model and how to report a vulnerability.
