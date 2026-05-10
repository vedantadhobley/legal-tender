# Deploy notes — what changes outside this repo

The application code is fully contained in this repo. The host-level
infrastructure changes needed to run legal-tender on luv live in the
workspace's `~/workspace/proxy/` stack — they aren't tracked here because
they're cross-cutting, but they're listed here so the deploy is
reproducible from one place.

legal-tender has **no public-facing component** (no Cloudflare tunnel
ingress); the Dagster and Arango admin UIs are tailnet-only. Once the
project surfaces a candidate-summary UI, it follows the workspace pattern
(consumed by vedanta-systems' portal, proxied as `/api/legal-tender/*`).

## 1. Caddy routes (luv)

The Caddy config on luv is split per-project. This project's routes live in:

```
~/workspace/proxy/caddy/caddy.d/legal-tender.caddy
```

Reference content (current source of truth is the file above):

```caddy
# ─── prod ──────────────────────────────────────────────────────────────────
http://legal-tender-prod-webserver.{$BASE_DOMAIN} { reverse_proxy legal-tender-prod-webserver:3000 }
http://legal-tender-prod-arango.{$BASE_DOMAIN}    { reverse_proxy legal-tender-prod-arango:8529 }

# ─── dev ───────────────────────────────────────────────────────────────────
http://legal-tender-dev-webserver.{$BASE_DOMAIN}  { reverse_proxy legal-tender-dev-webserver:3000 }
http://legal-tender-dev-arango.{$BASE_DOMAIN}     { reverse_proxy legal-tender-dev-arango:8529 }
```

After editing the file, reload Caddy without restarting the container:

```bash
docker exec proxy-caddy caddy reload --config /etc/caddy/Caddyfile
```

(The proxy stack uses a directory bind mount, so atomic-write edits to
files inside `caddy/` flow through and `caddy reload` picks them up.
See `~/workspace/proxy/README.md` for the gotcha mechanics.)

## 2. Cross-project network dependency

legal-tender's arangodb is on the shared `luv-prod` / `luv-dev` docker
network so other workspace projects (e.g., the eventual vedanta-systems
candidate-browser component) can reach it. These networks must exist:

```bash
docker network create luv-prod
docker network create luv-dev
docker network create proxy
```

(One-time on a fresh node; idempotent if already created.)

## 3. LLM dependency

Wikidata corporate-identity resolution and the candidate-narrative work
reach the local LLM endpoints on the joi node via tailnet split-DNS:

- `EMBEDDING_HOST=llama-embed.joi` (embeddings, Qwen3-Embedding-8B)

Set via `.env` (see `.env.example`). These resolve via the Tailscale
split-DNS rule for the `joi` domain (configured once in the Tailscale
admin console). No fallback — if joi is offline, embedding-dependent
assets fail.

## 4. Bring up

```bash
cd ~/workspace/dev/legal-tender
cp .env.example .env
$EDITOR .env                                  # set passwords + API keys
docker compose -f docker-compose.yml up -d --build         # prod
# or
docker compose -f docker-compose.dev.yml up -d --build     # dev
```

## 5. Verify

```bash
curl -sI http://legal-tender-prod-webserver.luv/server_info
curl -sI http://legal-tender-prod-arango.luv/
curl -sI http://legal-tender-dev-webserver.luv/server_info
curl -sI http://legal-tender-dev-arango.luv/
```

The Dagster healthcheck path is `/server_info`; Arango returns a redirect
to its web UI from `/`.
