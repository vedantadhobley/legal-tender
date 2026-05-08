# Per-Project Second Brain — Setup Guide

A self-hosted, multi-project knowledge system that combines:

1. **Karpathy-style in-repo agent context** (`AGENTS.md` / `CLAUDE.md`) — the LLM-readable spec lives next to the code
2. **Obsidian** as the human-facing vault for browsing, linking, and graph view
3. **Local RAG** over the vault using the existing llama.cpp endpoints on `joi`
4. **MCP write-back** so Claude Code can update the brain as it works

This document is the implementation guide. `legal-tender` is the proving ground — the same recipe is meant to be applied to `found-footy`, `spin-cycle`, and other workspace repos.

---

## 1. The two big ideas, in plain language

### "Context engineering" (Karpathy's term)

> *"Context engineering is the delicate art and science of filling the context window with just the right information for the next step."*
> — [Andrej Karpathy, June 25, 2025](https://x.com/karpathy/status/1937902205765607626)

The practical implication: the markdown files inside your repo *are* the LLM's working memory for that project. An agent (Claude Code, Codex, Cursor, etc.) reads them at session start and updates them as part of its work. The discipline is making sure they stay current and structured. Simon Willison's [June 2025 essay](https://simonwillison.net/2025/jun/27/context-engineering/) is the cleanest framing of this.

### `AGENTS.md` is now the open standard

`AGENTS.md` ([agents.md](https://agents.md/)) emerged in 2025 as the cross-vendor file format for agent context. It is now stewarded by the **Linux Foundation's Agentic AI Foundation**, with native support in GitHub Copilot (added [August 2025](https://www.infoq.com/news/2025/08/agents-md/)), OpenAI Codex, Cursor, Google Jules/Gemini, Factory, Amp, Windsurf, Zed, and RooCode.

**Anthropic is the holdout.** Claude Code still reads only `CLAUDE.md` ([issue #6235](https://github.com/anthropics/claude-code/issues/6235), 3k+ upvotes, no official engagement as of April 2026). The community workaround is universal:

```bash
mv CLAUDE.md AGENTS.md && ln -s AGENTS.md CLAUDE.md
```

This is the very first thing to do per repo.

### Anthropic's `CLAUDE.md` precedence (worth knowing)

Per [code.claude.com/docs/en/memory](https://code.claude.com/docs/en/memory), Claude Code reads memory files in this order (highest precedence first):

1. `/etc/claude-code/CLAUDE.md` — enterprise
2. `./CLAUDE.md` — project-level, team-shared
3. `./.claude/rules/*.md` — scoped rules with YAML frontmatter
4. `~/.claude/CLAUDE.md` — your personal global
5. `./CLAUDE.local.md` — project-local, gitignored
6. Auto-memory (`~/.claude/projects/<repo>/memory/`)

`@path/to/file.md` import syntax is supported, so `AGENTS.md` can split into modular files.

---

## Architecture decision: brain stack ≠ monitor stack

This is worth getting right early. You already have a `~/workspace/monitor/` stack (Prometheus, Grafana, Loki, cadvisor, Portainer) — coherent, single-purpose, observability. The brain stack is a different concern: knowledge management, read+write, slower-cadence, different security posture. Keep them separate:

```
~/workspace/
  monitor/         ← observability (existing)
  obsidian/        ← brain stack (vault + Khoj + Open WebUI + basic-memory)
  homepage/        ← single dashboard linking to all webUIs (NEW, optional)
  dev/<project>/   ← repo source-of-truth for AGENTS.md + docs/
  data/<project>/  ← bind-mounted persistent data per project
```

**The "everything in one place" experience comes from a homepage dashboard, not co-location.** Tools like [Homepage](https://gethomepage.dev/) (current favorite), [Dashy](https://dashy.to/), or [Homer](https://github.com/bastienwirtz/homer) are 1-container Docker setups with YAML config that give you `https://<tailnet>/` showing tiles for Grafana, Khoj, Open WebUI, ArangoDB UI, Dagster UI, Portainer, anything else — clickable from any device on the tailnet. That gets you the unified entry point without forcing the brain into the monitoring stack.

## 2. Architecture target

```
┌────────────────────────────────────────────────────────────────┐
│  Per-repo context layer  (lives in git, source of truth)       │
│                                                                │
│  ~/workspace/dev/legal-tender/                                 │
│    AGENTS.md         ← real file                               │
│    CLAUDE.md         ← symlink to AGENTS.md                    │
│    docs/                                                       │
│      PIPELINE.md, FEC.md, SECOND_BRAIN.md, ...                 │
│                                                                │
│  Same shape in every other repo: found-footy, spin-cycle, etc. │
└─────────────────────────┬──────────────────────────────────────┘
                          │  symlinks (read-only union)
                          ▼
┌────────────────────────────────────────────────────────────────┐
│  Brain vault             (single Obsidian vault, the "view")   │
│                                                                │
│  ~/workspace/obsidian/brain/                                   │
│    .obsidian/            ← Obsidian config (NOT in any repo)   │
│    personal/             ← cross-project notes, daily logs     │
│    projects/                                                   │
│      legal-tender/   → ~/workspace/dev/legal-tender/docs/      │
│      found-footy/    → ~/workspace/dev/found-footy/docs/       │
│      spin-cycle/     → ~/workspace/dev/spin-cycle/docs/        │
└─────────────────────────┬──────────────────────────────────────┘
                          │
       ┌──────────┬───────────────┬──────────────────┐
       ▼          ▼               ▼                  ▼
┌────────────┐ ┌──────────────┐ ┌────────────────┐ ┌──────────────────┐
│  Khoj      │ │ Open WebUI   │ │ basic-memory   │ │  Quartz (opt'l)  │
│  (Docker)  │ │ (Docker)     │ │ (MCP server)   │ │  static publish  │
│  vault RAG │ │ general LLM  │ │ agent writes   │ │  of vault → web  │
│  :3006     │ │ chat :3007   │ │ MCP :3008      │ │                  │
└────────────┘ └──────────────┘ └────────────────┘ └──────────────────┘
       │              │                 │
       └──────┬───────┘                 │
              ▼                         ▼
   ┌──────────────────────┐  ┌──────────────────────┐
   │  Existing on joi:    │  │  Claude Code session │
   │  :3101 chat (Qwen)   │  │  uses MCP to read+   │
   │  :3103 embeddings    │  │  write notes during  │
   └──────────────────────┘  │  development         │
                             └──────────────────────┘

              ┌──────────────────────┐
              │  Sync to laptop/phone│
              │  via Syncthing over  │
              │  tailnet             │
              └──────────────────────┘

              ┌──────────────────────┐
              │  Homepage dashboard  │
              │  (~/workspace/       │
              │   homepage/)         │
              │  links to all UIs    │
              └──────────────────────┘

              ┌──────────────────────┐
              │  Sync to laptop/phone │
              │  via Syncthing over  │
              │  tailnet              │
              └──────────────────────┘
```

**Three layers, one source of truth:** the markdown lives in each repo's `docs/`. The vault is a thin symlink-based view. The RAG and MCP layers operate on the vault but never own the data.

---

## 3. Implementation phases

The phases are ordered by *value-per-effort*, so you can stop after any phase and still have improved your workflow.

| Phase | What | Time to set up | Stop here if… |
|---|---|---|---|
| 1 | `AGENTS.md` + `CLAUDE.md` symlink, structured `docs/` | 30 min | You just want better agent context |
| 2 | Brain vault with symlinked projects | 30 min | You want to *browse* across projects but not chat with the vault |
| 3 | Syncthing over tailnet | 30 min | You only ever work from this machine |
| 4 | Khoj + Open WebUI containers (web UI for tailnet) | 1-2 hours | You don't want browser-based chat at all |
| 5 | basic-memory MCP for write-back | 1 hour | You don't want Claude writing notes back into the brain |
| 6 | Quartz publishing (optional) | 1 hour | You don't need a web view |

Total weekend budget: **~5 hours active**, lots of waiting room.

---

## 4. Phase 1 — Per-repo agent context

**Do this for every repo, including legal-tender.**

### 4a. Bootstrap the file pair

In each repo:

```bash
cd ~/workspace/dev/legal-tender
# If you already have CLAUDE.md (we do):
mv CLAUDE.md AGENTS.md
ln -s AGENTS.md CLAUDE.md
git add AGENTS.md CLAUDE.md
git commit -m "chore: rename CLAUDE.md → AGENTS.md, symlink for Claude Code compat"
```

If a repo doesn't have one yet, generate a starter via Claude Code's `/init` slash command.

### 4b. Decide what belongs in `AGENTS.md` vs `docs/`

A useful split:

- **`AGENTS.md` (root, terse, ~200 lines max):** tone, repo-specific conventions, "do/don't" rules, build/test commands, the absolute minimum to onboard an agent. Imports the bigger docs via `@docs/PIPELINE.md` syntax.
- **`docs/<TOPIC>.md`:** the substantive content. Per-feature, per-subsystem. These are the human-readable docs that double as agent context.
- **`CLAUDE.local.md` (gitignored):** machine-specific paths, secrets references, in-progress scratchpad. *Never committed.*

### 4c. Enforce updates with a hook

The Karpathy discipline is "agent must update docs as part of its work, not after." The cheapest way to enforce this is a Claude Code hook that nags on stop:

```jsonc
// ~/.claude/settings.json (or per-project .claude/settings.json)
{
  "hooks": {
    "Stop": [
      {
        "matcher": "",
        "hooks": [
          {
            "type": "command",
            "command": "git diff --name-only HEAD | grep -qE '^(src|lib|app)/' && ! git diff --name-only HEAD | grep -qE '^(AGENTS\\.md|docs/)' && echo '⚠️  Code changed but no docs updated. Update AGENTS.md or docs/ before stopping.' >&2"
          }
        ]
      }
    ]
  }
}
```

Tweak the path patterns per repo. The hook prints to stderr, which Claude Code surfaces as a warning.

---

## 5. Phase 2 — Build the brain vault

```bash
mkdir -p ~/workspace/obsidian/brain/{personal,projects}

# Symlink each repo's docs into the vault
for repo in legal-tender found-footy spin-cycle; do
  ln -s ~/workspace/dev/$repo/docs ~/workspace/obsidian/brain/projects/$repo
done

# Optional: also symlink each repo's AGENTS.md so it's reachable from the vault
for repo in legal-tender found-footy spin-cycle; do
  ln -s ~/workspace/dev/$repo/AGENTS.md ~/workspace/obsidian/brain/projects/$repo-AGENTS.md
done
```

On your laptop (where Obsidian actually runs — see Phase 3 first), open `~/workspace/obsidian/brain/` as a vault. The `.obsidian/` config dir gets created on first launch and lives outside any repo, so it doesn't pollute git.

**Why symlinks, not copies:** the markdown stays version-controlled inside each repo. When Claude Code updates `docs/PIPELINE.md` during a session, the change shows up in the vault automatically. No sync layer between repo and vault.

**Why a single vault, not per-repo vaults:** Obsidian only opens one vault at a time. A single vault gets you graph view spanning projects, vault-wide search, and one plugin/theme config. The [Obsidian forum consensus](https://forum.obsidian.md/t/one-vault-vs-multiple-vaults/1445) is overwhelmingly single-vault for technical users with multiple projects.

**Caveat:** Obsidian's file watcher across symlinks works fine on Linux but is flaky on macOS/Windows. Since the server is Linux and your laptop is Linux, fine.

---

## 6. Phase 3 — Sync the vault across machines

You cannot meaningfully run Obsidian "on the server" — it's an Electron desktop app. Two real options for getting it on your laptop and phone:

### Syncthing (recommended)

- Free, no cloud, works over tailnet
- File-level sync; eventually consistent
- Setup: install Syncthing on each device, add `~/workspace/obsidian/brain/` as a shared folder, accept the share on the other end
- Failure mode: simultaneous offline edits → sync-conflict files (rare in practice)

For Linux + Tailscale, this is the lowest-drama option. The [Syncthing-for-Obsidian guide](https://seansusmilch.github.io/posts/obsidian-syncthing-private-sync-guide/) walks through the specific config.

### obsidian-livesync + self-hosted CouchDB (heavier)

- Real-time CRDT-based sync, no conflict files
- Setup is fiddly: deploy CouchDB ([oleduc/docker-obsidian-livesync-couchdb](https://github.com/oleduc/docker-obsidian-livesync-couchdb)), configure the [livesync plugin](https://github.com/vrtmrz/obsidian-livesync), wire credentials
- Worth it if you have an active mobile workflow that needs instant convergence

**Skip "Obsidian in Docker" entirely.** Projects like [sytone/obsidian-remote](https://github.com/sytone/obsidian-remote) and [linuxserver/docker-obsidian](https://github.com/linuxserver/docker-obsidian) run the Electron app via KasmVNC and stream the desktop to your browser. It works but is laggy, awkward on mobile, and offers nothing over native-app + sync.

---

## 7. Phase 4 — Khoj + Open WebUI for chat (web UI from any device on tailnet)

Two complementary tools, both Docker, both plug into the joi LLM endpoints. They're not redundant — they serve different needs:

| Tool | Purpose | Access pattern |
|---|---|---|
| **Khoj** | Vault-aware second brain. Indexes the markdown, builds embeddings, answers questions grounded in your project docs. Has an Obsidian plugin AND a standalone web UI. | "Tell me what we decided about X in the legal-tender pipeline" |
| **Open WebUI** | General LLM frontend in a browser. Can also do RAG via its Knowledge feature, but its primary use is as a polished ChatGPT-style UI for chatting with joi's Qwen. Multi-user, very actively developed. | "Help me write a Python script" / "Explain this stack trace" — general assistant, not vault-grounded |

Run both. They share a Docker network and the same upstream LLMs. Together they cover (a) "chat with my brain" and (b) "general-purpose LLM browser interface", giving you a tailnet-accessible web UI for both modes from any device.

### Khoj

[Khoj](https://github.com/khoj-ai/khoj) explicitly accepts an OpenAI-compatible base URL for both chat and embeddings, supports multiple content sources, has an Obsidian plugin and a web UI, and runs in Docker.

### Compose stack — Khoj + Open WebUI together

Create `~/workspace/obsidian/docker-compose.yml`:

```yaml
name: obsidian-brain

services:
  # Khoj — vault-aware RAG (the "second brain" interface)
  khoj:
    image: ghcr.io/khoj-ai/khoj:latest
    container_name: brain-khoj
    ports:
      - "3006:42110"  # Host 3006 → container default 42110   # Khoj web UI + Obsidian plugin endpoint
    volumes:
      - khoj-config:/root/.khoj
      - khoj-models:/root/.cache/torch
      - ~/workspace/obsidian/brain:/data/brain:ro  # vault, read-only
    environment:
      KHOJ_ADMIN_EMAIL: vedanta1998@gmail.com
      KHOJ_ADMIN_PASSWORD: changeme
      KHOJ_DEBUG: "false"
    restart: unless-stopped
    networks: [brain]

  brain-postgres:
    image: ankane/pgvector:latest
    container_name: brain-postgres
    environment:
      POSTGRES_USER: khoj
      POSTGRES_PASSWORD: khoj
      POSTGRES_DB: khoj
    volumes:
      - khoj-pgdata:/var/lib/postgresql/data
    restart: unless-stopped
    networks: [brain]

  # Open WebUI — general-purpose LLM browser interface
  open-webui:
    image: ghcr.io/open-webui/open-webui:main
    container_name: brain-open-webui
    ports:
      - "3007:8080"   # Open WebUI
    volumes:
      - open-webui-data:/app/backend/data
    environment:
      # Point at joi llama.cpp's OpenAI-compatible endpoint
      OPENAI_API_BASE_URLS: "http://joi.tailf424db.ts.net:3101/v1"
      OPENAI_API_KEYS: "dummy"   # llama.cpp ignores
      # Disable Ollama lookup
      ENABLE_OLLAMA_API: "false"
      # Allow signup for first user, then disable
      ENABLE_SIGNUP: "true"
      WEBUI_AUTH: "true"
    restart: unless-stopped
    networks: [brain]

volumes:
  khoj-config:
  khoj-models:
  khoj-pgdata:
  open-webui-data:

networks:
  brain:
    driver: bridge
```

After `docker compose up -d`, you'll have:
- **Khoj at `http://<tailnet-name>:3006`** — vault chat + search (after admin config)
- **Open WebUI at `http://<tailnet-name>:3007`** — general LLM chat (Qwen via joi). First visitor signs up as admin, then turn off signup.

### Configure Khoj for joi LLMs

After bringing the stack up, navigate to `http://localhost:3006/server/admin/`. Add an **AI Model API**:

- Name: `joi-llama-cpp`
- API Base URL: `http://joi.tailf424db.ts.net:3101/v1`
- API Key: any non-empty string (llama.cpp ignores it)

Then add **Chat Model** entries pointing at this provider for the chat model and the embedding model (`:3103`). Khoj's [setup docs](https://docs.khoj.dev/get-started/setup/) cover the exact form fields.

### Add the brain vault

In Khoj's settings, add a content source pointing at `/data/brain`. It'll do an initial index (chunks + embeddings) and incremental re-index on file changes. Total index time for a few hundred markdown files is ~a minute.

### Optional: Khoj Obsidian plugin

Install the Khoj plugin from Obsidian's community plugins. Configure it to point at your local Khoj at `http://<server-tailscale-name>:3006`. You now get in-editor chat with the vault.

---

## 8. Phase 5 — basic-memory MCP for write-back

Phases 1-4 give you a *readable* brain. Phase 5 makes it *writable by the agent*.

[basic-memory](https://github.com/basicmachines-co/basic-memory) is an MCP server that exposes a markdown vault to MCP clients (Claude Desktop, Claude Code) as a structured memory tool. The agent can call `write_note`, `search`, `read_note`, etc. — and the notes are real markdown files that show up in Obsidian.

### Install

```bash
# Use uvx to run it as needed
pipx install basic-memory  # or: uv tool install basic-memory
basic-memory project add brain ~/workspace/obsidian/brain/personal/agent-notes
```

The agent's writes land in a dedicated subfolder (`personal/agent-notes/`) so they don't conflict with the symlinked project docs (which live in repos and shouldn't be edited from outside their repo).

### Wire to Claude Code

Add to `~/.claude/mcp.json`:

```json
{
  "mcpServers": {
    "basic-memory": {
      "command": "basic-memory",
      "args": ["mcp"]
    }
  }
}
```

Now during any Claude Code session you can ask "remember this for later" or "write a note about X to the brain" and the agent uses MCP tools to land it as a real markdown file. Cross-session continuity for free.

`basic-memory` v0.13+ supports multi-project switching/merging, so a single instance can manage multiple semantic projects ([release notes](https://github.com/basicmachines-co/basic-memory/releases/tag/v0.13.0)).

---

## 9. Phase 5.5 — Optional: Homepage dashboard at `~/workspace/homepage/`

If you want one URL on tailnet that links to every webUI you run (Grafana, Khoj, Open WebUI, Dagster, ArangoDB, Portainer, etc.):

```yaml
# ~/workspace/homepage/docker-compose.yml
services:
  homepage:
    image: ghcr.io/gethomepage/homepage:latest
    container_name: homepage
    ports:
      - "80:3000"   # the "front door" of your tailnet
    volumes:
      - ./config:/app/config
      - /var/run/docker.sock:/var/run/docker.sock:ro  # auto-discover containers
    restart: unless-stopped
```

Configure tiles in `~/workspace/homepage/config/services.yaml`. Auto-discovery via labels on each docker-compose can populate tiles automatically — see [gethomepage.dev docs](https://gethomepage.dev/configs/docker/).

This is the right "everything in one place" answer that does NOT require co-locating the brain with the monitor stack.

## 10. Phase 6 — Optional: publish via Quartz

If you want a web view of the brain (for sharing or reading on devices without Obsidian):

[Quartz 4](https://quartz.jzhao.xyz/) is a static-site generator that consumes an Obsidian vault and produces a site with wikilinks, graph view, full-text search, and backlinks intact. Free Obsidian Publish replacement.

```bash
git clone https://github.com/jackyzha0/quartz.git ~/workspace/obsidian/quartz
cd ~/workspace/obsidian/quartz
npm install
npx quartz create  # point at ~/workspace/obsidian/brain
npx quartz build --serve  # local preview
```

To publish behind your tailnet: `npx quartz build` and serve the output dir from nginx on `joi` or this box. Skip until you actually want a public/shared view — for solo use, Obsidian itself is enough.

---

## 11. What to skip (and why)

| Tempting but not worth it | Why |
|---|---|
| Running Obsidian itself in Docker (obsidian-remote, linuxserver/docker-obsidian) | KasmVNC streaming Electron is laggy, awkward on mobile. Native app + sync wins on every axis. |
| Multiple separate vaults | Obsidian only opens one at a time. Loses cross-project graph + search. Forum consensus is single-vault. |
| obsidian-git for sync | Merge conflicts on prose are unpleasant; mobile git is awkward. Use Syncthing or LiveSync. |
| Cloud Obsidian Sync | Defeats the self-hosted goal. |
| Smart Connections plugin instead of Khoj | Smart Connections is great for in-vault "related notes" but is single-vault and has no external API. Khoj covers more ground. |
| Reor as the chat-with-vault tool | Standalone Electron app, opinionated, less flexible than Khoj for our existing endpoint setup. |
| Building your own RAG pipeline from scratch | Khoj already does the chunking + embedding + retrieval + chat-orchestration. Don't rebuild. |

---

## 12. Concrete plan for legal-tender (proving ground)

Practical sequence to follow this weekend, using `legal-tender` as the test:

1. **Phase 1 here:**
   ```bash
   cd ~/workspace/dev/legal-tender
   # CLAUDE.md doesn't exist yet — generate via /init in Claude Code
   /init
   # Then:
   mv CLAUDE.md AGENTS.md
   ln -s AGENTS.md CLAUDE.md
   git add AGENTS.md CLAUDE.md && git commit -m "chore: adopt AGENTS.md, CLAUDE.md as symlink"
   ```

2. **Brain vault setup:**
   ```bash
   mkdir -p ~/workspace/obsidian/brain/{personal,projects}
   ln -s ~/workspace/dev/legal-tender/docs ~/workspace/obsidian/brain/projects/legal-tender
   ```

3. **Bring Obsidian up on the laptop**, open `~/workspace/obsidian/brain/`. You should immediately see `projects/legal-tender/` containing `PIPELINE.md`, `FEC.md`, `SECOND_BRAIN.md`, etc.

4. **Syncthing**: install on both ends, share the brain folder over tailnet. Done in 15 minutes.

5. **Khoj + Open WebUI**: bring up the docker-compose above. Configure Khoj's joi endpoints, index the vault. Sign up as the admin user in Open WebUI, then `ENABLE_SIGNUP=false`. Try a query in Khoj like *"what's the funding-channels model in legal-tender?"* — the answer should pull from `docs/PIPELINE.md`. Try Open WebUI for general chat with Qwen via joi.

6. **basic-memory**: install, point at `~/workspace/obsidian/brain/personal/agent-notes/`, wire to Claude Code MCP. Test by asking Claude to "save a note about today's storage relocation work" — verify the file appears in the vault.

7. **Onboard the next repo** (found-footy or spin-cycle) by repeating just steps 1-2. Khoj re-indexes automatically.

After step 5, you have a working personal RAG system over your legal-tender knowledge. After step 6, you have an agent that can write to your brain. After step 7, you have multi-project continuity.

---

## 13. Maintenance + housekeeping

- **Per-repo `docs/`** is the only source of truth. Edit there (or have Claude Code edit there). The vault reflects, doesn't store.
- **Personal notes** live in `~/workspace/obsidian/brain/personal/`. These aren't in any repo. Backed up via Syncthing replication.
- **Agent-written notes** live in `~/workspace/obsidian/brain/personal/agent-notes/` (basic-memory's home).
- **Khoj re-indexes** on filesystem changes. If indexing seems stale, restart the container.
- **Don't edit symlinked project docs from inside Obsidian** unless you mean to commit those changes to the underlying repo. Treat the `projects/` folder as read-mostly.

---

## Reference links

- [Karpathy — context engineering tweet](https://x.com/karpathy/status/1937902205765607626)
- [Simon Willison — context engineering essay](https://simonwillison.net/2025/jun/27/context-engineering/)
- [agents.md](https://agents.md/) — open standard
- [Anthropic CLAUDE.md docs](https://code.claude.com/docs/en/memory)
- [Khoj](https://github.com/khoj-ai/khoj) | [Khoj setup docs](https://docs.khoj.dev/get-started/setup/)
- [basic-memory](https://github.com/basicmachines-co/basic-memory)
- [obsidian-livesync](https://github.com/vrtmrz/obsidian-livesync) | [docker-obsidian-livesync-couchdb](https://github.com/oleduc/docker-obsidian-livesync-couchdb)
- [Quartz 4](https://quartz.jzhao.xyz/)
- [Syncthing-for-Obsidian guide](https://seansusmilch.github.io/posts/obsidian-syncthing-private-sync-guide/)
- [Obsidian forum: one vault vs multiple](https://forum.obsidian.md/t/one-vault-vs-multiple-vaults/1445)
- [forrestchang/andrej-karpathy-skills](https://github.com/forrestchang/andrej-karpathy-skills) — community distillation of Karpathy's agent-coding observations
