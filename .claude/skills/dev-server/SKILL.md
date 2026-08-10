---
name: dev-server
description: Start kontinuum's local dev server (make dev) in the background, syncing .env from the original repo when run inside a linked worktree, and auto-restarting only on a port-8080 outage.
---

# Dev server

Starts the local dev stack (`make dev`: postgres + air hot-reload + Caddy TLS
proxy, per `compose.yaml`) in the background and keeps it healthy.

## 1. Sync `.env` when in a linked worktree

`make dev`'s `kontinuum-dev` service loads `.env` via `env_file`, but `.env`
is gitignored (`.env.example` is the tracked template) so it does not exist
in freshly created worktrees.

Detect a linked worktree by comparing git-dir to git-common-dir — they match
in the main worktree and differ (git-dir gains a `/worktrees/<name>` suffix)
in a linked one:

```bash
git_dir=$(git rev-parse --path-format=absolute --git-dir)
common_dir=$(git rev-parse --path-format=absolute --git-common-dir)
toplevel=$(git rev-parse --show-toplevel)
```

If `$git_dir` != `$common_dir`, this is a linked worktree. The original repo
root (e.g. `~/repos/kontinuum`) is `dirname "$common_dir"`. If `.env` is
missing from `$toplevel` but present at `<original-root>/.env`, copy it in:

```bash
original_root=$(dirname "$common_dir")
if [ ! -f "$toplevel/.env" ] && [ -f "$original_root/.env" ]; then
  cp "$original_root/.env" "$toplevel/.env"
fi
```

Never overwrite an `.env` that already exists in the worktree — it may hold
worktree-specific edits.

## 2. Start the server in the background

```bash
make dev
```

Run this via the Bash tool with `run_in_background: true` (it's
`docker compose --profile dev up`, which stays attached and streams logs —
exactly what's needed for step 3). Keep the returned task ID.

Confirm it comes up by polling the health endpoint for up to ~30s:

```bash
until curl -sf http://localhost:8080/healthz >/dev/null; do sleep 1; done
```

## 3. Hot reload is automatic — don't intervene

`air` inside `kontinuum-dev` watches for file changes and rebuilds/restarts
the server on its own. A restart you triggered will show up as normal
`building...` / `running...` lines in the task log — this is expected and
needs no action.

**Never manually restart the server just because you see it rebuild.** Only
these two cases call for action:

- **Port 8080 stops responding and stays down** — restart immediately,
  without asking for confirmation (see step 4).
- **Anything else** (compile errors, postgres/proxy issues, the user wants a
  restart after editing `.env` or `compose.yaml`, which air doesn't watch) —
  explain what's wrong and ask before restarting.

## 4. Auto-restart only on a real port-8080 outage

A rebuild briefly drops the port for a couple of seconds — that's not an
outage. Only treat it as down if `/healthz` keeps failing well past a normal
rebuild:

```bash
fails=0
while true; do
  if curl -sf http://localhost:8080/healthz >/dev/null; then
    fails=0
  else
    fails=$((fails + 1))
    if [ "$fails" -ge 10 ]; then  # ~10s of sustained failure
      break
    fi
  fi
  sleep 1
done
```

When that fires, restart just the affected service (leave postgres/proxy
running) and re-verify health, with no confirmation prompt:

```bash
docker compose --profile dev restart kontinuum-dev
until curl -sf http://localhost:8080/healthz >/dev/null; do sleep 1; done
```

Tell the user afterward that it dropped off port 8080 and was restarted.

Use the Monitor tool for this watch loop (persistent, one notification per
restart) rather than blocking the conversation on it.

## Stopping

```bash
make dev-down       # stop, keep volumes (postgres data, caches)
make dev-clean       # stop and wipe volumes
```
