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

- **Port 8080 stops responding and stays down** past a rebuild's own
  window — restart without asking for confirmation (see step 4 for how long
  to wait, and for why a failing compile is not this case).
- **Anything else** (compile errors, postgres/proxy issues, the user wants a
  restart after editing `.env` or `compose.yaml`, which air doesn't watch) —
  explain what's wrong and ask before restarting.

## 4. Auto-restart only on a real port-8080 outage

A rebuild drops the port briefly — that's not an outage. Measured on this
project, a warm `air` rebuild is down for about **1 second** (~6s from edit
to healthy), but a cold-cache rebuild — after a dependency bump, a wiped
build cache, or heavy parallel `go build`/`go test` on the host — runs well
past ten. A threshold near the warm case restarts the server in the middle
of a perfectly healthy rebuild, which is worse than waiting: it throws away
the in-progress build and starts over.

So treat it as down only after **90 seconds** of sustained failure. A real
outage (crash loop, panic on boot) never recovers on its own, so waiting
longer costs nothing, while a false restart actively interrupts work.

A failing *compile* also holds the port down indefinitely, and restarting
the container rebuilds exactly the same broken code — check the logs before
restarting, and report a build error rather than looping on it (this is the
"compile errors → explain, don't restart" case from step 3):

```bash
fails=0
while true; do
  if curl -sf http://localhost:8080/healthz >/dev/null 2>&1; then
    fails=0
  else
    fails=$((fails + 1))
    # -eq, not -ge: fire once per outage rather than every second it stays down.
    if [ "$fails" -eq 90 ]; then
      errs=$(docker compose --profile dev logs --tail 40 kontinuum-dev 2>/dev/null \
        | grep -iE "\.go:[0-9]+:[0-9]+:|undefined:|syntax error|build failed" | tail -3)
      if [ -n "$errs" ]; then
        echo "port 8080 down 90s — build is failing, not restarting:"
        echo "$errs"
      else
        echo "port 8080 down 90s with no build error — restarting kontinuum-dev"
        docker compose --profile dev restart kontinuum-dev >/dev/null 2>&1 || echo "restart failed"
        until curl -sf http://localhost:8080/healthz >/dev/null 2>&1; do sleep 2; done
        echo "kontinuum-dev restarted; /healthz answering again"
        fails=0
      fi
    fi
  fi
  sleep 1
done
```

Only `kontinuum-dev` is restarted — postgres and proxy keep running.

Tell the user afterward that it dropped off port 8080 and was restarted, or
— for a build failure — what the compiler said, so they can fix it rather
than watch it retry.

Use the Monitor tool for this watch loop (persistent, one notification per
event) rather than blocking the conversation on it.

## Stopping

```bash
make dev-down       # stop, keep volumes (postgres data, caches)
make dev-clean       # stop and wipe volumes
```
