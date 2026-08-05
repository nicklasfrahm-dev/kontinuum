# Contribution guidelines

See [Local setup](local-setup.md) for building kontinuum and running the hot-reload dev environment before making changes.

## Conventions

The authoritative source for repo conventions is [AGENTS.md](https://github.com/nicklasfrahm-dev/kontinuum/blob/main/AGENTS.md) at the repo root — read it before contributing. Highlights:

- Every package must always have tests, written in an external `_test` package (e.g. `package config_test`), using testify's `assert`/`require`, and run with `-race`.
- Each controller lives in its own domain package under `pkg/domain/` (e.g. `pkg/domain/registry`).
- **New features must be documented** — see AGENTS.md's Documentation section.
- `//nolint` comments and disabling linters via config are both prohibited — fix the code, not the linter.
- All errors must be wrapped before being returned; never return a bare error.
- Log messages start with an uppercase letter and use structured fields, not string interpolation.
- Commits follow [Conventional Commits](https://www.conventionalcommits.org/) with a required scope (e.g. `feat(cli):`, `fix(config):`), and type must be `feat` or `fix`.

## Validating changes

Before considering a change done, run:

```sh
make lint
make test
```

Both must pass with zero issues. `make test-e2e` runs the gated end-to-end tests (requires Docker; boots real Talos containers) — these aren't part of the default `test` target because they're slow.

## Regenerating code

`make generate` regenerates deepcopy methods and CRD manifests for `api/...` via `controller-gen`, plus vendors the UI's third-party web assets (Tailwind CSS, htmx, PrismJS, JetBrains Mono). It's a dependency of `build`, `test`, `vet`, and `lint`, so you rarely need to run it directly — but if you change a type in `api/v1alpha1` or `api/v1alpha2`, run it and commit the result; CI's `validate-code-generation` job fails the build otherwise.

## Documentation

This site is built with [MkDocs](https://www.mkdocs.org/) and the [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) theme, and deployed to GitHub Pages on every push to `main` that touches `docs/` or `mkdocs.yml` (see `.github/workflows/docs.yml`).

Preview it locally:

```sh
make docs
```

This installs the pinned dependencies from `docs/requirements.txt` into a local virtualenv and runs `mkdocs serve`, which live-reloads at [http://127.0.0.1:8000](http://127.0.0.1:8000).

## Make targets

See the [README](https://github.com/nicklasfrahm-dev/kontinuum#make-targets) for the full `make help` output.
