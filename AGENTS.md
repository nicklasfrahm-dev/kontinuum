# AGENTS.md

## Rules

- Every package must always have tests.
- Each controller must live in its own domain package under `pkg/domain/` (e.g. `pkg/domain/registry`).
- `//nolint` comments are strictly prohibited. Fix the code, not the linter.
- Linters may never be disabled via the config. Fix the code, not the linter.
- Comments must be on their own line above the code, never on the same line. This applies to all files, including markdown code blocks.
- If the Makefile is modified, the README.md make targets section must be updated with the output of `make help`.
- All enums must be PascalCase, with abbreviations capitalized, following Kubernetes conventions (e.g. `ClusterIP`).

## UI

- Never use `alert()`, `confirm()`, or `prompt()` (including htmx's `hx-confirm`, which shows the browser's native `confirm()`). All dialogs must be native `<dialog>` modals styled to match the rest of the app (see `pkg/ui/templates/components/zone_leave_modal.html` for the pattern).

## Documentation

- All features must be documented in the `docs/` MkDocs site (see `mkdocs.yml`), not only in code comments.
- A new CRD, controller, CLI command, or config option must land in the same PR as the docs page (or section) describing it — extend an existing page under `docs/` (e.g. `docs/architecture.md`, `docs/quickstart.md`) or add a new one and wire it into `mkdocs.yml`'s `nav`.
- Preview docs changes locally with `make docs` before opening a PR.

## Commits

- Follow [Conventional Commits](https://www.conventionalcommits.org/).
- Scope is required (e.g. `feat(cli):`, `fix(config):`).
- Type MUST be `feat` or `fix`.
- Scope is preferred to be a package name (e.g. `cli`, `config`, `logging`).

## Logging

- Log messages must start with an uppercase letter.
- Log messages must be human-readable.
- Use structured fields instead of string interpolation (e.g. `logger.Info("Server started", "addr", addr)`, not `logger.Info(fmt.Sprintf("Server started on %s", addr))`).

## Errors

- All errors must be wrapped before being returned; never return a bare error.
- Wrapped error messages must use one of either format:
  - If another error exists: `fmt.Errorf("failed to <action>: %w", err)`
  - If no other error exists: `fmt.Errorf("%w: %s", ErrExample, exampleVar)`

## Tests

- Tests must use the `_test` package (external test package, e.g. `package config_test`). They MUST NEVER live in the same package as the code under test.
- If a test needs access to an unexported function, make the function exported instead (e.g. `applyCRD` becomes `ApplyCRD`). Do not use `export_test.go`, and do not add an internal (same-package) test file, to work around this.
- Tests must use `github.com/stretchr/testify`'s `assert`/`require` packages.
- Tests must run with `-race`.

## CI

- Job IDs must follow verb-noun naming (the noun is optional, e.g. `test` for a job that only runs tests).
- Every job must have a human-readable `name`.
- Prefer adding a job to an existing workflow over creating a new one, unless its trigger differs significantly from the existing workflows'.

## Validating changes

Before considering a task done, run:

```sh
make lint
make test
```

Both must pass with zero issues.
