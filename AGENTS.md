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
- Every `hx-get`/`hx-post`/`hx-put`/`hx-delete`/`hx-patch` element that fires in direct response to a user action (a form submit, a button click) must carry `hx-disabled-elt="this"`, instead of a hand-rolled `htmx:beforeRequest`/`afterRequest` listener toggling `disabled`/`hidden` in JS. htmx's own request lifecycle re-enables the element once the request settles — including on a network error or dropped connection — which a manual listener reliably gets wrong.
- Pair `hx-disabled-elt` with a spinner keyed off the `htmx-request` class htmx adds to the request-issuing element itself while a request is in flight — do **not** add an `hx-indicator` attribute, which diverts that class onto whatever it resolves to instead of the button, and do not rely on htmx's separately-injected `.htmx-indicator` opacity stylesheet. Every such button — labeled or icon-only — carries a leading icon that swaps for the spinner in place during the request, rather than a spinner appearing from nothing beside a static icon or label: appending a second element instead of swapping changes the button's width (or breaks an icon-only button's square shape) the moment it enters or leaves a request. Wrap the default icon and the spinner in their own spans and toggle each via a Tailwind ancestor arbitrary variant: `[.htmx-request_&]:hidden` on the default icon, `hidden [.htmx-request_&]:inline-flex` on the spinner. See `pkg/ui/templates/components/instance_add_modal.html`'s submit button (labeled) or `pkg/ui/templates/registry_content.html`'s row delete button (icon-only) for the pattern. If a button doesn't already have a natural leading icon, give it one that fits the action rather than leaving it text-only, so this pattern always applies.
- Exception: an element polling on `hx-trigger="every ..."` must NOT get `hx-indicator`/`hx-disabled-elt` — disabling or flashing a spinner over a whole page section on every background refresh tick is worse UX than the polling itself.

## Documentation

- All features must be documented in the `docs/` MkDocs site (see `mkdocs.yml`), not only in code comments.
- A new CRD, controller, CLI command, or config option must land in the same PR as the docs page (or section) describing it — extend an existing page under `docs/` (e.g. `docs/architecture.md`, `docs/quickstart.md`) or add a new one and wire it into `mkdocs.yml`'s `nav`.
- Preview docs changes locally with `make docs` before opening a PR.

## Commits

- Follow [Conventional Commits](https://www.conventionalcommits.org/).
- Scope is required (e.g. `feat(cli):`, `fix(config):`).
- Type MUST be `feat` or `fix`.
- Scope is preferred to be a package name (e.g. `cli`, `config`, `logging`).

## Pull requests

- PR titles must follow the same [Conventional Commits](https://www.conventionalcommits.org/) rules as commit messages: type MUST be `feat` or `fix`, scope is required, and the description must be in imperative mood.

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
make verify
```

This runs, in order: `build`, `vet`, `lint`, `test`, `test-e2e`, `tidy`,
`docs-lint`. All must pass with zero issues; `make tidy` must produce no diff to
`go.mod`/`go.sum` (if it does, that diff is a real, missed dependency
change — include it, don't discard it).

`make test-e2e` is gated behind `KONTINUUM_TEST_E2E=1` (needs Docker;
boots real Talos containers) specifically so `make test`/`go test ./...`
skip it by default — silently, with no build or vet error, because
`t.Skip()` is a runtime check inside the test body. A change that's only
wrong for a real, namespaced/reconciled object (not the fake client
fixtures the rest of the suite uses) can pass every other check and still
break at this layer, so a skip in the default `make test` run means "not
yet verified," never "passing" — always run `make test-e2e` too, not just
when a change looks like it touches that path.

`make docs-lint` mirrors CI's own "Build docs" job (`mkdocs build
--strict` plus a broken-link check) — like `make test-e2e`, nothing else
here catches a stale internal doc link or a broken `mkdocs.yml` nav
entry, so it doesn't overlap with `make lint`/`make test`.
