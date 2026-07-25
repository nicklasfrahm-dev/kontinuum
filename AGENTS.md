# AGENTS.md

## Rules

- Every package must always have tests.
- Each controller must live in its own domain package under `pkg/domain/` (e.g. `pkg/domain/registry`).
- `//nolint` comments are strictly prohibited. Fix the code, not the linter.
- Linters may never be disabled via the config. Fix the code, not the linter.
- Comments must be on their own line above the code, never on the same line. This applies to all files, including markdown code blocks.
- If the Makefile is modified, the README.md make targets section must be updated with the output of `make help`.

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

- Tests must use the `_test` package (external test package, e.g. `package config_test`).
- If a test needs access to an unexported function, make the function public. Do not use `export_test.go` to expose internals.
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
