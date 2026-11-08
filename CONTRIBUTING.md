# Contributing to dox_postgres_native

Thank you for helping Dox. The full guide is here:
https://dartondox.dev/latest/community/contribute/

## Bugs and feature requests

Use GitHub issues: https://github.com/dartondox/dox-postgres-native/issues

For security problems, see [SECURITY.md](SECURITY.md). Do not open a public issue.

## Pull requests

Send changes as pull requests against `main`. Before you ask for review:

- CI is green.
- In `native/`: `cargo fmt`, `cargo clippy --release --all-targets -- -D warnings` and `cargo test` pass.
- `dart format` is clean.
- `dart analyze --fatal-infos` has no warnings.
- Add a changelog entry under `## Unreleased` in `CHANGELOG.md`.
- Do not change versions in the PR.

## Tests

Every new feature and every bug fix must add tests.

Run the Rust tests with `cargo test` in `native/`. Run the Dart tests with `dart test`. CI runs both on every push and pull request.
