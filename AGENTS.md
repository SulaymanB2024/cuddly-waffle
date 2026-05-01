# AGENTS.md

## Working rules
- Keep changes scoped to the current milestone in `docs/PLAN.md`.
- After each milestone, run relevant checks and update `docs/STATUS.md`.
- Do not claim completion without running a live smoke test command from `docs/USAGE.md`.
- Prefer deterministic tests using fixtures over live network tests.

## Commands
- Install: `python -m pip install -e .[dev]`
- Run tests: `pytest -q`
- Run audit: `python -m seo_audit audit --domain https://void-agency.com --output ./out`
- Optional lint/format: `ruff check .` and `ruff format .`

## Conventions
- Python 3.11+, typed dataclasses, explicit return types.
- Separate observed facts from derived heuristics/scores.
- Keep modules small and focused; avoid speculative abstractions.
