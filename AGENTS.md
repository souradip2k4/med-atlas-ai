# Python Environment with uv

This project uses `uv` for dependency management and a virtual environment in `.venv/`.

## Environment Commands

- **Always** run Python scripts using: `uv run python <file>`
- **Install** new packages using: `uv add <package>`
- **Sync** the environment: `uv sync`
- **Run tests**: `uv run pytest`

## Guidelines

- Never use `pip` or `python` directly.
- All commands must be prefixed with `uv run` to ensure the local `.venv` is used.
