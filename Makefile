.PHONY: check lint format type-check lock tests

check: lint format type-check lock

lint:
	uvx ruff check .

format:
	uvx ruff format --check .

tests:
	uv run pytest

type-check:
	uv run pyright .

lock:
	uv lock --locked
