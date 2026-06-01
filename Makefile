all: deps lint test

uv:
	@which uv >/dev/null 2>&1 || { \
		echo "❌ uv is not installed"; \
		exit 1;\
	}

deps: uv
	@uv sync --all-extras

format:
	@uv run ruff format psycopg_listen tests
	@uv run ruff check --fix psycopg_listen tests

pyright:
	@uv run pyright

lint: format pyright

test:
	@uv run pytest -vv --rootdir tests .
