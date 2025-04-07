all: test

test:
	@echo "Running tests..."
	uv run pytest -v -s --cov ./unfazed_redis --cov-report term-missing

format:
	@echo "Formatting code..."
	uv run ruff format tests/ unfazed_redis/
	uv run ruff check tests/ unfazed_redis/  --fix
	uv run mypy --check-untyped-defs --explicit-package-bases tests/ unfazed_redis/

publish:
	@echo "Publishing package..."
	uv build
	uv publish
