all: test

test:
	@echo "Running tests..."
	uv run pytest -v -s --cov ./unfazed_redis --cov-report term-missing

format:
	@echo "Formatting code..."
	ruff format tests/ unfazed_redis/
	ruff check tests/ unfazed_redis/  --fix
	mypy --check-untyped-defs --explicit-package-bases tests/ unfazed_redis/

publish:
	@echo "Publishing package..."
	uv build
	uv publish
