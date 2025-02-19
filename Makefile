.PHONY: install lint format clean build test

install:
	poetry install

lint:
	poetry run ruff check .
	poetry run mypy .
	poetry run black --check .

format:
	poetry run ruff check --fix .
	poetry run black .

test_results:
	mkdir -p test_results

test: test_results
	poetry run pytest --verbose tests/ --junit-xml test_results/test-results.xml

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	poetry build
