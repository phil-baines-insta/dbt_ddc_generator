.PHONY: install lint format clean build

install:
	pip install -e .
	pip install -r requirements-dev.txt
	pip install -r requirements.txt

lint:
	ruff check .
	mypy .
	black --check .

format:
	ruff check --fix .
	black .

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python setup.py sdist bdist_wheel

test_results:
	mkdir -p test_results

test: test_results
	pytest --verbose tests/ --junit-xml test_results/test-results.xml
