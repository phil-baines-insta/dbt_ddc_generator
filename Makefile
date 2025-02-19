VENV_BIN=venv/bin

init-venv:
	python -m venv venv

format:
	$(VENV_BIN)/ruff format .
	$(VENV_BIN)/ruff check --fix .

lint:
	$(VENV_BIN)/ruff format --check .
	$(VENV_BIN)/ruff check --diff .
	$(VENV_BIN)/mypy --ignore-missing-imports .

test:
	$(VENV_BIN)/pytest --verbose tests/ --junit-xml test_results/test-results.xml