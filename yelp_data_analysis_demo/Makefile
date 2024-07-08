SHELL := /bin/bash # Use bash syntax

install_dev:
	pip install -r requirements.txt

format_code:
	isort app tests
	black --line-length 120 app tests

mypy:
	mypy --version
	mypy --show-error-codes app tests

black:
	black --version
	black --diff --line-length 120 --check app tests

pylint:
	pylint --version
	set -euo pipefail && pylint app

flake8:
	flake8 --version
	flake8 app tests

check_code_style: mypy black flake8 pylint

run_tests:
	python -m pytest --cov=app tests