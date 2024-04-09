SHELL := /bin/bash # Use bash syntax

install_dev:
	pip install -r requirements/dev.txt

launch_localstack:
	mkdir -p /tmp/localstack; cd docker/localstack; docker-compose up --detach

destroy_localstack:
	cd docker/localstack; docker-compose down

terraform_dev:
	cd terraform/environments/dev; terraform init; terraform plan; terraform apply

terraform_test:
	cd terraform/environments/test; terraform init; terraform plan; terraform apply

format_code:
	terraform fmt -recursive terraform/
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

tflint:
	tflint -f compact --recursive

check_code_style: tflint mypy black flake8 pylint

run_tests:
	python -m pytest --cov=app tests