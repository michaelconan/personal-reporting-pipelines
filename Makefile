# Personal Reporting Pipelines
# Makefile for development workflows and operations

# Python environment
PIPENV = pipenv run

# Default target
.DEFAULT_GOAL := help

## Help
.PHONY: help
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## Environment Setup
.PHONY: install
install: ## Install Python dependencies using pipenv
	pip install pipenv
	pipenv install --dev

## Testing
.PHONY: test-e2e
test-e2e: ## Run tests with coverage
	$(PIPENV) pytest tests/dlt_e2e \
	    -p no:pytest-responses \
		--cov=pipelines \
		--cov-config=.coveragerc \
		--cov-append \
		--cov-report= \
		--log-cli-level=INFO \
		-v
	@mv .coverage .coverage.e2e

.PHONY: test-local
test-local: ## Run offline local tests only
	$(PIPENV) pytest tests -p pytest-responses \
		-m local \
		--cov=pipelines \
		--cov-config=.coveragerc \
		--cov-append \
		--cov-report= \
		--log-cli-level=INFO \
		-v
	@mv .coverage .coverage.local

.PHONY: test-all
test-all: test-local test-e2e test-coverage

.PHONY: test-coverage
test-coverage: ## Generate coverage reports only
	$(PIPENV) coverage combine .coverage.local .coverage.e2e
	$(PIPENV) coverage report --show-missing
	$(PIPENV) coverage html

.PHONY: test-all
test-all:
	test-local test-e2e test-coverage

.PHONY: dlt-clean
dlt-clean:
	@rm -rf ~/.dlt
	@rm -f *.duckdb