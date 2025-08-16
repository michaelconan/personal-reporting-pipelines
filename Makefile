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
.PHONY: test
test: ## Run tests with coverage
	$(PIPENV) pytest tests \
		--cov=pipelines \
		--cov-report=html \
		--cov-report=xml \
		--cov-report=term-missing \
		--log-cli-level=INFO

.PHONY: test-coverage
test-coverage: ## Generate coverage reports only
	$(PIPENV) coverage html
	$(PIPENV) coverage xml

.PHONY: dlt-clean
dlt-clean:
	@rm -rf ~/.dlt
	@rm -f *.duckdb