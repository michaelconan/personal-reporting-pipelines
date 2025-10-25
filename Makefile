# Personal Reporting Pipelines
# Makefile for development workflows and operations

# Python environment
PIPENV = pipenv run
PYTEST = $(PIPENV) pytest \
	--cov=pipelines \
	--cov-append \
	-v -s
DBTARGS = --project-dir dbt --profiles-dir dbt

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
		--cov=pipelines \
		--cov-append \
		--cov-branch \
		--cov-report=xml \
		--junitxml=test-results-e2e.xml \
		-v -s \
		|| true

.PHONY: test-local
test-local: ## Run offline local tests only
	$(PIPENV) pytest tests/dlt_unit \
		--cov=pipelines \
		--cov-append \
		--cov-branch \
		--cov-report=xml \
		--junitxml=test-results-local.xml \
		-v -s \
		|| true

.PHONY: test-all
test-all: test-local test-e2e ## Run all tests with coverage

.PHONY: test-coverage
test-coverage: ## Generate coverage reports only
	$(PIPENV) coverage report --show-missing
	$(PIPENV) coverage html

.PHONY: clean
clean: ## Remove Python cache files and temporary artifacts
	@echo "Cleaning up temporary files..."
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type d -name "*.egg-info" -exec rm -rf {} +
	@find . -type f -name "*.pyo" -delete
	@find . -type f -name ".DS_Store" -delete
	@rm -rf .pytest_cache
	@rm -rf .coverage.*
	@rm -rf htmlcov
	@rm -rf build
	@rm -rf dist
	@rm -rf docs/_build
	@rm -f test-results-*.xml
	@echo "Cleanup complete!"

.PHONY: dlt-clean
dlt-clean: ## Clean DLT-specific files and data
	@rm -rf ~/.dlt
	@rm -f *.duckdb

.PHONY: dbt-deps
dbt-deps:
	$(PIPENV) dbt deps $(DBTARGS)

.PHONY: dbt-build-dev
dbt-build-dev:
	@DBT_TARGET=dev $(PIPENV) dbt build $(DBTARGS)

.PHONY: dbt-build-test
dbt-build-test:
	@DBT_TARGET=test RAW_SCHEMA=test_raw $(PIPENV) dbt build $(DBTARGS)

.PHONY: dbt-build-prod
dbt-build-prod:
	@DBT_TARGET=prod $(PIPENV) dbt build $(DBTARGS)

.PHONY: dbt-doc-coverage
dbt-doc-coverage:
	$(PIPENV) dbt-coverage compute doc --run-artifacts-dir dbt/target --output-format markdown

.PHONY: dbt-test-coverage
dbt-test-coverage:
	$(PIPENV) dbt-coverage compute test --run-artifacts-dir dbt/target --output-format markdown

.PHONY: docs
docs: ## Generate dbt and Sphinx documentation
	@echo "Installing dbt dependencies..."
	$(PIPENV) dbt deps $(DBTARGS)
	@echo "Generating dbt documentation..."
	$(PIPENV) dbt docs generate $(DBTARGS) --target-path ../target --static
	@cp target/static_index.html docs/source/dbt.html
	@echo "Building Sphinx documentation..."
	$(PIPENV) sphinx-build -b html docs/source docs/_build/html
	@echo "Copying dbt docs to Sphinx output..."
	@mkdir -p docs/_build/html/dbt
	@cp target/static_index.html docs/_build/html/dbt.html
	@echo "Documentation available at docs/_build/html/index.html"
