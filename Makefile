# Personal Reporting Pipelines
# Makefile for development workflows and operations

# Python environment
PIPENV = uv run
PYTEST = $(PIPENV) pytest \
	--log-cli-level=INFO \
	--cov-append \
	-v -s
DBTARGS = --project-dir dbt --profiles-dir dbt
target ?= mock
select ?= "*"

# dbt exclude logic for dev environment
# mock seeds are used in place of sources
DBT_EXCLUDE :=
ifeq ($(target),mock)
	DBT_EXCLUDE := --exclude "source:*"
endif

# Optional full-refresh support for dbt commands
full ?= false
DBT_FULL_REFRESH :=
ifeq ($(full),true)
	DBT_FULL_REFRESH := --full-refresh
endif

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
install: ## Install Python dependencies using uv
	@pip install --upgrade uv
	@uv sync

.PHONY: inject
inject:
	@op inject -f -i .dlt/secrets.toml.tpl -o .dlt/secrets.toml

## Testing
.PHONY: test-e2e
test-e2e: ## Run tests with coverage
	# coverage source configured in pyproject.toml [tool.coverage.run]
	$(PIPENV) pytest tests/dlt_e2e \
		--log-cli-level=INFO \
		--cov \
		--cov-append \
		--cov-report=xml \
		--junitxml=test-results-e2e.xml \
		-v -s

.PHONY: test-local
test-local: ## Run offline local tests only
	# coverage source configured in pyproject.toml [tool.coverage.run]
	$(PIPENV) pytest tests/dlt_unit \
		--log-cli-level=INFO \
		--cov \
		--cov-append \
		--cov-report=xml \
		--junitxml=test-results-local.xml \
		-v -s

.PHONY: test-all
test-all: test-local test-e2e ## Run all tests with coverage

.PHONY: lint
lint: ## Run prek checks on all files
	$(PIPENV) prek run -c prek.yml --all-files

.PHONY: test-coverage
test-coverage: ## Generate coverage reports only
	$(PIPENV) coverage report --show-missing
	$(PIPENV) coverage html

.PHONY: run-pipeline
run-pipeline: ## Run a pipeline via the new CLI. Usage: make run-pipeline PIPELINE=notion ARGS="--select name --full"
	@if [ -z "$(PIPELINE)" ]; then echo "Please set PIPELINE=<name>"; exit 2; fi
	PYTHONUNBUFFERED=1 $(PIPENV) python -m pipelines.run_pipeline $(PIPELINE) $(ARGS)

.PHONY: refresh-fitbit
refresh-fitbit: ## Run Fitbit dlt pipeline refresh
	$(MAKE) run-pipeline PIPELINE=fitbit ARGS="$(ARGS)"

.PHONY: refresh-notion
refresh-notion: ## Run Notion dlt pipeline refresh
	$(MAKE) run-pipeline PIPELINE=notion ARGS="$(ARGS)"

.PHONY: refresh-google-health
refresh-google-health: ## Run Google Health dlt pipeline refresh
	$(MAKE) run-pipeline PIPELINE=google_health ARGS="$(ARGS)"

.PHONY: refresh-hubspot
refresh-hubspot: ## Run HubSpot dlt pipeline refresh
	$(MAKE) run-pipeline PIPELINE=hubspot ARGS="$(ARGS)"

.PHONY: refresh-all
refresh-all: refresh-notion refresh-hubspot refresh-google-health ## Run all dlt pipeline refreshes

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
	@echo "Installing dbt dependencies..."
	$(PIPENV) dbt deps $(DBTARGS)

.PHONY: dbt-seed
dbt-seed:
	@echo "Seeding dbt project with $(target) target..."
	$(PIPENV) dbt seed $(DBTARGS) --target $(target) --select $(select) $(DBT_EXCLUDE) $(DBT_FULL_REFRESH)

.PHONY: dbt-run
dbt-run:
	@echo "Running dbt project with $(target) target..."
	$(PIPENV) dbt run $(DBTARGS) --target $(target) --select $(select) $(DBT_EXCLUDE) $(DBT_FULL_REFRESH)

.PHONY: dbt-test
dbt-test:
	@echo "Testing dbt project with $(target) target..."
	$(PIPENV) dbt test $(DBTARGS) --target $(target) --select $(select) $(DBT_EXCLUDE)

.PHONY: dbt-build
dbt-build:
	@echo "Building dbt project with $(target) target..."
	$(PIPENV) dbt build $(DBTARGS) --target $(target) --select $(select) $(DBT_EXCLUDE) $(DBT_FULL_REFRESH)

.PHONY: dbt-docs
dbt-docs:
	@echo "Generating dbt documentation..."
	$(PIPENV) dbt docs generate $(DBTARGS) --static --target $(target)

.PHONY: dbt-bouncer
dbt-bouncer: ## Run dbt-bouncer checks
	$(PIPENV) dbt-bouncer --config-file dbt/dbt-bouncer.yml

.PHONY: dbt-fix-lint
dbt-fix-lint: ## Auto-fix and lint SQL files
	@echo "Auto-fixing SQL files..."
	( cd dbt && $(PIPENV) sqlfluff fix )
	@echo "Linting SQL files..."
	( cd dbt && $(PIPENV) sqlfluff lint )

## Generate dbt and Sphinx documentation
.PHONY: docs
docs: dbt-deps dbt-docs
	@echo "Consolidating documentation..."
	@cp dbt/target/static_index.html docs/source/dbt.html
	@echo "Building Sphinx documentation..."
	$(PIPENV) sphinx-build -b html docs/source docs/_build/html
	@echo "Copying dbt docs to Sphinx output..."
	@mkdir -p docs/_build/html/dbt
	@cp dbt/target/static_index.html docs/_build/html/dbt.html
	@echo "Documentation available at docs/_build/html/index.html"