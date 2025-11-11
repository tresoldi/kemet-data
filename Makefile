# Makefile for KEMET Data
# Ancient Egyptian and Coptic Annotated Corpus and Lexicon
# Author: Tiago Tresoldi, Marwan Kilani

.PHONY: help install install-dev build curate clean clean-curated clean-all stats validate test check zenodo backup

# Paths
PYTHON := python3
CLI := $(PYTHON) -m scripts.kemet_cli
DATA_DERIVED := data/derived
DATA_CURATED := data/curated
CORPUS_DB := $(DATA_DERIVED)/corpus.duckdb
LEXICON_DB := $(DATA_DERIVED)/lexicon.duckdb
DIST_DIR := dist
ZENODO_DIR := $(DIST_DIR)/zenodo

# ============================================================================
# Help
# ============================================================================

help: ## Show this help message
	@echo "KEMET Data - Makefile"
	@echo "====================="
	@echo ""
	@echo "Available targets:"
	@echo ""
	@echo "Building:"
	@echo "  make build         - Build databases from scratch (download, curate, derive)"
	@echo "  make curate        - Curate all enabled sources (for manual testing)"
	@echo ""
	@echo "Cleaning:"
	@echo "  make clean         - Remove derived databases only"
	@echo "  make clean-curated - Remove curated data only"
	@echo "  make clean-all     - Remove all derived data (databases + curated)"
	@echo ""
	@echo "Quality & Testing:"
	@echo "  make test          - Run test suite"
	@echo "  make validate      - Validate database integrity"
	@echo "  make stats         - Display database statistics"
	@echo "  make check         - Run linting and type checks"
	@echo ""
	@echo "Distribution:"
	@echo "  make zenodo        - Prepare Zenodo distribution package"
	@echo ""
	@echo "Development:"
	@echo "  make install       - Install Python dependencies"
	@echo "  make install-dev   - Install development dependencies"
	@echo "  make backup        - Create backup of databases"
	@echo ""

# ============================================================================
# Installation
# ============================================================================

install: ## Install dependencies
	pip install -e .

install-dev: ## Install dev dependencies
	pip install -e ".[dev]"

# ============================================================================
# Building
# ============================================================================

build: clean-all ## Build databases from scratch (download, curate, derive)
	@echo "Building databases from scratch (with auto-curation)..."
	$(CLI) database build --drop --auto-curate
	@echo ""
	@echo "Build complete! Databases:"
	@echo "  Corpus:  $(CORPUS_DB)"
	@echo "  Lexicon: $(LEXICON_DB)"

curate: ## Curate all enabled sources (for manual testing)
	@echo "Curating all enabled sources..."
	@$(PYTHON) -c "\
	import yaml; \
	from pathlib import Path; \
	config = yaml.safe_load(Path('etc/sources.yaml').read_text()); \
	sources = [name for name, cfg in config.get('sources', {}).items() if cfg.get('enabled', False)]; \
	print(' '.join(sources))" | xargs -n1 -I {} sh -c 'echo "Curating {}..." && $(CLI) data curate --source {} || true'
	@echo "Curation complete!"

# ============================================================================
# Cleaning
# ============================================================================

clean: ## Remove derived databases only
	@echo "Removing derived databases..."
	rm -f $(CORPUS_DB) $(LEXICON_DB)
	@echo "Databases removed."

clean-curated: ## Remove curated data only
	@echo "Removing curated data..."
	rm -rf $(DATA_CURATED)/*
	@echo "Curated data removed."

clean-all: clean clean-curated ## Remove all derived data (databases + curated)
	@echo "All derived data removed."

clean-python: ## Clean Python cache files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name ".coverage" -delete
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf dist
	rm -rf build
	rm -rf *.egg-info

# ============================================================================
# Quality & Testing
# ============================================================================

test: ## Run test suite
	@echo "Running test suite..."
	pytest tests/ -v

test-cov: ## Run tests with coverage
	pytest tests/ -v --cov=scripts --cov-report=html --cov-report=term

validate: ## Validate database integrity
	@echo "Validating database integrity..."
	$(CLI) database validate

stats: ## Display database statistics
	@echo "Computing database statistics..."
	@echo ""
	$(CLI) database stats || echo "Run 'make build' first"

check: lint mypy test ## Run all checks (lint, type check, test)
	@echo "All checks passed!"

lint: ## Lint code with ruff
	@echo "Linting with ruff..."
	ruff check scripts/ kemet/ tests/ || true

mypy: ## Type check with mypy
	@echo "Type checking with mypy..."
	mypy scripts/ kemet/ || true

format: ## Format code with black
	@echo "Formatting with black..."
	black scripts/ kemet/ tests/

# ============================================================================
# Distribution
# ============================================================================

zenodo: validate ## Prepare Zenodo distribution package
	@echo "Preparing Zenodo distribution package..."
	@mkdir -p $(ZENODO_DIR)

	@echo "Copying databases..."
	@cp $(CORPUS_DB) $(ZENODO_DIR)/corpus.duckdb
	@cp $(LEXICON_DB) $(ZENODO_DIR)/lexicon.duckdb

	@echo "Copying documentation..."
	@cp README.md $(ZENODO_DIR)/
	@cp LICENSE $(ZENODO_DIR)/
	@cp DATABASE.md $(ZENODO_DIR)/
	@cp SPECS.md $(ZENODO_DIR)/

	@echo "Generating metadata..."
	@echo "KEMET Data v0.1.0" > $(ZENODO_DIR)/VERSION
	@echo "Built on: $$(date -u +%Y-%m-%d)" >> $(ZENODO_DIR)/VERSION
	@echo "Python: $$(python3 --version)" >> $(ZENODO_DIR)/VERSION
	@echo "DuckDB: $$(python3 -c 'import duckdb; print(duckdb.__version__)')" >> $(ZENODO_DIR)/VERSION

	@echo "Computing checksums..."
	@cd $(ZENODO_DIR) && sha256sum corpus.duckdb lexicon.duckdb > SHA256SUMS.txt

	@echo "Creating archive..."
	@cd $(DIST_DIR) && tar -czf kemet-data-v0.1.0.tar.gz zenodo/

	@echo ""
	@echo "Zenodo package created: $(DIST_DIR)/kemet-data-v0.1.0.tar.gz"
	@ls -lh $(DIST_DIR)/kemet-data-v0.1.0.tar.gz

# ============================================================================
# Utilities
# ============================================================================

backup: ## Create backup of databases
	@echo "Creating backup..."
	@BACKUP_DIR=backup_$$(date +%Y%m%d_%H%M%S); \
	mkdir -p $$BACKUP_DIR; \
	cp $(CORPUS_DB) $$BACKUP_DIR/ 2>/dev/null || true; \
	cp $(LEXICON_DB) $$BACKUP_DIR/ 2>/dev/null || true; \
	echo "Backup created in $$BACKUP_DIR/"

inspect-corpus: ## Open corpus database in DuckDB CLI
	@echo "Opening corpus database in DuckDB CLI..."
	duckdb $(CORPUS_DB)

inspect-lexicon: ## Open lexicon database in DuckDB CLI
	@echo "Opening lexicon database in DuckDB CLI..."
	duckdb $(LEXICON_DB)
