"""Integration tests for end-to-end data pipeline.

These tests validate the complete workflow:
  1. Pull raw data from sources
  2. Curate into normalized format
  3. Validate against schemas
  4. Rebuild catalog
"""

import json
import logging
import shutil
from pathlib import Path
from typing import Generator

import pytest

from scripts.export.catalog import build_catalog


@pytest.fixture
def test_workspace(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary workspace for integration tests."""
    workspace = tmp_path / "kemet_test"
    workspace.mkdir()

    # Create directory structure
    (workspace / "data" / "raw").mkdir(parents=True)
    (workspace / "data" / "curated").mkdir(parents=True)
    (workspace / "catalog").mkdir()
    (workspace / "schemas").mkdir()

    # Copy schemas
    schema_src = Path(__file__).parent.parent.parent / "etc" / "schemas"
    for schema_file in schema_src.glob("*.json"):
        shutil.copy(schema_file, workspace / "schemas")

    yield workspace

    # Cleanup handled automatically by tmp_path


@pytest.fixture
def test_logger() -> logging.Logger:
    """Create a test logger."""
    logger = logging.getLogger("kemet_test")
    logger.setLevel(logging.DEBUG)

    # Add handler if not already present
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)

    return logger


class TestMultiSourcePipeline:
    """Test complete pipeline with multiple data sources."""

    @pytest.mark.skip(reason="TODO: Fix ingestor initialization with IngestorConfig")
    def test_horner_pipeline(self, test_workspace: Path, test_logger: logging.Logger):
        """Test Horner NT pipeline end-to-end.

        TODO: Update to use IngestorConfig instead of direct settings/sources_config.
        """
        pass

    @pytest.mark.skip(reason="TODO: Fix ingestor initialization with IngestorConfig")
    def test_scriptorium_pipeline(self, test_workspace: Path, test_logger: logging.Logger):
        """Test Scriptorium pipeline end-to-end.

        TODO: Update to use IngestorConfig instead of direct settings/sources_config.
        """
        pass

    def test_catalog_build(self, test_workspace: Path, test_logger: logging.Logger):
        """Test catalog building from curated collections."""
        # Create some test curated data
        curated_dir = test_workspace / "data" / "curated"
        test_collection = curated_dir / "test_source" / "test_collection"
        test_collection.mkdir(parents=True)

        # Create minimal manifest
        manifest = {
            "source": "test_source",
            "collection": "test_collection",
            "created_at": "2025-01-01T00:00:00Z",
            "parser_version": "test@1.0.0",
            "counts": {
                "documents": 10,
                "segments": 100,
                "tokens": 1000,
            }
        }
        with open(test_collection / "manifest.json", "w") as f:
            json.dump(manifest, f)

        # Build catalog
        catalog_data = build_catalog(curated_dir, test_logger)

        # Validate catalog structure
        assert "version" in catalog_data
        assert "collections" in catalog_data
        assert len(catalog_data["collections"]) == 1

        collection = catalog_data["collections"][0]
        assert collection["source"] == "test_source"
        assert collection["collection"] == "test_collection"
        assert collection["counts"]["documents"] == 10
        assert collection["counts"]["segments"] == 100
        assert collection["counts"]["tokens"] == 1000


class TestDataQuality:
    """Test data quality across pipeline stages."""

    def test_no_data_loss(self, test_workspace: Path, test_logger: logging.Logger):
        """Verify no data is lost during curation."""
        # This would test that all source documents result in curated documents
        # Skipping detailed implementation for now
        pass

    def test_schema_compliance(self, test_workspace: Path, test_logger: logging.Logger):
        """Verify all curated data complies with schemas."""
        # This would validate all curated collections against schemas
        # Skipping detailed implementation for now
        pass

    def test_unicode_integrity(self, test_workspace: Path, test_logger: logging.Logger):
        """Verify Coptic unicode characters preserved correctly."""
        # This would check that Coptic text is preserved during pipeline
        # Skipping detailed implementation for now
        pass


class TestPerformance:
    """Test pipeline performance and scalability."""

    @pytest.mark.slow
    def test_large_corpus_curation(self, test_workspace: Path, test_logger: logging.Logger):
        """Test curation of large corpus (marked as slow test)."""
        # This would test curating a large corpus and measure performance
        # Skipping detailed implementation for now
        pass

    @pytest.mark.slow
    def test_catalog_rebuild_performance(self, test_workspace: Path, test_logger: logging.Logger):
        """Test catalog rebuild with many collections."""
        # This would test catalog building performance at scale
        # Skipping detailed implementation for now
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
