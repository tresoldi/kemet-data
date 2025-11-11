"""Pytest fixtures for KEMET tests."""

import tempfile
from pathlib import Path

import pytest

from scripts.models import (
    Document,
    DocumentCounts,
    Provenance,
    Segment,
    Stage,
    Substage,
    create_timestamp,
)
from scripts.utils.hashing import hash_string


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_coptic_text():
    """Sample Coptic text."""
    return "ⲁⲩⲱ ⲡⲉϫⲁϥ ⲛⲁⲩ ϫⲉ ⲙⲁⲣⲟⲩϣⲧⲟⲣⲧⲣ"


@pytest.fixture
def sample_document():
    """Sample document for testing."""
    return Document(
        document_id="test:work:sample",
        source="test",
        collection="sample",
        stage=Stage.COPTIC,
        substage=Substage.SAHIDIC,
        language="cop",
        title="Test Document",
        authors=["Test Author"],
        genre=["test"],
        provenance=Provenance(
            source_item_id="test_001",
            retrieved_at=create_timestamp(),
            hash_raw="blake3:0000000000000000000000000000000000000000000000000000000000000000",
            parser_version="test@1.0.0",
        ),
        counts=DocumentCounts(segments=1, tokens=0),
    )


@pytest.fixture
def sample_segment(sample_coptic_text):
    """Sample segment for testing."""
    return Segment(
        document_id="test:work:sample",
        segment_id="s000001",
        order=0,
        text_canonical=sample_coptic_text,
        text_stripped=sample_coptic_text,
        content_hash=hash_string(sample_coptic_text),
        created_at=create_timestamp(),
    )


@pytest.fixture
def schema_dir():
    """Path to schemas directory."""
    return Path(__file__).parent.parent / "etc" / "schemas"
