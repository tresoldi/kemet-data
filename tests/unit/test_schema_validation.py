"""Tests for schema validation."""

from scripts.utils.schema import validate_document, validate_segment


def test_validate_document_valid(sample_document, schema_dir):
    """Test validation of valid document."""
    doc_dict = sample_document.to_dict()
    errors = validate_document(doc_dict, schema_dir)

    assert len(errors) == 0, f"Unexpected errors: {errors}"


def test_validate_document_missing_field(schema_dir):
    """Test validation of document with missing required field."""
    doc_dict = {
        "document_id": "test:work:sample",
        "source": "test",
        # Missing required fields
    }

    errors = validate_document(doc_dict, schema_dir)
    assert len(errors) > 0


def test_validate_segment_valid(sample_segment, schema_dir):
    """Test validation of valid segment."""
    seg_dict = sample_segment.to_dict()
    errors = validate_segment(seg_dict, schema_dir)

    assert len(errors) == 0, f"Unexpected errors: {errors}"


def test_validate_segment_invalid_hash(schema_dir):
    """Test validation of segment with invalid hash format."""
    seg_dict = {
        "document_id": "test:work:sample",
        "segment_id": "s000001",
        "order": 0,
        "text_canonical": "test",
        "content_hash": "invalid_hash",  # Wrong format
        "created_at": "2025-01-01T00:00:00Z",
    }

    errors = validate_segment(seg_dict, schema_dir)
    assert len(errors) > 0
