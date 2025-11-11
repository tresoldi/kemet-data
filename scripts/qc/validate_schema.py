"""Schema validation for curated data."""

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from scripts.utils.io import read_json, read_jsonl
from scripts.utils.schema import (
    validate_document,
    validate_manifest,
    validate_segment,
    validate_token,
)


@dataclass
class ValidationResult:
    """Result of validation."""

    valid: bool
    errors: list[str]
    warnings: list[str]


def validate_documents_file(
    documents_path: Path,
    schema_dir: Path,
    logger: logging.Logger,
) -> ValidationResult:
    """
    Validate documents.jsonl file.

    Args:
        documents_path: Path to documents.jsonl
        schema_dir: Path to schemas directory
        logger: Logger instance

    Returns:
        Validation result
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not documents_path.exists():
        errors.append(f"Documents file not found: {documents_path}")
        return ValidationResult(valid=False, errors=errors, warnings=warnings)

    # Validate each document
    for idx, doc in enumerate(read_jsonl(documents_path)):
        doc_errors = validate_document(doc, schema_dir)
        if doc_errors:
            errors.extend([f"Document {idx}: {err}" for err in doc_errors])

    valid = len(errors) == 0
    return ValidationResult(valid=valid, errors=errors, warnings=warnings)


def validate_segments_file(
    segments_path: Path,
    schema_dir: Path,
    logger: logging.Logger,
) -> ValidationResult:
    """
    Validate segments.parquet file.

    Args:
        segments_path: Path to segments.parquet
        schema_dir: Path to schemas directory
        logger: Logger instance

    Returns:
        Validation result
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not segments_path.exists():
        warnings.append(f"Segments file not found: {segments_path}")
        return ValidationResult(valid=True, errors=errors, warnings=warnings)

    # Read parquet
    df = pd.read_parquet(segments_path)

    # Validate sample of segments (first 100)
    sample_size = min(100, len(df))
    for idx in range(sample_size):
        seg = df.iloc[idx].to_dict()
        seg_errors = validate_segment(seg, schema_dir)
        if seg_errors:
            errors.extend([f"Segment {idx}: {err}" for err in seg_errors])

    if len(errors) > 0:
        logger.warning(f"Found {len(errors)} validation errors in sample")

    valid = len(errors) == 0
    return ValidationResult(valid=valid, errors=errors, warnings=warnings)


def validate_tokens_file(
    tokens_path: Path,
    schema_dir: Path,
    logger: logging.Logger,
) -> ValidationResult:
    """
    Validate tokens.parquet file.

    Args:
        tokens_path: Path to tokens.parquet
        schema_dir: Path to schemas directory
        logger: Logger instance

    Returns:
        Validation result
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not tokens_path.exists():
        warnings.append(f"Tokens file not found: {tokens_path}")
        return ValidationResult(valid=True, errors=errors, warnings=warnings)

    # Read parquet
    df = pd.read_parquet(tokens_path)

    # Validate sample of tokens (first 100)
    sample_size = min(100, len(df))
    for idx in range(sample_size):
        tok = df.iloc[idx].to_dict()
        tok_errors = validate_token(tok, schema_dir)
        if tok_errors:
            errors.extend([f"Token {idx}: {err}" for err in tok_errors])

    if len(errors) > 0:
        logger.warning(f"Found {len(errors)} validation errors in sample")

    valid = len(errors) == 0
    return ValidationResult(valid=valid, errors=errors, warnings=warnings)


def validate_manifest_file(
    manifest_path: Path,
    schema_dir: Path,
    logger: logging.Logger,
) -> ValidationResult:
    """
    Validate manifest.json file.

    Args:
        manifest_path: Path to manifest.json
        schema_dir: Path to schemas directory
        logger: Logger instance

    Returns:
        Validation result
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not manifest_path.exists():
        errors.append(f"Manifest file not found: {manifest_path}")
        return ValidationResult(valid=False, errors=errors, warnings=warnings)

    # Load and validate manifest
    manifest = read_json(manifest_path)
    manifest_errors = validate_manifest(manifest, schema_dir)
    errors.extend(manifest_errors)

    valid = len(errors) == 0
    return ValidationResult(valid=valid, errors=errors, warnings=warnings)


def validate_collection(
    collection_path: Path,
    schema_dir: Path,
    logger: logging.Logger,
) -> ValidationResult:
    """
    Validate entire collection directory.

    Args:
        collection_path: Path to collection directory
        schema_dir: Path to schemas directory
        logger: Logger instance

    Returns:
        Combined validation result
    """
    all_errors = []
    all_warnings = []

    # Validate each file type
    results = [
        validate_documents_file(collection_path / "documents.jsonl", schema_dir, logger),
        validate_segments_file(collection_path / "segments.parquet", schema_dir, logger),
        validate_tokens_file(collection_path / "tokens.parquet", schema_dir, logger),
        validate_manifest_file(collection_path / "manifest.json", schema_dir, logger),
    ]

    for result in results:
        all_errors.extend(result.errors)
        all_warnings.extend(result.warnings)

    valid = len(all_errors) == 0
    return ValidationResult(valid=valid, errors=all_errors, warnings=all_warnings)
