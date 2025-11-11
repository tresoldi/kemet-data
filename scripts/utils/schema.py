"""JSON Schema validation utilities."""

import json
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft7Validator


def load_schema(schema_path: Path) -> dict[str, Any]:
    """
    Load JSON schema from file.

    Args:
        schema_path: Path to schema file

    Returns:
        Parsed schema dict
    """
    with schema_path.open("r") as f:
        return cast(dict[str, Any], json.load(f))


def validate_against_schema(
    data: dict[str, Any],
    schema: dict[str, Any],
) -> list[str]:
    """
    Validate data against JSON schema.

    Args:
        data: Data to validate
        schema: JSON schema

    Returns:
        List of validation error messages (empty if valid)
    """
    validator = Draft7Validator(schema)
    errors = []

    for error in sorted(validator.iter_errors(data), key=str):
        # Format error message with path
        path = ".".join(str(p) for p in error.path) if error.path else "root"
        errors.append(f"{path}: {error.message}")

    return errors


def validate_document(data: dict[str, Any], schema_dir: Path) -> list[str]:
    """Validate document against schema."""
    schema = load_schema(schema_dir / "document.schema.json")
    return validate_against_schema(data, schema)


def validate_segment(data: dict[str, Any], schema_dir: Path) -> list[str]:
    """Validate segment against schema."""
    schema = load_schema(schema_dir / "segment.schema.json")
    return validate_against_schema(data, schema)


def validate_token(data: dict[str, Any], schema_dir: Path) -> list[str]:
    """Validate token against schema."""
    schema = load_schema(schema_dir / "token.schema.json")
    return validate_against_schema(data, schema)


def validate_manifest(data: dict[str, Any], schema_dir: Path) -> list[str]:
    """Validate manifest against schema."""
    schema = load_schema(schema_dir / "manifest.schema.json")
    return validate_against_schema(data, schema)
