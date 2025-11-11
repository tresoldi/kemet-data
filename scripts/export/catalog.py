"""Catalog generation utilities."""

import logging
from pathlib import Path
from typing import Any

from scripts.utils.io import read_json


def build_catalog(curated_dir: Path, logger: logging.Logger) -> dict[str, Any]:
    """
    Build catalog from curated collections.

    Args:
        curated_dir: Path to curated data directory
        logger: Logger instance

    Returns:
        Catalog data dictionary
    """
    catalog: dict[str, Any] = {
        "version": 1,
        "collections": [],
    }

    # Find all source directories
    if not curated_dir.exists():
        logger.warning(f"Curated directory not found: {curated_dir}")
        return catalog

    for source_dir in curated_dir.iterdir():
        if not source_dir.is_dir():
            continue

        source_name = source_dir.name

        # Find all collection directories under this source
        for collection_dir in source_dir.iterdir():
            if not collection_dir.is_dir():
                continue

            collection_name = collection_dir.name
            manifest_path = collection_dir / "manifest.json"

            if not manifest_path.exists():
                logger.warning(f"No manifest found for {source_name}/{collection_name}")
                continue

            # Read manifest
            manifest = read_json(manifest_path)

            # Extract info for catalog
            collection_entry = {
                "source": source_name,
                "collection": collection_name,
                "path": str(collection_dir.relative_to(curated_dir)),
                "created_at": manifest.get("created_at"),
                "parser_version": manifest.get("parser_version"),
                "counts": manifest.get("counts", {}),
                "artifacts": [
                    {
                        "path": a["path"],
                        "type": a["type"],
                        "size_bytes": a["size_bytes"],
                        "row_count": a.get("row_count"),
                    }
                    for a in manifest.get("artifacts", [])
                ],
            }

            catalog["collections"].append(collection_entry)
            logger.info(f"Added {source_name}/{collection_name} to catalog")

    logger.info(f"Built catalog with {len(catalog['collections'])} collections")
    return catalog
