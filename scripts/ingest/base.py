"""Base ingestor interface."""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from scripts.models import Document, Manifest, Segment, Token, create_timestamp
from scripts.utils.hashing import hash_file
from scripts.utils.io import ensure_dir, write_json, write_jsonl


@dataclass
class IngestorConfig:
    """Configuration for an ingestor."""

    source_name: str
    source_config: dict[str, Any]
    settings: dict[str, Any]
    paths: dict[str, Path]
    schema_dir: Path


@dataclass
class CurationResult:
    """Result of a curation operation."""

    documents: list[Document]
    segments: list[Segment]
    tokens: list[Token]
    manifest: Manifest


class BaseIngestor(ABC):
    """
    Abstract base class for source ingestors.

    Subclasses must implement:
    - list_collections()
    - pull_collection()
    - curate_collection()
    """

    def __init__(self, config: IngestorConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.source_name = config.source_name

        # Setup paths
        self.raw_dir = config.paths["raw"] / self.source_name
        self.curated_dir = config.paths["curated"] / self.source_name

    @abstractmethod
    def list_collections(self) -> list[str]:
        """
        List available collections from this source.

        Returns:
            List of collection names
        """
        pass

    @abstractmethod
    async def pull_collection(self, collection: str) -> Path:
        """
        Download raw data for a collection.

        Args:
            collection: Collection name

        Returns:
            Path to downloaded raw data directory
        """
        pass

    @abstractmethod
    def curate_collection(self, collection: str, raw_path: Path) -> CurationResult:
        """
        Curate raw data into normalized format.

        Args:
            collection: Collection name
            raw_path: Path to raw data

        Returns:
            Curation result with documents, segments, tokens, and manifest
        """
        pass

    def get_collection_raw_dir(self, collection: str) -> Path:
        """Get raw data directory for collection."""
        return ensure_dir(self.raw_dir / collection)

    def get_collection_curated_dir(self, collection: str) -> Path:
        """Get curated data directory for collection."""
        return ensure_dir(self.curated_dir / collection)

    def write_curated_data(
        self,
        collection: str,
        curation_result: CurationResult,
    ) -> None:
        """
        Write curated data to disk atomically.

        Args:
            collection: Collection name
            curation_result: Curation result to write
        """
        curated_dir = self.get_collection_curated_dir(collection)

        # Write documents JSONL
        doc_path = curated_dir / "documents.jsonl"
        doc_count = write_jsonl(
            doc_path,
            (doc.to_dict() for doc in curation_result.documents),
        )
        self.logger.info(f"Wrote {doc_count} documents to {doc_path}")

        # Write segments Parquet
        if curation_result.segments:
            seg_path = curated_dir / "segments.parquet"
            seg_df = pd.DataFrame([seg.to_dict() for seg in curation_result.segments])
            self._write_parquet(seg_df, seg_path)
            self.logger.info(f"Wrote {len(seg_df)} segments to {seg_path}")

        # Write tokens Parquet if available
        if curation_result.tokens:
            tok_path = curated_dir / "tokens.parquet"
            tok_df = pd.DataFrame([tok.to_dict() for tok in curation_result.tokens])
            self._write_parquet(tok_df, tok_path)
            self.logger.info(f"Wrote {len(tok_df)} tokens to {tok_path}")

        # Write manifest
        manifest_path = curated_dir / "manifest.json"
        write_json(manifest_path, curation_result.manifest.to_dict())
        self.logger.info(f"Wrote manifest to {manifest_path}")

    def _write_parquet(self, df: pd.DataFrame, path: Path) -> None:
        """Write DataFrame to Parquet with configured compression."""
        compression = self.config.settings.get("parquet", {}).get("compression", "zstd")
        compression_level = self.config.settings.get("parquet", {}).get("compression_level", 3)

        df.to_parquet(
            path,
            engine="pyarrow",
            compression=compression,
            compression_level=compression_level,
            index=False,
        )

    def create_manifest(
        self,
        collection: str,
        documents: list[Document],
        segments: list[Segment],
        tokens: list[Token],
    ) -> Manifest:
        """
        Create manifest for curated collection.

        Args:
            collection: Collection name
            documents: List of documents
            segments: List of segments
            tokens: List of tokens

        Returns:
            Manifest object
        """
        from scripts.models import Artifact, CollectionCounts

        curated_dir = self.get_collection_curated_dir(collection)
        artifacts = []

        # Document artifact
        doc_path = curated_dir / "documents.jsonl"
        if doc_path.exists():
            artifacts.append(
                Artifact(
                    path=str(doc_path.relative_to(self.config.paths["curated"])),
                    type="documents",
                    hash=hash_file(doc_path),
                    size_bytes=doc_path.stat().st_size,
                    row_count=len(documents),
                )
            )

        # Segments artifact
        seg_path = curated_dir / "segments.parquet"
        if seg_path.exists():
            artifacts.append(
                Artifact(
                    path=str(seg_path.relative_to(self.config.paths["curated"])),
                    type="segments",
                    hash=hash_file(seg_path),
                    size_bytes=seg_path.stat().st_size,
                    row_count=len(segments),
                )
            )

        # Tokens artifact
        tok_path = curated_dir / "tokens.parquet"
        if tok_path.exists():
            artifacts.append(
                Artifact(
                    path=str(tok_path.relative_to(self.config.paths["curated"])),
                    type="tokens",
                    hash=hash_file(tok_path),
                    size_bytes=tok_path.stat().st_size,
                    row_count=len(tokens),
                )
            )

        return Manifest(
            source=self.source_name,
            collection=collection,
            version=self.config.settings.get("schema_versions", {}).get("manifest", 1),
            created_at=create_timestamp(),
            parser_version=f"{self.source_name}@0.1.0",
            artifacts=artifacts,
            counts=CollectionCounts(
                documents=len(documents),
                segments=len(segments),
                tokens=len(tokens),
            ),
        )
