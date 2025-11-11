"""Horner Sahidic NT ingestor."""

import logging
import shutil
from pathlib import Path
from typing import Any

from scripts.ingest.base import BaseIngestor, CurationResult, IngestorConfig
from scripts.models import (
    Document,
    DocumentCounts,
    Provenance,
    Segment,
    Stage,
    Substage,
    Token,
    create_timestamp,
)
from scripts.normalize.coptic_unicode import normalize_and_strip
from scripts.normalize.segmentation import segment_by_blank_lines, segment_by_verse
from scripts.utils.hashing import hash_file, hash_string


class HornerIngestor(BaseIngestor):
    """
    Ingestor for Horner Sahidic NT files.

    Expects plain text files with verse markers or simple text.
    """

    def __init__(self, config: IngestorConfig, logger: logging.Logger):
        super().__init__(config, logger)

        # Get source path from config
        self.source_path = Path(config.source_config.get("path", ""))
        if not self.source_path.exists():
            raise ValueError(f"Horner source path does not exist: {self.source_path}")

    def list_collections(self) -> list[str]:
        """
        List collections (subdirectories in source path).

        Returns:
            List of collection names
        """
        collections = self.config.source_config.get("collections", ["nt"])

        # If "all", discover from filesystem
        if collections == ["all"] and self.source_path.is_dir():
            collections = [d.name for d in self.source_path.iterdir() if d.is_dir()]

        return collections if collections else ["nt"]

    async def pull_collection(self, collection: str) -> Path:
        """
        Copy files from source path to raw directory.

        Args:
            collection: Collection name

        Returns:
            Path to raw data directory
        """
        self.logger.info(f"Pulling Horner collection: {collection}")

        raw_dir = self.get_collection_raw_dir(collection)

        # Determine source directory
        if self.source_path.is_dir():
            source_dir = self.source_path / collection
            if not source_dir.exists():
                source_dir = self.source_path
        else:
            source_dir = self.source_path.parent

        # Copy all text files
        copied = 0
        for text_file in source_dir.glob("*.txt"):
            dest = raw_dir / text_file.name
            shutil.copy2(text_file, dest)
            copied += 1
            self.logger.info(f"Copied {text_file.name} to {dest}")

        self.logger.info(f"Pulled {copied} files for collection {collection}")
        return raw_dir

    def curate_collection(self, collection: str, raw_path: Path) -> CurationResult:
        """
        Curate Horner files into normalized format.

        Args:
            collection: Collection name
            raw_path: Path to raw data

        Returns:
            Curation result
        """
        self.logger.info(f"Curating Horner collection: {collection}")

        documents: list[Document] = []
        segments: list[Segment] = []
        tokens: list[Token] = []

        # Process each text file
        for text_file in raw_path.glob("*.txt"):
            doc_result = self._process_file(text_file, collection)
            documents.append(doc_result["document"])
            segments.extend(doc_result["segments"])
            # Horner doesn't have token-level annotations
            # tokens would remain empty

        # Create manifest
        manifest = self.create_manifest(collection, documents, segments, tokens)

        self.logger.info(f"Curated {len(documents)} documents, {len(segments)} segments")

        return CurationResult(
            documents=documents,
            segments=segments,
            tokens=tokens,
            manifest=manifest,
        )

    def _process_file(self, file_path: Path, collection: str) -> dict[str, Any]:
        """
        Process a single Horner text file.

        Args:
            file_path: Path to text file
            collection: Collection name

        Returns:
            Dict with document and segments
        """
        self.logger.info(f"Processing {file_path.name}")

        # Read file
        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()

        # Hash raw content
        raw_hash = hash_file(file_path)

        # Try to detect if this has verse markers
        verses = segment_by_verse(content)

        # If no verses detected, fall back to paragraph segmentation
        if not verses:
            self.logger.info(f"No verse markers found in {file_path.name}, using paragraphs")
            paragraphs = segment_by_blank_lines(content)
            verses = [(None, para) for para in paragraphs]  # type: ignore[misc]

        # Create document ID
        doc_id = f"horner:work:{file_path.stem}"

        # Create segments
        segments_list = []
        for idx, (ref, text) in enumerate(verses):
            # Normalize Coptic text
            text_canonical, text_stripped = normalize_and_strip(text)

            # Create segment ID
            seg_id = f"v_{ref.replace(' ', '_').replace(':', '_')}" if ref else f"s{idx:06d}"

            segment = Segment(
                document_id=doc_id,
                segment_id=seg_id,
                order=idx,
                text_canonical=text_canonical,
                text_stripped=text_stripped,
                passage_ref=ref,
                dialect="SAHIDIC",
                content_hash=hash_string(text_canonical),
                created_at=create_timestamp(),
            )
            segments_list.append(segment)

        # Create document
        document = Document(
            document_id=doc_id,
            source=self.source_name,
            collection=collection,
            stage=Stage.COPTIC,
            substage=Substage.SAHIDIC,
            language="cop",
            title=file_path.stem.replace("_", " ").title(),
            authors=["UNKNOWN"],
            genre=["biblical", "nt"],
            provenance=Provenance(
                source_item_id=str(file_path),
                retrieved_at=create_timestamp(),
                hash_raw=raw_hash,
                parser_version="horner@0.1.0",
            ),
            counts=DocumentCounts(
                segments=len(segments_list),
                tokens=0,
            ),
        )

        return {
            "document": document,
            "segments": segments_list,
        }
