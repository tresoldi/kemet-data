"""Ramses Online Late Egyptian corpus ingestor."""

import logging
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, ClassVar

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
from scripts.utils.hashing import hash_string


class RamsesIngestor(BaseIngestor):
    """
    Ingestor for Ramses Online Late Egyptian corpus.

    The Ramses corpus is a richly annotated historical corpus of Late Egyptian texts
    from the 18th dynasty down to the Third Intermediate Period (ca. 1350-700 BCE).

    Data format:
    - Source files (src-*.txt): Space-separated Gardiner codes (hieroglyphic signs)
    - Target files (tgt-*.txt): Space-separated transliterations with _ for word boundaries
    - Parallel line-by-line correspondence between source and target files

    License: CC-BY-NC-SA 4.0 (Université de Liège/Projet Ramsès)
    Citation: "the Ramses transliteration corpus V. 2019-09-01, University of Liege/Projet Ramses"
    """

    # Available dataset splits
    COLLECTIONS: ClassVar[list[str]] = ["train", "val", "test"]

    # Zenodo DOI and download URL
    ZENODO_DOI = "10.5281/zenodo.4954597"
    ZENODO_DOWNLOAD_URL = "https://zenodo.org/records/4954597/files/ramses-trl_2021_05_29.zip?download=1"

    def __init__(self, config: IngestorConfig, logger: logging.Logger):
        super().__init__(config, logger)

    def list_collections(self) -> list[str]:
        """List available Ramses dataset splits."""
        return self.COLLECTIONS

    def get_collection_raw_dir(self, collection: str) -> Path:
        """
        Override to return the Ramses data directory.

        All collections are in ramses-trl/data/
        """
        ramses_data_dir = self.raw_dir / "ramses-trl" / "data"
        return ramses_data_dir

    async def pull_collection(self, collection: str) -> Path:
        """
        Download Ramses data from Zenodo if not present.
        """
        if collection not in self.COLLECTIONS:
            raise ValueError(f"Unknown Ramses collection: {collection}")

        self.logger.info(f"Pulling Ramses collection: {collection}")

        raw_dir = self.get_collection_raw_dir(collection)

        # Download and extract if not present
        if not raw_dir.exists():
            self.logger.info("Downloading Ramses corpus from Zenodo")
            self.raw_dir.mkdir(parents=True, exist_ok=True)

            # Download zip file
            zip_path = self.raw_dir / "ramses-trl.zip"
            self.logger.info(f"Downloading {self.ZENODO_DOWNLOAD_URL}")

            urllib.request.urlretrieve(self.ZENODO_DOWNLOAD_URL, zip_path)
            self.logger.info(f"Downloaded {zip_path.stat().st_size / 1024 / 1024:.1f} MB")

            # Extract zip file
            self.logger.info(f"Extracting {zip_path}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.raw_dir)

            # Clean up zip file
            zip_path.unlink()
            self.logger.info(f"Downloaded and extracted to {raw_dir}")

        # Verify data files exist
        src_file = raw_dir / f"src-{collection}.txt"
        tgt_file = raw_dir / f"tgt-{collection}.txt"

        if not src_file.exists():
            raise FileNotFoundError(f"Ramses source file not found: {src_file}")
        if not tgt_file.exists():
            raise FileNotFoundError(f"Ramses target file not found: {tgt_file}")

        self.logger.info(f"Found Ramses data: {src_file} and {tgt_file}")
        return raw_dir

    def curate_collection(self, collection: str, raw_path: Path) -> CurationResult:
        """
        Curate Ramses data into normalized format.

        Creates one collective document per split, with each line pair
        as a segment containing transliterated text and tokens.
        """
        self.logger.info(f"Curating Ramses collection: {collection}")

        # Load parallel data
        src_file = raw_path / f"src-{collection}.txt"
        tgt_file = raw_path / f"tgt-{collection}.txt"

        with src_file.open("r", encoding="utf-8") as f:
            src_lines = [line.strip() for line in f if line.strip()]

        with tgt_file.open("r", encoding="utf-8") as f:
            tgt_lines = [line.strip() for line in f if line.strip()]

        if len(src_lines) != len(tgt_lines):
            raise ValueError(
                f"Mismatched line counts: {len(src_lines)} source lines vs {len(tgt_lines)} target lines"
            )

        self.logger.info(f"Loaded {len(src_lines)} parallel sentence pairs from {collection}")

        # Process sentences
        result = self._process_sentences(src_lines, tgt_lines, collection)

        # Create manifest
        manifest = self.create_manifest(
            collection, result["documents"], result["segments"], result["tokens"]
        )

        self.logger.info(
            f"Curated {len(result['documents'])} documents, "
            f"{len(result['segments'])} segments, "
            f"{len(result['tokens'])} tokens for {collection}"
        )

        return CurationResult(
            documents=result["documents"],
            segments=result["segments"],
            tokens=result["tokens"],
            manifest=manifest,
        )

    def _process_sentences(self, src_lines: list[str], tgt_lines: list[str], collection: str) -> dict[str, Any]:
        """Process Ramses parallel sentences into normalized format."""
        documents = []
        segments = []
        tokens = []

        # Create single collective document for the split
        document_id = f"ramses:{collection}"

        # Count total tokens
        total_tokens = 0
        for tgt_line in tgt_lines:
            # Split by spaces and count non-underscore tokens (underscores are word boundaries)
            words = [w for w in tgt_line.split() if w != "_"]
            total_tokens += len(words)

        document = Document(
            document_id=document_id,
            source="ramses",
            collection=collection,
            stage=Stage.EGYPTIAN,
            substage=Substage.LATE_EGYPTIAN,
            language="egy",
            title=f"Ramses Late Egyptian Corpus ({collection.title()} Split)",
            authors=["Université de Liège/Projet Ramsès"],
            date_from=-1350,  # 18th dynasty
            date_to=-700,     # Third Intermediate Period
            genre=["administrative", "literary", "religious", "funerary"],
            license="CC-BY-NC-SA 4.0",
            provenance=Provenance(
                source_item_id=f"ramses-{collection}",
                retrieved_at=create_timestamp(),
                hash_raw=hash_string("\n".join(src_lines) + "\n".join(tgt_lines)),
                parser_version="1.0.0",
            ),
            counts=DocumentCounts(
                segments=len(src_lines),
                tokens=total_tokens,
            ),
            metadata={
                "corpus_version": "2019-09-01",
                "zenodo_doi": self.ZENODO_DOI,
                "description": "Annotated corpus of Late Egyptian texts",
                "period": "New Kingdom to Third Intermediate Period (ca. 1350-700 BCE)",
                "citation": "the Ramses transliteration corpus V. 2019-09-01, University of Liege/Projet Ramses",
            },
        )
        documents.append(document)

        # Process each sentence pair
        for seg_idx, (src_line, tgt_line) in enumerate(zip(src_lines, tgt_lines, strict=True)):
            segment_id = f"{document_id}:seg{seg_idx:05d}"

            # Parse transliteration (target)
            words = self._parse_transliteration(tgt_line)
            text_canonical = " ".join(words)

            # Create segment
            segment = Segment(
                document_id=document_id,
                segment_id=segment_id,
                order=seg_idx,
                text_canonical=text_canonical,
                text_stripped=text_canonical.replace(" ", ""),
                content_hash=hash_string(text_canonical),
                created_at=create_timestamp(),
                metadata={
                    "gardiner_codes": src_line,  # Store original hieroglyphic encoding
                },
            )
            segments.append(segment)

            # Create tokens
            for tok_idx, word in enumerate(words):
                if not word or word in ["LACUNA", "MISSING", "SHADED2"]:
                    # Skip damage markers
                    continue

                token_id = f"{segment_id}:tok{tok_idx:03d}"

                # Parse token metadata
                token_meta = self._parse_token(word)

                token = Token(
                    document_id=document_id,
                    segment_id=segment_id,
                    token_id=token_id,
                    order=tok_idx,
                    form=token_meta["form"],
                    form_norm=token_meta["form_norm"],
                    lemma=token_meta["lemma"],
                    pos=token_meta.get("pos"),
                    lang="egy",
                    content_hash=hash_string(token_meta["form"]),
                    metadata=token_meta.get("metadata"),
                )
                tokens.append(token)

        return {
            "documents": documents,
            "segments": segments,
            "tokens": tokens,
        }

    def _parse_transliteration(self, tgt_line: str) -> list[str]:
        """
        Parse transliteration line into words.

        Underscores (_) mark word boundaries. Remove them and return words.
        """
        # Split by whitespace and remove underscores
        parts = tgt_line.split()
        words: list[str] = []
        current_word: list[str] = []

        for part in parts:
            if part == "_":
                # Word boundary - save current word if any
                if current_word:
                    words.append("".join(current_word))
                    current_word = []
            else:
                current_word.append(part)

        # Add final word if any
        if current_word:
            words.append("".join(current_word))

        return words

    def _parse_token(self, word: str) -> dict[str, Any]:
        """
        Parse a token and extract linguistic features.

        Ramses transliteration conventions:
        - . (dot) separates morpheme boundaries (e.g., jrj.t = infinitive)
        - = marks pronominal suffixes (e.g., =f = his)
        - ( ) marks optional/reconstructed parts
        - [ ] marks damaged/uncertain readings
        """
        form = word
        form_norm = word
        lemma = None
        metadata = {}

        # Extract suffix pronouns (marked with =)
        if "=" in word:
            parts = word.split("=")
            lemma = parts[0]
            metadata["suffix"] = "=" + parts[1]
        # Extract morphological boundaries (marked with .)
        elif "." in word:
            lemma = word.split(".")[0]  # Base form is first element
            metadata["morphology"] = word
        else:
            lemma = word

        # Normalize: remove brackets and parentheses
        form_norm = form_norm.replace("(", "").replace(")", "")
        form_norm = form_norm.replace("[", "").replace("]", "")

        if lemma:
            lemma = lemma.replace("(", "").replace(")", "")
            lemma = lemma.replace("[", "").replace("]", "")

        return {
            "form": form,
            "form_norm": form_norm,
            "lemma": lemma if lemma and lemma != form_norm else None,
            "metadata": metadata if metadata else None,
        }

