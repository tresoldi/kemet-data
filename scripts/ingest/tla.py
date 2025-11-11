"""TLA (Thesaurus Linguae Aegyptiae) ingestor."""

import logging
from pathlib import Path
from typing import Any, ClassVar

from datasets import load_dataset

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


class TLAIngestor(BaseIngestor):
    """
    Ingestor for TLA (Thesaurus Linguae Aegyptiae) data from Hugging Face.

    Ingests Egyptian texts (Earlier Egyptian and Demotic) from the TLA v18
    premium datasets on Hugging Face.
    """

    # HuggingFace dataset names
    DATASETS: ClassVar[dict[str, str]] = {
        "earlier_egyptian": "thesaurus-linguae-aegyptiae/tla-Earlier_Egyptian_original-v18-premium",
        "late_egyptian": "thesaurus-linguae-aegyptiae/tla-late_egyptian-v19-premium",
        "demotic": "thesaurus-linguae-aegyptiae/tla-demotic-v18-premium",
    }

    def __init__(self, config: IngestorConfig, logger: logging.Logger):
        super().__init__(config, logger)

    def list_collections(self) -> list[str]:
        """
        List available TLA collections.

        Returns:
            List of collection names
        """
        return list(self.DATASETS.keys())

    async def pull_collection(self, collection: str) -> Path:
        """
        Download TLA dataset from Hugging Face.

        Args:
            collection: Collection name ('earlier_egyptian' or 'demotic')

        Returns:
            Path to raw data directory
        """
        if collection not in self.DATASETS:
            raise ValueError(f"Unknown TLA collection: {collection}")

        self.logger.info(f"Pulling TLA collection: {collection}")

        raw_dir = self.get_collection_raw_dir(collection)

        # Load dataset from Hugging Face
        dataset_name = self.DATASETS[collection]
        self.logger.info(f"Loading dataset from Hugging Face: {dataset_name}")

        try:
            dataset = load_dataset(dataset_name, split="train")
            self.logger.info(f"Loaded {len(dataset)} sentences")

            # Save to parquet for caching
            output_file = raw_dir / "sentences.parquet"
            dataset.to_parquet(str(output_file))
            self.logger.info(f"Saved to {output_file}")

            return raw_dir

        except Exception as e:
            self.logger.error(f"Failed to load dataset {dataset_name}: {e}")
            raise

    def curate_collection(self, collection: str, raw_path: Path) -> CurationResult:
        """
        Curate TLA data into normalized format.

        Args:
            collection: Collection name
            raw_path: Path to raw data

        Returns:
            Curation result
        """
        self.logger.info(f"Curating TLA collection: {collection}")

        # Load the parquet file
        import pandas as pd

        parquet_file = raw_path / "sentences.parquet"
        if not parquet_file.exists():
            raise FileNotFoundError(f"Raw data not found: {parquet_file}")

        df = pd.read_parquet(parquet_file)
        self.logger.info(f"Loaded {len(df)} sentences from {parquet_file}")

        # Process sentences into documents, segments, and tokens
        result = self._process_sentences(df, collection)

        # Create manifest
        manifest = self.create_manifest(
            collection, result["documents"], result["segments"], result["tokens"]
        )

        self.logger.info(
            f"Curated {len(result['documents'])} documents, "
            f"{len(result['segments'])} segments, "
            f"{len(result['tokens'])} tokens"
        )

        return CurationResult(
            documents=result["documents"],
            segments=result["segments"],
            tokens=result["tokens"],
            manifest=manifest,
        )

    def _process_sentences(self, df: Any, collection: str) -> dict[str, Any]:
        """
        Process TLA sentences into normalized format.

        Since TLA data is sentence-level, we create:
        - One document per collection (representing the entire corpus subset)
        - One segment per sentence
        - One token per word

        Args:
            df: DataFrame with TLA sentences
            collection: Collection name

        Returns:
            Dict with documents, segments, and tokens lists
        """
        documents = []
        segments = []
        tokens = []

        # Determine stage and substage based on collection
        if collection == "earlier_egyptian":
            stage = Stage.EGYPTIAN
            substage = Substage.MIDDLE_EGYPTIAN  # Most common in this dataset
            script = "HIEROGLYPHIC"
        elif collection == "late_egyptian":
            stage = Stage.EGYPTIAN
            substage = Substage.LATE_EGYPTIAN
            script = "HIEROGLYPHIC"
        elif collection == "demotic":
            stage = Stage.EGYPTIAN
            substage = Substage.DEMOTIC
            script = "DEMOTIC"
        else:
            raise ValueError(f"Unknown collection: {collection}")

        # Create a single document for the collection
        # Get date range (filter out empty strings)
        date_from = None
        date_to = None
        if "dateNotBefore" in df.columns:
            valid_dates = df["dateNotBefore"][df["dateNotBefore"] != ""]
            if len(valid_dates) > 0:
                date_from = int(valid_dates.min())
        if "dateNotAfter" in df.columns:
            valid_dates = df["dateNotAfter"][df["dateNotAfter"] != ""]
            if len(valid_dates) > 0:
                date_to = int(valid_dates.max())

        # Count stats
        num_segments = len(df)
        num_tokens = sum(len(row["transliteration"].split()) for _, row in df.iterrows())

        document = Document(
            document_id=f"tla:{collection}",
            source="tla",
            collection=collection,
            stage=stage,
            substage=substage,
            language="egy",  # ISO 639-3 code for Egyptian
            title=f"TLA {collection.replace('_', ' ').title()} Corpus v18",
            authors=["Thesaurus Linguae Aegyptiae Team"],
            date_from=date_from,
            date_to=date_to,
            genre=["various"],
            license="CC BY-SA 4.0",
            provenance=Provenance(
                source_item_id=collection,
                retrieved_at=create_timestamp(),
                hash_raw="",  # No raw file hash for HuggingFace datasets
                parser_version="v1.0",
            ),
            counts=DocumentCounts(
                segments=num_segments,
                tokens=num_tokens,
            ),
            metadata={
                "dataset": self.DATASETS[collection],
                "version": "v18-premium",
                "description": "TLA premium dataset (fully intact sentences)",
            },
        )
        documents.append(document)

        # Process each sentence as a segment
        for idx, row in df.iterrows():
            segment_id = f"{collection}_s{idx:05d}"

            # Extract text content
            transliteration = row["transliteration"]
            hieroglyphs = row.get("hieroglyphs", None)  # Only in earlier_egyptian
            translation_de = row["translation"]

            # Create segment
            # Note: text_en, text_de, text_hieroglyphs are now direct fields, not in metadata
            segment_metadata = {
                "script": script,
            }

            segment = Segment(
                segment_id=segment_id,
                document_id=document.document_id,
                order=idx,
                text_canonical=transliteration,  # Use transliteration as canonical
                text_stripped=transliteration,  # Same for now
                text_hieroglyphs=hieroglyphs,  # Direct field for hieroglyphs
                text_en=None,  # TLA has German translations
                text_de=translation_de,  # Direct field for German translation
                translation_language="de" if translation_de else None,
                passage_ref=None,
                dialect=substage,
                content_hash=hash_string(transliteration),
                created_at=create_timestamp(),
                metadata=segment_metadata,
            )
            segments.append(segment)

            # Create tokens
            trans_words = transliteration.split()
            lemmas = row.get("lemmatization", "").split()
            pos_tags = row.get("UPOS", "").split()
            glossing = row.get("glossing", "").split()

            # Process lemmatization (format: "lemma_id|lemma")
            lemma_ids = []
            lemma_texts = []
            for lemma in lemmas:
                if "|" in lemma:
                    lid, ltext = lemma.split("|", 1)
                    lemma_ids.append(lid)
                    lemma_texts.append(ltext)
                else:
                    lemma_ids.append(None)
                    lemma_texts.append(lemma)

            # Create a token for each word
            for word_idx, word in enumerate(trans_words):
                token_id = f"{segment_id}_t{word_idx:04d}"

                # Put Egyptian-specific fields in metadata
                token_metadata = {
                    "form_transliterated": word,  # Already transliterated
                    "lemma_id": lemma_ids[word_idx] if word_idx < len(lemma_ids) else None,
                    "glossing": glossing[word_idx] if word_idx < len(glossing) else None,
                    "head": None,  # No dependency parsing
                    "deprel": None,
                    "xpos": None,
                }

                token = Token(
                    token_id=token_id,
                    segment_id=segment_id,
                    document_id=document.document_id,
                    order=word_idx,
                    form=word,  # Original transliterated form
                    form_norm=word,  # No normalization for now
                    lemma=lemma_texts[word_idx] if word_idx < len(lemma_texts) else None,
                    pos=pos_tags[word_idx] if word_idx < len(pos_tags) else None,
                    morph=None,  # Not in dataset
                    lang="egy",
                    content_hash=hash_string(word),
                    metadata=token_metadata,
                )
                tokens.append(token)

        return {
            "documents": documents,
            "segments": segments,
            "tokens": tokens,
        }
