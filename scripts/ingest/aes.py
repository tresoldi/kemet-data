"""AES (Ancient Egyptian Sentences) corpus ingestor."""

import json
import logging
import subprocess
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


class AESIngestor(BaseIngestor):
    """
    Ingestor for AES (Ancient Egyptian Sentences) corpus.

    Ingests 100,000+ Egyptian sentences from the AES corpus on GitHub,
    spanning all periods from Old Kingdom through Late Period.
    """

    REPO_URL = "https://github.com/simondschweitzer/aes.git"

    # Subcorpora available
    SUBCORPORA: ClassVar[list[str]] = [
        "bbawamarna",        # Amarna Period texts
        "bbawarchive",       # Archive texts
        "bbawbriefe",        # Letters
        "bbawfelsinschriften",  # Rock inscriptions
        "bbawgrabinschriften",  # Tomb inscriptions
        "bbawgraeberspzt",   # Late Period tombs
        "bbawhistbiospzt",   # Historical-biographical texts (Late Period)
        "bbawpyramidentexte",  # Pyramid Texts
        "bbawramessiden",    # Ramesside Period texts
        "bbawtempelbib",     # Temple libraries
        "bbawtotenlit",      # Funerary literature
        "sawlit",            # Literary texts
        "sawmedizin",        # Medical texts
        "smaek",             # SMAEK collection
        "tb",                # Book of the Dead
        "tuebingerstelen",   # Tübingen stelae
    ]

    # Period mapping for substage determination
    PERIOD_MAP: ClassVar[dict[str, Substage]] = {
        "AR": Substage.MIDDLE_EGYPTIAN,      # Old Kingdom (Archaic/Early Dynastic)
        "OK": Substage.MIDDLE_EGYPTIAN,      # Old Kingdom
        "MR": Substage.MIDDLE_EGYPTIAN,      # Middle Kingdom
        "2. ZwZt": Substage.MIDDLE_EGYPTIAN, # Second Intermediate Period
        "NK": Substage.LATE_EGYPTIAN,        # New Kingdom
        "3. ZwZt": Substage.LATE_EGYPTIAN,   # Third Intermediate Period
        "Sp": Substage.DEMOTIC,              # Late Period
        "Ptolem": Substage.DEMOTIC,          # Ptolemaic
        "Röm": Substage.DEMOTIC,             # Roman
    }

    def __init__(self, config: IngestorConfig, logger: logging.Logger):
        super().__init__(config, logger)

    def list_collections(self) -> list[str]:
        """List available AES subcorpora."""
        return self.SUBCORPORA

    def get_collection_raw_dir(self, collection: str) -> Path:
        """
        Override to return the AES data directory for all collections.

        AES uses a single repository with all subcorpora in one location.
        """
        # self.raw_dir is already data/raw/aes (BaseIngestor adds source_name)
        # Repository is cloned to data/raw/aes/aes
        # Data files are in aes/files/aes
        aes_data_dir = self.raw_dir / "aes" / "files" / "aes"
        return aes_data_dir

    async def pull_collection(self, collection: str) -> Path:
        """
        Clone AES repository if not present, or update if it exists.
        """
        if collection not in self.SUBCORPORA:
            raise ValueError(f"Unknown AES subcorpus: {collection}")

        self.logger.info(f"Pulling AES collection: {collection}")

        # Clone repository if needed (only once for all collections)
        repo_dir = self.raw_dir / "aes"

        if not repo_dir.exists():
            self.logger.info(f"Cloning AES repository to {repo_dir}")
            self.raw_dir.mkdir(parents=True, exist_ok=True)

            subprocess.run(
                ["git", "clone", "--depth", "1", self.REPO_URL, str(repo_dir)],
                check=True,
                capture_output=True,
                text=True
            )
            self.logger.info("Repository cloned successfully")
        else:
            self.logger.info(f"Updating existing repository at {repo_dir}")
            subprocess.run(
                ["git", "-C", str(repo_dir), "pull"],
                check=True,
                capture_output=True,
                text=True
            )

        # Get commit hash for provenance
        result = subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True
        )
        commit_hash = result.stdout.strip()
        self.logger.info(f"Using commit: {commit_hash}")

        # Get the AES data directory
        raw_dir = self.get_collection_raw_dir(collection)

        # Check if the JSON file exists
        json_file = raw_dir / f"_aes_{collection}.json"
        if not json_file.exists():
            raise FileNotFoundError(f"AES data file not found: {json_file}")

        self.logger.info(f"Found AES data at {json_file}")
        return raw_dir

    def curate_collection(self, collection: str, raw_path: Path) -> CurationResult:
        """
        Curate AES data into normalized format.

        Creates one collective document per subcorpus, with each sentence
        as a segment and each word as a token.
        """
        self.logger.info(f"Curating AES collection: {collection}")

        # Load JSON data
        json_file = raw_path / f"_aes_{collection}.json"
        with json_file.open("r", encoding="utf-8") as f:
            data = json.load(f)

        self.logger.info(f"Loaded {len(data)} sentences from {json_file}")

        # Process sentences
        result = self._process_sentences(data, collection)

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

    def _process_sentences(self, data: dict[str, Any], collection: str) -> dict[str, Any]:
        """Process AES sentences into normalized format."""
        documents = []
        segments = []
        tokens = []

        # Collect statistics for document
        total_sentences = len(data)
        total_tokens = sum(len(sent.get("token", [])) for sent in data.values())

        # Sample some sentences to determine typical period
        sample_dates = [
            sent.get("date", "")
            for sent in list(data.values())[:100]
            if sent.get("date")
        ]

        # Determine dominant substage
        substage_counts: dict[Substage, int] = {}
        for date_str in sample_dates:
            substage = self._map_date_to_substage(date_str)
            substage_counts[substage] = substage_counts.get(substage, 0) + 1

        # Use most common substage, default to MIDDLE_EGYPTIAN
        if substage_counts:
            dominant_substage = max(substage_counts.items(), key=lambda x: x[1])[0]
        else:
            dominant_substage = Substage.MIDDLE_EGYPTIAN

        # Create single collective document
        document = Document(
            document_id=f"aes:{collection}",
            source="aes",
            collection=collection,
            stage=Stage.EGYPTIAN,
            substage=dominant_substage,
            language="egy",
            title=f"AES {collection.replace('_', ' ').title()} Corpus",
            authors=["AED-TEI Contributors"],
            date_from=None,
            date_to=None,
            genre=self._get_genre(collection),
            license="CC BY-SA 4.0",
            provenance=Provenance(
                source_item_id=collection,
                retrieved_at=create_timestamp(),
                hash_raw="",
                parser_version="aes@1.0",
            ),
            counts=DocumentCounts(
                segments=total_sentences,
                tokens=total_tokens,
            ),
            metadata={
                "source_repo": self.REPO_URL,
                "subcorpus": collection,
            },
        )
        documents.append(document)

        # Process each sentence
        for idx, (sent_id, sent_data) in enumerate(data.items()):
            segment, sent_tokens = self._create_segment_and_tokens(
                document.document_id, sent_id, sent_data, idx
            )
            segments.append(segment)
            tokens.extend(sent_tokens)

        return {
            "documents": documents,
            "segments": segments,
            "tokens": tokens,
        }

    def _create_segment_and_tokens(
        self, document_id: str, sent_id: str, sent_data: dict[str, Any], order: int
    ) -> tuple[Segment, list[Token]]:
        """Create a Segment and Tokens from an AES sentence."""

        # Extract sentence-level data
        text_id = sent_data.get("text", "")
        owner = sent_data.get("owner", "")
        corpus = sent_data.get("corpus", "")
        date_str = sent_data.get("date", "")
        findspot = sent_data.get("findspot", "")
        translation_de = sent_data.get("sentence_translation", "")
        token_data = sent_data.get("token", [])

        # Build canonical text from transliterations
        text_canonical = " ".join(
            tok.get("written_form", "")
            for tok in token_data
            if tok.get("written_form")
        )

        # Determine substage for this sentence
        substage = self._map_date_to_substage(date_str)

        # Create segment
        segment = Segment(
            segment_id=f"{sent_id}",
            document_id=document_id,
            order=order,
            text_canonical=text_canonical,
            text_stripped=text_canonical,  # No diacritics in transliteration
            passage_ref=text_id,
            dialect=substage.value if substage else None,
            content_hash=hash_string(text_canonical),
            created_at=create_timestamp(),
            metadata={
                "text_id": text_id,
                "text_de": translation_de,
                "editor": owner,
                "date_period": date_str,
                "findspot": findspot,
                "corpus": corpus,
            },
        )

        # Create tokens
        segment_tokens = []
        for tok_idx, tok in enumerate(token_data):
            token_id = tok.get("_id", f"{sent_id}_t{tok_idx:04d}")

            # Extract token fields
            written_form = tok.get("written_form", "")
            mdc = tok.get("mdc", "")
            lemma_id = tok.get("lemmaID", "")
            lemma_form = tok.get("lemma_form", "")
            cotext_translation = tok.get("cotext_translation", "")
            hiero = tok.get("hiero", "")
            hiero_unicode = tok.get("hiero_unicode", "")
            pos = tok.get("pos", "")

            # Build morphology features string
            morph_features = []
            for field in ["voice", "genus", "numerus", "inflection", "morphology"]:
                if tok.get(field):
                    morph_features.append(f"{field}={tok[field]}")
            morph_str = "|".join(morph_features) if morph_features else None

            # Create token
            token = Token(
                token_id=token_id,
                segment_id=sent_id,
                document_id=document_id,
                order=tok_idx,
                form=written_form,
                form_norm=written_form,
                lemma=lemma_form if lemma_form else None,
                pos=pos if pos and pos != "undefined" else None,
                morph=morph_str,
                lang="egy",
                content_hash=hash_string(written_form),
                metadata={
                    "mdc": mdc,
                    "lemma_id": lemma_id,
                    "cotext_translation": cotext_translation,
                    "hiero_gardiner": hiero,
                    "hiero_unicode": hiero_unicode,
                    "hiero_inventar": tok.get("hiero_inventar"),
                    "line_count": tok.get("lineCount"),
                    # Detailed morphology
                    "voice": tok.get("voice"),
                    "genus": tok.get("genus"),
                    "numerus": tok.get("numerus"),
                    "pronoun": tok.get("pronoun"),
                    "name": tok.get("name"),
                    "number": tok.get("number"),
                    "epitheton": tok.get("epitheton"),
                    "adjective": tok.get("adjective"),
                    "particle": tok.get("particle"),
                    "adverb": tok.get("adverb"),
                    "verbal_class": tok.get("verbalClass"),
                    "status": tok.get("status"),
                },
            )
            segment_tokens.append(token)

        return segment, segment_tokens

    def _map_date_to_substage(self, date_str: str) -> Substage:
        """Map AES date string to Egyptian substage."""
        if not date_str:
            return Substage.MIDDLE_EGYPTIAN  # Default

        # Try exact match first
        if date_str in self.PERIOD_MAP:
            return self.PERIOD_MAP[date_str]

        # Try partial match (e.g., "NK (Amenhotep III.)" -> "NK")
        for period_code, substage in self.PERIOD_MAP.items():
            if date_str.startswith(period_code):
                return substage

        # Default to Middle Egyptian
        return Substage.MIDDLE_EGYPTIAN

    def _get_genre(self, collection: str) -> list[str]:
        """Determine genre from collection name."""
        genre_map = {
            "bbawpyramidentexte": ["funerary", "religious"],
            "tb": ["funerary", "religious"],
            "bbawtotenlit": ["funerary"],
            "bbawgrabinschriften": ["funerary", "biographical"],
            "sawlit": ["literary"],
            "sawmedizin": ["medical"],
            "bbawbriefe": ["epistolary"],
            "bbawramessiden": ["various"],
            "bbawamarna": ["various"],
            "bbawarchive": ["administrative"],
            "bbawtempelbib": ["religious"],
            "bbawhistbiospzt": ["biographical", "historical"],
            "bbawfelsinschriften": ["commemorative"],
            "tuebingerstelen": ["commemorative"],
        }

        return genre_map.get(collection, ["various"])
