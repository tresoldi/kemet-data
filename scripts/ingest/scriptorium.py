"""Coptic Scriptorium corpus ingestor."""

import json
import logging
import re
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
from scripts.normalize.coptic_unicode import strip_diacritics
from scripts.utils.hashing import hash_file, hash_string


class ScriptoriumIngestor(BaseIngestor):
    """
    Ingestor for Coptic Scriptorium corpus.

    Downloads CoNLL-U formatted data from GitHub and converts to our unified schema.
    Integrates rich metadata from meta.json including CTS URNs, licenses, and
    annotation quality levels.
    """

    REPO_URL = "https://github.com/CopticScriptorium/corpora.git"

    # Licenses we exclude
    # NOTE: Academic use is now permitted for non-profit educational project
    # with community support and proper attribution
    RESTRICTED_LICENSES: ClassVar[list[str]] = [
        # Empty - we now accept all licenses including academic use
    ]

    def __init__(self, config: IngestorConfig, logger: logging.Logger):
        super().__init__(config, logger)
        self._metadata: dict[str, dict[str, Any]] = {}

    def get_collection_raw_dir(self, collection: str) -> Path:
        """
        Override to return repository root for all collections.

        Scriptorium has all collections in one repository, so we always
        return the same directory.

        Args:
            collection: Collection name (ignored)

        Returns:
            Path to the repository root
        """
        from scripts.utils.io import ensure_dir

        return ensure_dir(self.raw_dir / "scriptorium")

    def _load_metadata(self, raw_path: Path) -> dict[str, dict[str, Any]]:
        """
        Load meta.json containing metadata for all documents.

        Args:
            raw_path: Path to the repository root

        Returns:
            Dictionary mapping document IDs to metadata dicts
        """
        meta_file = raw_path / "meta.json"
        if not meta_file.exists():
            self.logger.warning(f"meta.json not found at {meta_file}")
            return {}

        self.logger.info(f"Loading metadata from {meta_file}")
        with meta_file.open("r", encoding="utf-8") as f:
            metadata: dict[str, dict[str, Any]] = json.load(f)
            return metadata

    def _is_allowed_license(self, license_str: str) -> bool:
        """
        Check if a license allows redistribution.

        Args:
            license_str: License string from metadata

        Returns:
            True if license is allowed, False if restricted
        """
        if not license_str:
            self.logger.warning("Document has no license information")
            return False

        license_lower = license_str.lower()
        for restricted in self.RESTRICTED_LICENSES:
            if restricted.lower() in license_lower:
                return False

        return True

    def list_collections(self) -> list[str]:
        """
        List available collections (corpora).

        Returns a subset based on config if specified, otherwise all corpora.

        Returns:
            List of corpus names
        """
        # Try to read from meta.json if repository is cloned
        raw_dir = self.get_collection_raw_dir("scriptorium")
        meta_file = raw_dir / "meta.json"

        if meta_file.exists():
            import json
            with meta_file.open("r", encoding="utf-8") as f:
                metadata = json.load(f)
            # Extract unique corpus names
            corpora = sorted({doc.get("corpus") for doc in metadata.values() if doc.get("corpus")})
            self.logger.info(f"Found {len(corpora)} corpora in meta.json")
            return corpora
        else:
            # Fallback to curated list if meta.json not available
            self.logger.warning("meta.json not found, returning curated list")
            return [
                "sahidic.ot",
                "sahidica.nt",
                "bohairic.ot",
                "bohairic.nt",
                "shenoute.a22",
                "shenoute.abraham",
                "shenoute.eagerness",
                "life.onnophrius",
                "apophthegmata.patrum",
                "besa.letters",
                "pistis.sophia",
            ]

    async def pull_collection(self, collection: str) -> Path:
        """
        Clone or update the Scriptorium repository.

        Note: All collections are in one repository, so we clone the entire
        repo and then filter by collection during curation.

        Args:
            collection: Collection name (corpus name)

        Returns:
            Path to the cloned repository
        """
        self.logger.info(f"Pulling Scriptorium repository for collection: {collection}")

        # Use "scriptorium" as the raw directory name (all collections in one repo)
        raw_dir = self.get_collection_raw_dir("scriptorium")

        if (raw_dir / ".git").exists():
            # Repository already exists, update it
            self.logger.info(f"Updating existing repository at {raw_dir}")
            result = subprocess.run(
                ["git", "pull"],
                cwd=raw_dir,
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0:
                self.logger.warning(
                    f"Git pull failed: {result.stderr}. Continuing with existing data."
                )
        else:
            # Clone new repository (shallow clone for speed)
            self.logger.info(f"Cloning Scriptorium repository to {raw_dir}")
            result = subprocess.run(
                ["git", "clone", "--depth", "1", self.REPO_URL, str(raw_dir)],
                capture_output=True,
                text=True,
                check=True,
            )
            self.logger.info("Repository cloned successfully")

        # Get current commit hash for provenance
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=raw_dir,
            capture_output=True,
            text=True,
            check=True,
        )
        commit_hash = result.stdout.strip()
        self.logger.info(f"Using commit: {commit_hash}")

        return raw_dir

    def curate_collection(self, collection: str, raw_path: Path) -> CurationResult:
        """
        Parse CoNLL-U files for a specific corpus and convert to our unified schema.

        Filters documents by corpus name and license restrictions.

        Args:
            collection: Corpus name to process
            raw_path: Path to the cloned repository

        Returns:
            CurationResult with documents, segments, and tokens
        """
        self.logger.info(f"Curating Scriptorium corpus: {collection}")

        # Load metadata
        self._metadata = self._load_metadata(raw_path)
        if not self._metadata:
            raise ValueError("Failed to load meta.json - cannot proceed")

        documents: list[Document] = []
        segments: list[Segment] = []
        tokens: list[Token] = []

        # Get git commit hash for provenance
        commit_hash = self._get_commit_hash(raw_path)

        # Find all CoNLL-U directories for this corpus
        # Pattern: <corpus>/*_CONLLU/*.conllu
        conllu_dirs = list(raw_path.glob("*/*_CONLLU"))
        self.logger.info(f"Found {len(conllu_dirs)} CoNLL-U directories")

        # Track current document
        current_doc_id = None
        current_normalized_doc_id = None  # NEW: Track normalized ID for segments/tokens
        current_doc_meta: dict[str, str] = {}
        doc_segments: list[Segment] = []
        doc_tokens: list[Token] = []

        # Track statistics
        skipped_license = 0
        skipped_corpus = 0

        for conllu_dir in conllu_dirs:
            # Get corpus name from directory
            # e.g., "abraham/shenoute.abraham_CONLLU" -> check if docs are in our corpus
            conllu_files = sorted(conllu_dir.glob("*.conllu"))

            for conllu_file in conllu_files:
                self.logger.debug(f"Processing {conllu_file.name}")

                # Parse the CoNLL-U file
                parsed_data = self._parse_conllu_file(conllu_file)

                for item in parsed_data:
                    if item["type"] == "newdoc":
                        # Save previous document if exists
                        if current_doc_id:
                            doc = self._create_document(
                                current_doc_id,
                                current_doc_meta,
                                doc_segments,
                                doc_tokens,
                                conllu_file,
                                commit_hash,
                            )
                            if doc:  # None if filtered out
                                documents.append(doc)
                                segments.extend(doc_segments)
                                tokens.extend(doc_tokens)

                        # Start new document
                        doc_id = item["doc_id"]

                        # Get metadata for this document
                        # Try to match doc_id to metadata keys
                        doc_meta = self._get_doc_metadata(doc_id)

                        # Check if this document belongs to our collection
                        if doc_meta.get("corpus") != collection:
                            skipped_corpus += 1
                            current_doc_id = None
                            continue

                        # Check license
                        license_str = doc_meta.get("license", "")
                        if not self._is_allowed_license(license_str):
                            self.logger.info(
                                f"Skipping {doc_id}: restricted license ({license_str})"
                            )
                            skipped_license += 1
                            current_doc_id = None
                            continue

                        current_doc_id = doc_id
                        # Create normalized ID immediately for use in segments/tokens
                        normalized_id = doc_id.lower().replace(":", "_").replace("-", "_")
                        current_normalized_doc_id = f"scriptorium:work:{normalized_id}"
                        current_doc_meta = doc_meta
                        doc_segments = []
                        doc_tokens = []

                    elif item["type"] == "sentence":
                        if current_doc_id and current_normalized_doc_id:  # Only process if we have a valid document
                            # Create segment and tokens for this sentence
                            # Use normalized document ID for consistency with Document table
                            segment, sent_tokens = self._create_segment_and_tokens(
                                current_normalized_doc_id, item, current_doc_meta
                            )
                            doc_segments.append(segment)
                            doc_tokens.extend(sent_tokens)

        # Save last document
        if current_doc_id:
            doc = self._create_document(
                current_doc_id,
                current_doc_meta,
                doc_segments,
                doc_tokens,
                conllu_file,
                commit_hash,
            )
            if doc:
                documents.append(doc)
                segments.extend(doc_segments)
                tokens.extend(doc_tokens)

        # Log statistics
        self.logger.info(
            f"Skipped {skipped_corpus} documents (wrong corpus), "
            f"{skipped_license} documents (restricted license)"
        )

        # Create manifest
        manifest = self.create_manifest(collection, documents, segments, tokens)

        self.logger.info(
            f"Curated {len(documents)} documents, {len(segments)} segments, "
            f"{len(tokens)} tokens for {collection}"
        )

        return CurationResult(
            documents=documents, segments=segments, tokens=tokens, manifest=manifest
        )

    def _get_commit_hash(self, repo_path: Path) -> str:
        """Get the current git commit hash."""
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def _get_doc_metadata(self, doc_id: str) -> dict[str, str]:
        """
        Get metadata for a document from meta.json.

        Handles ID format variations between CoNLL-U and meta.json.

        Args:
            doc_id: Document ID from CoNLL-U (e.g., "shenoute.abraham:XL93-94" or "besa.letters:exhortations")

        Returns:
            Metadata dict, or empty dict if not found
        """
        # Try exact match first
        if doc_id in self._metadata:
            return self._metadata[doc_id]

        # Try without corpus prefix (after colon)
        # "besa.letters:exhortations" → "exhortations"
        if ":" in doc_id:
            after_colon = doc_id.split(":", 1)[1]
            if after_colon in self._metadata:
                return self._metadata[after_colon]

        # Try normalizing the ID
        # Replace colons with underscores, etc.
        normalized = doc_id.replace(":", "_").replace("-", "_")
        if normalized in self._metadata:
            return self._metadata[normalized]

        # Try extracting base name (before colon) and find matches
        if ":" in doc_id:
            before_colon = doc_id.split(":")[0]
            after_colon = doc_id.split(":", 1)[1]

            # Try matching with the part after colon
            matches = [k for k in self._metadata if after_colon in k]
            if len(matches) == 1:
                return self._metadata[matches[0]]
            elif len(matches) > 1:
                self.logger.debug(f"Multiple metadata matches for {doc_id}: {matches[:5]}...")
                # Filter by corpus if possible
                corpus_matches = [
                    k
                    for k in matches
                    if self._metadata[k].get("corpus", "").startswith(before_colon)
                ]
                if corpus_matches:
                    return self._metadata[corpus_matches[0]]
                return self._metadata[matches[0]]

        self.logger.debug(f"No metadata found for document ID: {doc_id}")
        return {}

    def _parse_conllu_file(self, file_path: Path) -> list[dict[str, Any]]:
        """
        Parse a CoNLL-U file into documents and sentences.

        Same format as UD Coptic.

        Args:
            file_path: Path to .conllu file

        Returns:
            List of dicts with type="newdoc" or type="sentence"
        """
        results: list[dict[str, Any]] = []
        current_sent_id: str | None = None
        current_sent_text: str | None = None
        current_sent_text_en: str | None = None
        current_sent_tokens: list[dict[str, Any]] = []
        current_sent_comments: dict[str, str] = {}

        with file_path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line = line.rstrip("\n\r")

                # Empty line = end of sentence
                if not line:
                    if current_sent_id and current_sent_tokens:
                        results.append(
                            {
                                "type": "sentence",
                                "sent_id": current_sent_id,
                                "text": current_sent_text,
                                "text_en": current_sent_text_en,
                                "tokens": current_sent_tokens,
                                "comments": current_sent_comments,
                            }
                        )
                        current_sent_id = None
                        current_sent_text = None
                        current_sent_text_en = None
                        current_sent_tokens = []
                        current_sent_comments = {}
                    continue

                # Comment line
                if line.startswith("#"):
                    comment = line[1:].strip()

                    # New document marker
                    if comment.startswith("newdoc id = "):
                        doc_id = comment[len("newdoc id = ") :]
                        results.append(
                            {
                                "type": "newdoc",
                                "doc_id": doc_id,
                                "metadata": {},
                            }
                        )

                    # Sentence ID
                    elif comment.startswith("sent_id = "):
                        current_sent_id = comment[len("sent_id = ") :]

                    # Sentence text (Coptic)
                    elif comment.startswith("text = "):
                        current_sent_text = comment[len("text = ") :]

                    # Sentence text (English translation)
                    elif comment.startswith("text_en = "):
                        current_sent_text_en = comment[len("text_en = ") :]

                    # Other comments
                    elif " = " in comment:
                        key, value = comment.split(" = ", 1)
                        current_sent_comments[key] = value

                    continue

                # Token line
                fields = line.split("\t")
                if len(fields) != 10:
                    self.logger.warning(
                        f"Invalid CoNLL-U line at {file_path.name}:{line_no}: "
                        f"expected 10 fields, got {len(fields)}"
                    )
                    continue

                token_id = fields[0]

                # Skip multiword tokens (ranges like "1-2")
                if "-" in token_id:
                    continue

                # Skip empty nodes (decimals like "5.1")
                if "." in token_id:
                    continue

                # Parse token
                token_data = {
                    "id": token_id,
                    "form": fields[1],
                    "lemma": fields[2],
                    "upos": fields[3],
                    "xpos": fields[4],
                    "feats": fields[5],
                    "head": fields[6],
                    "deprel": fields[7],
                    "deps": fields[8],
                    "misc": fields[9],
                }

                current_sent_tokens.append(token_data)

        # Handle last sentence if file doesn't end with blank line
        if current_sent_id and current_sent_tokens:
            results.append(
                {
                    "type": "sentence",
                    "sent_id": current_sent_id,
                    "text": current_sent_text,
                    "text_en": current_sent_text_en,
                    "tokens": current_sent_tokens,
                    "comments": current_sent_comments,
                }
            )

        return results

    def _create_document(
        self,
        doc_id: str,
        metadata: dict[str, str],
        doc_segments: list[Segment],
        doc_tokens: list[Token],
        source_file: Path,
        commit_hash: str,
    ) -> Document:
        """
        Create a Document from parsed data and metadata.

        Args:
            doc_id: Original Scriptorium document ID
            metadata: Document metadata from meta.json
            doc_segments: List of segments in this document
            doc_tokens: List of tokens in this document
            source_file: Source CoNLL-U file
            commit_hash: Git commit hash

        Returns:
            Document object, or None if document should be filtered
        """
        # Normalize document ID to our format
        normalized_id = doc_id.lower().replace(":", "_").replace("-", "_")
        document_id = f"scriptorium:work:{normalized_id}"

        # Extract metadata
        title = metadata.get("title", doc_id)
        # Try to extract author from various fields
        author = metadata.get("author") or "UNKNOWN"
        if "shenoute" in doc_id.lower():
            author = "Shenoute"

        authors = [author] if author != "UNKNOWN" else ["UNKNOWN"]

        # Determine dialect from corpus name or metadata
        corpus = metadata.get("corpus", "")
        if "sahidic" in corpus:
            substage = Substage.SAHIDIC
        elif "bohairic" in corpus:
            substage = Substage.BOHAIRIC
        else:
            substage = Substage.SAHIDIC  # Default

        # Extract genre from corpus type
        genre = []
        if any(x in corpus for x in ["ot", "nt", "mark", "corinthians", "ruth"]):
            genre.append("biblical")
        if "shenoute" in corpus:
            genre.append("monastic")
        if "life" in corpus or "martyrdom" in corpus:
            genre.append("hagiographic")
        if "apophthegmata" in corpus:
            genre.append("apophthegmata")
        if not genre:
            genre.append("other")

        # Create document
        document = Document(
            document_id=document_id,
            source=self.source_name,
            collection=corpus,
            stage=Stage.COPTIC,
            substage=substage,
            language="cop",
            title=title,
            authors=authors,
            date_from=None,
            date_to=None,
            genre=genre,
            license=metadata.get("license", "UNKNOWN"),
            provenance=Provenance(
                source_item_id=doc_id,
                retrieved_at=create_timestamp(),
                hash_raw=hash_file(source_file),
                parser_version=f"scriptorium@{commit_hash[:7]}",
            ),
            counts=DocumentCounts(segments=len(doc_segments), tokens=len(doc_tokens)),
            metadata={
                "cts_urn": metadata.get("document_cts_urn"),
                "annotation_quality": {
                    "segmentation": metadata.get("segmentation"),
                    "tagging": metadata.get("tagging"),
                    "parsing": metadata.get("parsing"),
                    "entities": metadata.get("entities"),
                    "identities": metadata.get("identities"),
                },
                "project": metadata.get("project"),
                "source_info": metadata.get("source"),
                "version": metadata.get("version_n"),
                **{
                    k: v
                    for k, v in metadata.items()
                    if k
                    not in [
                        "title",
                        "author",
                        "license",
                        "corpus",
                        "document_cts_urn",
                    ]
                },
            },
        )

        return document

    def _create_segment_and_tokens(
        self, document_id: str, sentence_data: dict[str, Any], doc_meta: dict[str, Any]
    ) -> tuple[Segment, list[Token]]:
        """
        Create a Segment and its Tokens from a parsed sentence.

        Args:
            document_id: Parent document ID
            sentence_data: Parsed sentence dict
            doc_meta: Document metadata

        Returns:
            Tuple of (Segment, List[Token])
        """
        sent_id = sentence_data["sent_id"]
        sent_text = sentence_data["text"] or ""
        sent_text_en = sentence_data.get("text_en")
        tokens_data = sentence_data["tokens"]

        # Get segment order from sent_id (use last number if available)
        order_match = re.search(r"_s(\d+)$", sent_id)
        order = int(order_match.group(1)) if order_match else 0

        # Create segment
        text_canonical = sent_text
        text_stripped = strip_diacritics(text_canonical)

        # Determine dialect
        corpus = doc_meta.get("corpus", "")
        if "sahidic" in corpus:
            dialect = "SAHIDIC"
        elif "bohairic" in corpus:
            dialect = "BOHAIRIC"
        else:
            dialect = "SAHIDIC"

        segment = Segment(
            document_id=document_id,
            segment_id=sent_id,
            order=order,
            text_canonical=text_canonical,
            text_stripped=text_stripped,
            passage_ref=None,
            dialect=dialect,
            content_hash=hash_string(text_canonical),
            created_at=create_timestamp(),
            metadata={"text_en": sent_text_en} if sent_text_en else {},
        )

        # Create tokens
        tokens = []
        for token_data in tokens_data:
            token_id = f"t{int(token_data['id']):06d}"

            # Parse MISC field (pipe-separated key=value pairs)
            misc_dict = self._parse_misc_field(token_data["misc"])

            # Create token
            token = Token(
                document_id=document_id,
                segment_id=sent_id,
                token_id=token_id,
                order=int(token_data["id"]) - 1,  # 0-indexed
                form=token_data["form"],
                form_norm=misc_dict.get("Orig"),  # Original orthography
                lemma=token_data["lemma"] if token_data["lemma"] != "_" else None,
                pos=token_data["upos"] if token_data["upos"] != "_" else None,
                morph=token_data["feats"] if token_data["feats"] != "_" else None,
                lang=misc_dict.get("OrigLang"),  # Language of loanword
                content_hash=hash_string(token_data["form"]),
                metadata={
                    "xpos": token_data["xpos"] if token_data["xpos"] != "_" else None,
                    "head": token_data["head"] if token_data["head"] != "_" else None,
                    "deprel": (token_data["deprel"] if token_data["deprel"] != "_" else None),
                    "deps": token_data["deps"] if token_data["deps"] != "_" else None,
                    "misc": misc_dict,  # Full MISC field as dict
                },
            )
            tokens.append(token)

        return segment, tokens

    def _parse_misc_field(self, misc_str: str) -> dict[str, str]:
        """
        Parse the MISC field from CoNLL-U.

        MISC contains pipe-separated key=value pairs.

        Args:
            misc_str: MISC field string

        Returns:
            Dict of key-value pairs
        """
        if misc_str == "_":
            return {}

        misc_dict = {}
        for pair in misc_str.split("|"):
            if "=" in pair:
                key, value = pair.split("=", 1)
                misc_dict[key] = value

        return misc_dict
