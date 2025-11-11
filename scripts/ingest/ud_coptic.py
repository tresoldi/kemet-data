"""Universal Dependencies Coptic-Scriptorium ingestor."""

import logging
import re
import subprocess
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
from scripts.normalize.coptic_unicode import strip_diacritics
from scripts.utils.hashing import hash_file, hash_string


class UDCopticIngestor(BaseIngestor):
    """
    Ingestor for Universal Dependencies Coptic-Scriptorium treebank.

    Downloads CoNLL-U formatted data from GitHub and converts to our unified schema.
    Preserves all linguistic annotations including POS tags, morphology, dependencies,
    and named entities.
    """

    REPO_URL = "https://github.com/UniversalDependencies/UD_Coptic-Scriptorium.git"

    def __init__(self, config: IngestorConfig, logger: logging.Logger):
        super().__init__(config, logger)

    def list_collections(self) -> list[str]:
        """
        List available collections.

        UD Coptic has a single collection containing all splits (train/dev/test).

        Returns:
            List containing ["scriptorium"]
        """
        return ["scriptorium"]

    async def pull_collection(self, collection: str) -> Path:
        """
        Clone or update the UD Coptic repository.

        Uses git clone for initial download and git pull for updates.
        Tracks the commit hash for reproducibility.

        Args:
            collection: Collection name (should be "scriptorium")

        Returns:
            Path to the cloned repository
        """
        self.logger.info(f"Pulling UD Coptic collection: {collection}")

        raw_dir = self.get_collection_raw_dir(collection)

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
            # Clone new repository
            self.logger.info(f"Cloning UD Coptic repository to {raw_dir}")
            result = subprocess.run(
                ["git", "clone", self.REPO_URL, str(raw_dir)],
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
        Parse CoNLL-U files and convert to our unified schema.

        Processes all three splits (train, dev, test) as a single collection.
        Creates documents based on '# newdoc' markers and segments from sentences.

        Args:
            collection: Collection name
            raw_path: Path to the cloned repository

        Returns:
            CurationResult with documents, segments, and tokens
        """
        self.logger.info(f"Curating UD Coptic collection: {collection}")

        documents: list[Document] = []
        segments: list[Segment] = []
        tokens: list[Token] = []

        # Get git commit hash for provenance
        commit_hash = self._get_commit_hash(raw_path)

        # Process all CoNLL-U files
        conllu_files = sorted(raw_path.glob("*.conllu"))

        if not conllu_files:
            raise FileNotFoundError(f"No .conllu files found in {raw_path}")

        self.logger.info(f"Found {len(conllu_files)} CoNLL-U files to process")

        # Track current document
        current_doc_id = None
        current_doc_meta: dict[str, str] = {}
        doc_segments: list[Segment] = []
        doc_tokens: list[Token] = []

        for conllu_file in conllu_files:
            self.logger.info(f"Processing {conllu_file.name}")

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
                        documents.append(doc)
                        segments.extend(doc_segments)
                        tokens.extend(doc_tokens)

                    # Start new document
                    current_doc_id = item["doc_id"]
                    current_doc_meta = item["metadata"]
                    doc_segments = []
                    doc_tokens = []

                elif item["type"] == "sentence":
                    # Create segment and tokens for this sentence
                    # Normalize doc ID to match what will be used for the document
                    if current_doc_id:
                        normalized_doc_id = current_doc_id.lower().replace(":", "_").replace("-", "_")
                        normalized_doc_id = f"ud_coptic:work:{normalized_doc_id}"
                        segment, sent_tokens = self._create_segment_and_tokens(normalized_doc_id, item)
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
                documents.append(doc)
                segments.extend(doc_segments)
                tokens.extend(doc_tokens)

        # Create manifest
        manifest = self.create_manifest(collection, documents, segments, tokens)

        self.logger.info(
            f"Curated {len(documents)} documents, {len(segments)} segments, "
            f"{len(tokens)} tokens"
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

    def _parse_conllu_file(self, file_path: Path) -> list[dict[str, Any]]:
        """
        Parse a CoNLL-U file into documents and sentences.

        Args:
            file_path: Path to .conllu file

        Returns:
            List of dicts with type="newdoc" or type="sentence"
        """
        results: list[dict[str, Any]] = []
        current_doc_meta: dict[str, str] = {}
        current_sent_id: str | None = None
        current_sent_text: str | None = None
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
                                "tokens": current_sent_tokens,
                                "comments": current_sent_comments,
                            }
                        )
                        current_sent_id = None
                        current_sent_text = None
                        current_sent_tokens = []
                        current_sent_comments = {}
                    continue

                # Comment line
                if line.startswith("#"):
                    comment = line[1:].strip()

                    # New document marker
                    if comment.startswith("newdoc id = "):
                        doc_id = comment[len("newdoc id = ") :]
                        current_doc_meta = {}
                        results.append(
                            {
                                "type": "newdoc",
                                "doc_id": doc_id,
                                "metadata": current_doc_meta,
                            }
                        )

                    # Document-level metadata
                    elif comment.startswith("meta::"):
                        key_value = comment[len("meta::") :]
                        if " = " in key_value:
                            key, value = key_value.split(" = ", 1)
                            current_doc_meta[key] = value

                    # Global document comment
                    elif comment.startswith("global."):
                        key_value = comment[len("global.") :]
                        if " = " in key_value:
                            key, value = key_value.split(" = ", 1)
                            current_doc_meta[f"global_{key}"] = value

                    # Sentence ID
                    elif comment.startswith("sent_id = "):
                        current_sent_id = comment[len("sent_id = ") :]

                    # Sentence text
                    elif comment.startswith("text = "):
                        current_sent_text = comment[len("text = ") :]

                    # Other sentence-level comments
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
        Create a Document from parsed data.

        Args:
            doc_id: Original UD document ID
            metadata: Document metadata from comments
            doc_segments: List of segments in this document
            doc_tokens: List of tokens in this document
            source_file: Source CoNLL-U file
            commit_hash: Git commit hash

        Returns:
            Document object
        """
        # Normalize document ID to our format
        # "shenoute.fox:XH204-216" -> "ud_coptic:work:shenoute.fox_xh204-216"
        normalized_id = doc_id.lower().replace(":", "_").replace("-", "_")
        document_id = f"ud_coptic:work:{normalized_id}"

        # Extract metadata
        title = metadata.get("title", doc_id)
        author = metadata.get("author", "UNKNOWN")
        authors = [author] if author != "UNKNOWN" else ["UNKNOWN"]

        # Create document
        document = Document(
            document_id=document_id,
            source=self.source_name,
            collection="scriptorium",
            stage=Stage.COPTIC,
            substage=Substage.SAHIDIC,
            language="cop",
            title=title,
            authors=authors,
            date_from=None,
            date_to=None,
            genre=["treebank"],
            license="CC BY 4.0",
            provenance=Provenance(
                source_item_id=doc_id,
                retrieved_at=create_timestamp(),
                hash_raw=hash_file(source_file),
                parser_version=f"ud_coptic@{commit_hash[:7]}",
            ),
            counts=DocumentCounts(segments=len(doc_segments), tokens=len(doc_tokens)),
            metadata={
                "cts_urn": metadata.get("document_cts_urn"),
                "source_info": metadata.get("source"),
                **{k: v for k, v in metadata.items() if k not in ["title", "author"]},
            },
        )

        return document

    def _create_segment_and_tokens(
        self, document_id: str, sentence_data: dict[str, Any]
    ) -> tuple[Segment, list[Token]]:
        """
        Create a Segment and its Tokens from a parsed sentence.

        Args:
            document_id: Parent document ID
            sentence_data: Parsed sentence dict

        Returns:
            Tuple of (Segment, List[Token])
        """
        sent_id = sentence_data["sent_id"]
        sent_text = sentence_data["text"] or ""
        tokens_data = sentence_data["tokens"]

        # Get segment order from sent_id (use last number if available)
        # e.g., "shenoute.fox_204-216_478" -> 478
        order_match = re.search(r"_(\d+)$", sent_id)
        order = int(order_match.group(1)) if order_match else 0

        # Create segment
        # Use UD text as-is (already normalized by UD team)
        text_canonical = sent_text
        text_stripped = strip_diacritics(text_canonical)

        segment = Segment(
            document_id=document_id,
            segment_id=sent_id,
            order=order,
            text_canonical=text_canonical,
            text_stripped=text_stripped,
            passage_ref=None,  # UD doesn't have verse references
            dialect="SAHIDIC",
            content_hash=hash_string(text_canonical),
            created_at=create_timestamp(),
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
                lang=misc_dict.get("OrigLang"),  # Greek/Hebrew loanwords might have this
                content_hash=hash_string(token_data["form"]),
                metadata={
                    "xpos": token_data["xpos"] if token_data["xpos"] != "_" else None,
                    "head": token_data["head"] if token_data["head"] != "_" else None,
                    "deprel": token_data["deprel"] if token_data["deprel"] != "_" else None,
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
        Example: "Orig=ⲡⲣⲱⲙⲉ|Entity=(person-Jesus)|MSeg=ⲡ-ⲣⲱⲙⲉ"

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
