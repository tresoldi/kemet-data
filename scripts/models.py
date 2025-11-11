"""Data models for KEMET corpus."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class Stage(str, Enum):
    """Language stage."""

    COPTIC = "COPTIC"
    EGYPTIAN = "EGYPTIAN"


class Substage(str, Enum):
    """Language substage or dialect."""

    # Coptic dialects
    SAHIDIC = "SAHIDIC"
    BOHAIRIC = "BOHAIRIC"
    FAYYUMIC = "FAYYUMIC"
    AKHMIMIC = "AKHMIMIC"
    LYCOPOLITAN = "LYCOPOLITAN"

    # Egyptian stages
    OLD_EGYPTIAN = "OLD_EGYPTIAN"
    MIDDLE_EGYPTIAN = "MIDDLE_EGYPTIAN"
    LATE_EGYPTIAN = "LATE_EGYPTIAN"
    DEMOTIC = "DEMOTIC"

    UNKNOWN = "UNKNOWN"


@dataclass
class Provenance:
    """Document provenance metadata."""

    source_item_id: str
    retrieved_at: str
    hash_raw: str
    parser_version: str


@dataclass
class DocumentCounts:
    """Document statistics."""

    segments: int = 0
    tokens: int = 0


@dataclass
class Document:
    """A document (work or logical unit) in the corpus."""

    document_id: str
    source: str
    collection: str
    stage: Stage
    substage: Substage
    language: str
    provenance: Provenance
    title: str | None = None
    authors: list[str] = field(default_factory=list)
    date_from: int | None = None
    date_to: int | None = None
    genre: list[str] = field(default_factory=list)
    license: str = "SEE_SOURCE"
    counts: DocumentCounts = field(default_factory=DocumentCounts)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "document_id": self.document_id,
            "source": self.source,
            "collection": self.collection,
            "stage": self.stage.value,
            "substage": self.substage.value,
            "language": self.language,
            "title": self.title,
            "authors": self.authors,
            "date_from": self.date_from,
            "date_to": self.date_to,
            "genre": self.genre,
            "license": self.license,
            "provenance": {
                "source_item_id": self.provenance.source_item_id,
                "retrieved_at": self.provenance.retrieved_at,
                "hash_raw": self.provenance.hash_raw,
                "parser_version": self.provenance.parser_version,
            },
            "counts": {
                "segments": self.counts.segments,
                "tokens": self.counts.tokens,
            },
            "metadata": self.metadata,
        }


@dataclass
class Segment:
    """A text segment (sentence, verse, or logical unit)."""

    document_id: str
    segment_id: str
    order: int
    text_canonical: str
    content_hash: str
    created_at: str
    text_stripped: str | None = None
    text_hieroglyphs: str | None = None  # Unicode hieroglyphs (Egyptian only)
    text_en: str | None = None  # English translation
    text_de: str | None = None  # German translation
    translation_language: str | None = None  # Translation language code
    passage_ref: str | None = None
    dialect: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for DataFrame."""
        return {
            "document_id": self.document_id,
            "segment_id": self.segment_id,
            "order": self.order,
            "text_canonical": self.text_canonical,
            "text_stripped": self.text_stripped,
            "text_hieroglyphs": self.text_hieroglyphs,
            "text_en": self.text_en,
            "text_de": self.text_de,
            "translation_language": self.translation_language,
            "passage_ref": self.passage_ref,
            "dialect": self.dialect,
            "metadata": self.metadata,
            "content_hash": self.content_hash,
            "created_at": self.created_at,
        }


@dataclass
class Token:
    """A linguistic token with annotations."""

    document_id: str
    segment_id: str
    token_id: str
    order: int
    form: str
    content_hash: str
    form_norm: str | None = None
    lemma: str | None = None
    pos: str | None = None
    morph: str | None = None
    lang: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for DataFrame."""
        return {
            "document_id": self.document_id,
            "segment_id": self.segment_id,
            "token_id": self.token_id,
            "order": self.order,
            "form": self.form,
            "form_norm": self.form_norm,
            "lemma": self.lemma,
            "pos": self.pos,
            "morph": self.morph,
            "lang": self.lang,
            "metadata": self.metadata,
            "content_hash": self.content_hash,
        }


@dataclass
class Alignment:
    """Segment-level alignment between sources."""

    left_source: str
    right_source: str
    left_document_id: str
    right_document_id: str
    left_segment_id: str
    right_segment_id: str
    score: float
    method: str
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for DataFrame."""
        return {
            "left_source": self.left_source,
            "right_source": self.right_source,
            "left_document_id": self.left_document_id,
            "right_document_id": self.right_document_id,
            "left_segment_id": self.left_segment_id,
            "right_segment_id": self.right_segment_id,
            "score": self.score,
            "method": self.method,
            "metadata": self.metadata,
        }


@dataclass
class Artifact:
    """An artifact in a collection manifest."""

    path: str
    type: str
    hash: str
    size_bytes: int
    row_count: int | None = None


@dataclass
class CollectionCounts:
    """Collection statistics."""

    documents: int = 0
    segments: int = 0
    tokens: int = 0


@dataclass
class Manifest:
    """Collection manifest with artifacts and checksums."""

    source: str
    collection: str
    version: int
    created_at: str
    artifacts: list[Artifact]
    parser_version: str | None = None
    counts: CollectionCounts = field(default_factory=CollectionCounts)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "source": self.source,
            "collection": self.collection,
            "version": self.version,
            "created_at": self.created_at,
            "parser_version": self.parser_version,
            "artifacts": [
                {
                    "path": a.path,
                    "type": a.type,
                    "hash": a.hash,
                    "size_bytes": a.size_bytes,
                    "row_count": a.row_count,
                }
                for a in self.artifacts
            ],
            "counts": {
                "documents": self.counts.documents,
                "segments": self.counts.segments,
                "tokens": self.counts.tokens,
            },
            "metadata": self.metadata,
        }


def create_timestamp() -> str:
    """Create ISO 8601 timestamp."""
    return datetime.utcnow().isoformat() + "Z"
