"""SQL schema definitions for KEMET unified database.

Extended schema supporting both Coptic and Egyptian texts.
"""

CREATE_DOCUMENTS_TABLE = """
CREATE TABLE documents (
    -- Primary key
    document_id TEXT PRIMARY KEY,

    -- Source tracking
    source TEXT NOT NULL,
    collection TEXT NOT NULL,

    -- Content classification
    stage TEXT,                     -- 'COPTIC', 'EGYPTIAN'
    substage TEXT,                  -- 'BOHAIRIC', 'SAHIDIC', 'OLD_EGYPTIAN', 'MIDDLE_EGYPTIAN', 'LATE_EGYPTIAN', 'DEMOTIC'
    script TEXT,                    -- 'COPTIC', 'HIEROGLYPHIC', 'HIERATIC', 'DEMOTIC'
    language TEXT,                  -- ISO 639-3: 'cop' (Coptic), 'egy' (Egyptian)
    genre TEXT[],                   -- Array of genres

    -- Temporal metadata
    date_from INTEGER,              -- Earliest date (year, negative for BCE)
    date_to INTEGER,                -- Latest date (year, negative for BCE)
    century INTEGER,                -- Computed: FLOOR(date_from/100)

    -- Descriptive metadata
    title TEXT,
    authors TEXT[],                 -- Array of authors
    license TEXT,

    -- Counts
    num_segments INTEGER,
    num_tokens INTEGER,

    -- Full metadata (JSON)
    metadata JSON,                  -- All source-specific metadata
    provenance JSON,                -- Source provenance info

    -- Timestamps
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""

CREATE_SEGMENTS_TABLE = """
CREATE TABLE segments (
    -- Primary key
    segment_id TEXT PRIMARY KEY,

    -- Foreign keys
    document_id TEXT NOT NULL,

    -- Ordering
    "order" INTEGER NOT NULL,

    -- Text content (original script)
    text_canonical TEXT NOT NULL,   -- Normalized text (Coptic or Egyptian transliteration)
    text_stripped TEXT,             -- Without punctuation
    text_display TEXT,              -- Display version
    text_hieroglyphs TEXT,          -- Hieroglyphic Unicode (for Egyptian texts)

    -- Translations (multiple languages supported)
    text_en TEXT,                   -- English translation
    text_de TEXT,                   -- German translation
    translation_language TEXT,      -- Primary translation language ('en', 'de', etc.)

    -- Classification (denormalized from document)
    dialect TEXT,                   -- BOHAIRIC, SAHIDIC, OLD_EGYPTIAN, etc.
    script TEXT,                    -- COPTIC, HIEROGLYPHIC, HIERATIC, DEMOTIC
    genre TEXT[],                   -- Inherited from document

    -- Reference
    passage_ref TEXT,               -- Biblical reference, chapter, TLA reference, etc.

    -- Metadata
    metadata JSON,

    -- Hashing
    content_hash TEXT,              -- Blake3 hash of content

    -- Timestamps
    created_at TIMESTAMP
)
"""

CREATE_TOKENS_TABLE = """
CREATE TABLE tokens (
    -- Primary key
    token_id TEXT PRIMARY KEY,

    -- Foreign keys
    segment_id TEXT NOT NULL,
    document_id TEXT NOT NULL,

    -- Ordering
    "order" INTEGER NOT NULL,

    -- Surface form
    form TEXT NOT NULL,             -- Original token (Coptic or hieroglyphic)
    form_norm TEXT,                 -- Normalized form
    form_transliterated TEXT,       -- Transliteration (for hieroglyphic texts)

    -- Linguistic annotations
    lemma TEXT,                     -- Dictionary form
    lemma_id TEXT,                  -- TLA lemma ID (for Egyptian texts)
    pos TEXT,                       -- Universal POS tag
    morph TEXT,                     -- Morphological features
    lang TEXT,                      -- Language (cop, egy, grc for Greek loans)

    -- Syntax
    head INTEGER,                   -- Dependency head
    deprel TEXT,                    -- Dependency relation

    -- Extended metadata
    xpos TEXT,                      -- Language-specific POS tag
    glossing TEXT,                  -- Inflectional glossing (TLA)
    metadata JSON,                  -- misc, entity, etc.

    -- Hashing
    content_hash TEXT
)
"""

CREATE_SEGMENTS_ENRICHED_VIEW = """
CREATE VIEW segments_enriched AS
SELECT
    s.segment_id,
    s.document_id,
    s."order",
    s.text_canonical,
    s.text_hieroglyphs,
    s.text_en,
    s.text_de,
    s.translation_language,
    s.dialect,
    s.script AS segment_script,
    s.passage_ref,
    -- Document metadata
    d.source,
    d.collection,
    d.stage,
    d.substage,
    d.script AS document_script,
    d.language,
    d.genre,
    d.century,
    d.date_from,
    d.date_to,
    d.title,
    d.authors
FROM segments s
JOIN documents d ON s.document_id = d.document_id
"""

CREATE_TOKENS_ENRICHED_VIEW = """
CREATE VIEW tokens_enriched AS
SELECT
    t.token_id,
    t.segment_id,
    t.document_id,
    t."order",
    t.form,
    t.form_transliterated,
    t.lemma,
    t.lemma_id,
    t.pos,
    t.morph,
    t.lang,
    t.glossing,
    -- Segment context
    s.text_canonical AS segment_text,
    s.dialect,
    s.script AS segment_script,
    -- Document metadata
    d.source,
    d.collection,
    d.stage,
    d.substage,
    d.script AS document_script,
    d.language,
    d.genre,
    d.century
FROM tokens t
JOIN segments s ON t.segment_id = s.segment_id
JOIN documents d ON t.document_id = d.document_id
"""

CREATE_INDEXES = [
    "CREATE INDEX idx_documents_source ON documents(source)",
    "CREATE INDEX idx_documents_collection ON documents(collection)",
    "CREATE INDEX idx_documents_stage ON documents(stage)",
    "CREATE INDEX idx_documents_substage ON documents(substage)",
    "CREATE INDEX idx_documents_script ON documents(script)",
    "CREATE INDEX idx_documents_language ON documents(language)",
    "CREATE INDEX idx_documents_century ON documents(century)",
    "CREATE INDEX idx_segments_document ON segments(document_id)",
    "CREATE INDEX idx_segments_dialect ON segments(dialect)",
    "CREATE INDEX idx_segments_script ON segments(script)",
    "CREATE INDEX idx_tokens_segment ON tokens(segment_id)",
    "CREATE INDEX idx_tokens_document ON tokens(document_id)",
    "CREATE INDEX idx_tokens_pos ON tokens(pos)",
    "CREATE INDEX idx_tokens_lemma ON tokens(lemma)",
    "CREATE INDEX idx_tokens_lang ON tokens(lang)",
]

CREATE_STATS_TABLE = """
CREATE TABLE corpus_stats (
    key TEXT PRIMARY KEY,
    value INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
