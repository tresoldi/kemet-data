-- CORPUS DATABASE SCHEMA
-- Optimized for: NLP, language model training, concordance, contextual analysis
-- Focus: Sequential text reading, contextualized token instances

-- ==============================================================================
-- DOCUMENTS TABLE
-- ==============================================================================
-- Contains metadata about source texts
-- Same as unified schema (no changes needed)

CREATE TABLE documents (
    document_id VARCHAR PRIMARY KEY,
    source VARCHAR NOT NULL,
    collection VARCHAR NOT NULL,

    -- Classification
    stage VARCHAR NOT NULL,           -- COPTIC, EGYPTIAN
    substage VARCHAR,                 -- SAHIDIC, BOHAIRIC, MIDDLE_EGYPTIAN, etc.
    script VARCHAR,                   -- Coptic, Hieroglyphic, Hieratic, Demotic
    language VARCHAR NOT NULL,        -- cop, egy, grc
    genre VARCHAR[],                  -- biblical, literary, funerary, etc.

    -- Dating
    date_from INTEGER,
    date_to INTEGER,
    century INTEGER,

    -- Metadata
    title VARCHAR NOT NULL,
    authors VARCHAR[],
    license VARCHAR,

    -- Counts
    num_segments INTEGER NOT NULL DEFAULT 0,
    num_tokens INTEGER NOT NULL DEFAULT 0,

    -- Additional data
    metadata JSON,
    provenance JSON,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_documents_source ON documents(source);
CREATE INDEX idx_documents_collection ON documents(collection);
CREATE INDEX idx_documents_stage ON documents(stage);
CREATE INDEX idx_documents_substage ON documents(substage);
CREATE INDEX idx_documents_language ON documents(language);


-- ==============================================================================
-- SEGMENTS TABLE
-- ==============================================================================
-- Contains text segments (sentences, verses, paragraphs)
-- Same as unified schema (no changes needed)

CREATE TABLE segments (
    segment_id VARCHAR PRIMARY KEY,
    document_id VARCHAR NOT NULL REFERENCES documents(document_id),
    "order" INTEGER NOT NULL,

    -- Text variants
    text_canonical VARCHAR NOT NULL,  -- Primary text representation
    text_stripped VARCHAR,            -- Without diacritics/supralinears
    text_display VARCHAR,             -- For display (with formatting)
    text_hieroglyphs VARCHAR,         -- Unicode hieroglyphs (Egyptian only)

    -- Translations
    text_en VARCHAR,                  -- English translation
    text_de VARCHAR,                  -- German translation
    translation_language VARCHAR,     -- Language code for translation

    -- Metadata
    dialect VARCHAR,                  -- SAHIDIC, BOHAIRIC, etc.
    script VARCHAR,                   -- Script variant
    genre VARCHAR[],                  -- Genre tags
    passage_ref VARCHAR,              -- Biblical/textual reference
    metadata JSON,                    -- Additional metadata

    -- Content hash for deduplication
    content_hash VARCHAR,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_segments_document ON segments(document_id, "order");
CREATE INDEX idx_segments_dialect ON segments(dialect);
CREATE INDEX idx_segments_content_hash ON segments(content_hash);


-- ==============================================================================
-- TOKEN_INSTANCES TABLE
-- ==============================================================================
-- Contains individual token occurrences in context
-- MODIFIED: Removes lexical data, adds lemma_id reference

CREATE TABLE token_instances (
    token_id VARCHAR PRIMARY KEY,
    segment_id VARCHAR NOT NULL REFERENCES segments(segment_id),
    document_id VARCHAR NOT NULL REFERENCES documents(document_id),
    "order" INTEGER NOT NULL,

    -- =========================================================================
    -- CONTEXTUAL DATA (instance-specific, cannot be shared)
    -- =========================================================================

    form VARCHAR NOT NULL,            -- Word form as it appears in text

    -- Syntax (dependency parsing)
    head INTEGER,                     -- Head token index in sentence
    deprel VARCHAR,                   -- Dependency relation

    -- =========================================================================
    -- LEXICAL REFERENCE (points to lexicon database)
    -- =========================================================================

    lemma_id VARCHAR,                 -- FK to lexicon.lemmas (cross-DB reference)
                                      -- Format: "cop:lemma:ⲁⲛⲟⲕ" or "egy:lemma:jnk"

    -- =========================================================================
    -- MINIMAL METADATA (for filtering without lexicon lookup)
    -- =========================================================================

    lang VARCHAR,                     -- cop, egy, grc (for fast filtering)

    -- =========================================================================
    -- EXTENDED METADATA (stored as JSON for flexibility)
    -- =========================================================================

    metadata JSON,                    -- Additional contextual data
                                      -- e.g., cotext_translation, line_count, etc.

    -- Content hash
    content_hash VARCHAR
);

-- Optimized for sequential reading (corpus analysis, LM training)
CREATE INDEX idx_tokens_segment ON token_instances(segment_id, "order");
CREATE INDEX idx_tokens_document ON token_instances(document_id, "order");

-- Optimized for lemma lookup (concordance)
CREATE INDEX idx_tokens_lemma ON token_instances(lemma_id);

-- Optimized for language filtering
CREATE INDEX idx_tokens_lang ON token_instances(lang);

-- Optimized for concordance queries (KWIC with ORDER BY)
CREATE INDEX idx_tokens_lemma_doc_seg_order ON token_instances(lemma_id, document_id, segment_id, "order");


-- ==============================================================================
-- VIEWS
-- ==============================================================================

-- Enriched segments view (joins segments with document metadata)
CREATE VIEW segments_enriched AS
SELECT
    s.*,
    d.source,
    d.collection,
    d.stage,
    d.substage,
    d.script AS document_script,
    d.language,
    d.genre AS document_genre,
    d.title,
    d.authors
FROM segments s
JOIN documents d ON s.document_id = d.document_id;


-- Token instances with segment context (for KWIC/concordance)
CREATE VIEW tokens_with_context AS
SELECT
    t.token_id,
    t.form,
    t.lemma_id,
    t.order AS token_order,
    t.lang,
    s.segment_id,
    s.text_canonical AS segment_text,
    s.passage_ref,
    s.dialect,
    d.document_id,
    d.title,
    d.collection,
    d.substage,
    d.stage
FROM token_instances t
JOIN segments s ON t.segment_id = s.segment_id
JOIN documents d ON t.document_id = d.document_id;


-- ==============================================================================
-- STATISTICS TABLES (for quick queries)
-- ==============================================================================

CREATE TABLE corpus_statistics (
    stat_key VARCHAR PRIMARY KEY,
    stat_value JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Example entries:
-- ('total_documents', '{"count": 1549}')
-- ('total_segments', '{"count": 181563}')
-- ('total_tokens', '{"count": 1027480}')
-- ('tokens_by_language', '{"cop": 0, "egy": 1027044, "grc": 18}')

-- Pre-computed collection statistics (100x faster than GROUP BY queries)
CREATE TABLE collection_statistics (
    collection VARCHAR PRIMARY KEY,
    document_count INTEGER NOT NULL,
    segment_count INTEGER NOT NULL,
    token_count INTEGER NOT NULL,
    unique_lemma_count INTEGER NOT NULL,
    language_distribution JSON,  -- {"cop": 1234, "egy": 5678, "grc": 90}
    pos_distribution JSON,        -- {"NOUN": 1000, "VERB": 800, ...}
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
