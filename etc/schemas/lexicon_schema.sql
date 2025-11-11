-- LEXICON DATABASE SCHEMA
-- Optimized for: Dictionary queries, frequency analysis, reconstruction, lexicography
-- Focus: Unique lemmas with pre-computed statistics and attestations

-- ==============================================================================
-- LEMMAS TABLE (Core lexical entries)
-- ==============================================================================
-- Contains unique lemmas with lexical properties and statistics

CREATE TABLE lemmas (
    lemma_id VARCHAR PRIMARY KEY,     -- Format: "cop:lemma:ⲁⲛⲟⲕ" or "egy:lemma:jnk"
    lemma VARCHAR NOT NULL,            -- Canonical form (citation form)

    -- Classification
    language VARCHAR NOT NULL,         -- cop, egy, grc
    script VARCHAR,                    -- Coptic, Hieroglyphic, Hieratic, Demotic, Greek
    period VARCHAR,                    -- For Egyptian: OLD_KINGDOM, MIDDLE_KINGDOM, etc.

    -- =========================================================================
    -- CORE LEXICAL PROPERTIES
    -- =========================================================================

    pos VARCHAR,                       -- Part of speech (NOUN, VERB, ADJ, etc.)
    pos_detail VARCHAR,                -- Detailed POS (e.g., VERB_TRANSITIVE)

    -- Glosses (translations)
    gloss_en VARCHAR,                  -- English gloss
    gloss_de VARCHAR,                  -- German gloss
    gloss_fr VARCHAR,                  -- French gloss (for future)

    -- Semantic classification
    semantic_domain VARCHAR[],         -- ["anatomy", "body_parts"] etc.
    semantic_field VARCHAR,            -- Broader category

    -- =========================================================================
    -- SCRIPT-SPECIFIC PROPERTIES
    -- =========================================================================

    -- Egyptian (Hieroglyphic/Hieratic/Demotic)
    hieroglyphic_writing VARCHAR,      -- Unicode hieroglyphs (𓇋𓈖𓎡)
    mdc_transcription VARCHAR,         -- Manuel de Codage (i-n:k)
    gardiner_codes VARCHAR[],          -- Sign list codes (["A1", "N35", "A24"])
    transliteration VARCHAR,           -- jnk, ꜥnḫ, etc.

    -- Coptic
    bohairic_form VARCHAR,             -- Bohairic variant
    sahidic_form VARCHAR,              -- Sahidic variant
    other_dialects JSON,               -- {"AKHMIMIC": "form", "LYCOPOLITAN": "form"}

    -- =========================================================================
    -- CORPUS-DERIVED STATISTICS (pre-computed from corpus)
    -- =========================================================================

    frequency INTEGER DEFAULT 0,       -- Total occurrences across all texts
    document_count INTEGER DEFAULT 0,  -- Number of documents where it appears
    collection_count INTEGER DEFAULT 0,-- Number of collections where it appears

    -- Temporal distribution
    first_attested_date INTEGER,       -- Earliest attestation (year or century)
    last_attested_date INTEGER,        -- Latest attestation
    first_attested_period VARCHAR,     -- OLD_KINGDOM, SAHIDIC, etc.
    last_attested_period VARCHAR,

    -- Geographic distribution (if available)
    attested_regions VARCHAR[],        -- Geographic attestations

    -- =========================================================================
    -- OPTION 3 READY: ETYMOLOGY & RELATIONSHIPS (placeholder fields)
    -- =========================================================================

    -- Etymology
    etymology_source_lemma_id VARCHAR, -- Parent lemma (for loans, derivations)
    etymology_type VARCHAR,            -- INHERITED, BORROWED, DERIVED, COMPOUND
    etymology_notes TEXT,              -- Free-form notes

    -- Semantic relationships (for future graph queries)
    synonyms VARCHAR[],                -- Array of lemma_ids
    antonyms VARCHAR[],                -- Array of lemma_ids
    hypernyms VARCHAR[],               -- Broader terms
    hyponyms VARCHAR[],                -- Narrower terms
    related_lemmas VARCHAR[],          -- General semantic relations

    -- Phonological information (for reconstruction)
    phonetic_form VARCHAR,             -- IPA or phonological representation
    phonological_notes TEXT,

    -- =========================================================================
    -- METADATA & PROVENANCE
    -- =========================================================================

    source VARCHAR NOT NULL,           -- TLA, AES, SCRIPTORIUM, etc.
    source_id VARCHAR,                 -- Original ID in source database
    confidence DECIMAL(3,2),           -- Quality score (0.0-1.0)

    metadata JSON,                     -- Additional flexible metadata

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    UNIQUE(lemma, language, pos)       -- Same lemma can have multiple POS entries
);

-- Indexes for fast lookups
CREATE INDEX idx_lemmas_lemma ON lemmas(lemma);
CREATE INDEX idx_lemmas_language ON lemmas(language);
CREATE INDEX idx_lemmas_script ON lemmas(script);
CREATE INDEX idx_lemmas_pos ON lemmas(pos);
CREATE INDEX idx_lemmas_frequency ON lemmas(frequency DESC);
CREATE INDEX idx_lemmas_period ON lemmas(period);

-- Composite index for language + POS filtering (common query pattern)
CREATE INDEX idx_lemmas_language_pos ON lemmas(language, pos);

-- Full-text search on glosses (optional, for future)
-- CREATE INDEX idx_lemmas_gloss_en_fts ON lemmas USING GIN(to_tsvector('english', gloss_en));


-- ==============================================================================
-- FORMS TABLE (Attested word forms / inflections)
-- ==============================================================================
-- Relates surface forms to lemmas with morphological information

CREATE TABLE forms (
    form_id VARCHAR PRIMARY KEY,       -- Unique form identifier
    lemma_id VARCHAR NOT NULL REFERENCES lemmas(lemma_id),

    -- Form information
    form VARCHAR NOT NULL,             -- Surface form as attested
    form_normalized VARCHAR,           -- Normalized orthography
    form_transliterated VARCHAR,       -- Transliteration (for Egyptian)

    -- Morphological analysis
    morphology VARCHAR,                -- Compact string: "Gender=Masc|Number=Sing"
    morphology_detailed JSON,          -- Structured: {"gender": "masc", "number": "sing"}

    -- Grammatical features
    tense VARCHAR,                     -- For verbs
    aspect VARCHAR,
    mood VARCHAR,
    voice VARCHAR,
    person VARCHAR,
    number VARCHAR,
    gender VARCHAR,
    case_marking VARCHAR,

    -- Statistics
    frequency INTEGER DEFAULT 0,       -- How often this form appears
    relative_frequency DECIMAL(5,4),   -- Within this lemma (0.0-1.0)

    -- Metadata
    metadata JSON,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(lemma_id, form, morphology)
);

CREATE INDEX idx_forms_lemma ON forms(lemma_id);
CREATE INDEX idx_forms_form ON forms(form);
CREATE INDEX idx_forms_frequency ON forms(frequency DESC);


-- ==============================================================================
-- LEMMA_ATTESTATIONS TABLE (Period/Dialect-specific statistics)
-- ==============================================================================
-- Pre-computed aggregations of lemma usage by period, dialect, collection

CREATE TABLE lemma_attestations (
    attestation_id VARCHAR PRIMARY KEY,
    lemma_id VARCHAR NOT NULL REFERENCES lemmas(lemma_id),

    -- Aggregation dimension
    dimension_type VARCHAR NOT NULL,   -- PERIOD, DIALECT, COLLECTION, GENRE
    dimension_value VARCHAR NOT NULL,  -- MIDDLE_EGYPTIAN, SAHIDIC, biblical, etc.

    -- Statistics
    frequency INTEGER DEFAULT 0,
    document_count INTEGER DEFAULT 0,
    first_occurrence DATE,
    last_occurrence DATE,

    -- Sample attestations (for quick concordance preview)
    example_segment_ids VARCHAR[],     -- Top 10 segments where it appears
    example_forms VARCHAR[],           -- Top forms in this dimension

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(lemma_id, dimension_type, dimension_value)
);

CREATE INDEX idx_attestations_lemma ON lemma_attestations(lemma_id);
CREATE INDEX idx_attestations_dimension ON lemma_attestations(dimension_type, dimension_value);
CREATE INDEX idx_attestations_frequency ON lemma_attestations(frequency DESC);


-- ==============================================================================
-- OPTION 3 READY: ETYMOLOGY TABLE (for future graph relationships)
-- ==============================================================================
-- Explicit etymology relationships (not just stored in lemmas.etymology_*)

CREATE TABLE etymology_relations (
    relation_id VARCHAR PRIMARY KEY,
    source_lemma_id VARCHAR NOT NULL REFERENCES lemmas(lemma_id),
    target_lemma_id VARCHAR NOT NULL REFERENCES lemmas(lemma_id),

    -- Relationship type
    relation_type VARCHAR NOT NULL,    -- DERIVED_FROM, BORROWED_FROM, COGNATE_WITH, EVOLVED_TO
    confidence DECIMAL(3,2),           -- Certainty (0.0-1.0)

    -- Dating
    approximate_date INTEGER,          -- When relationship occurred
    date_range_from INTEGER,
    date_range_to INTEGER,

    -- Evidence
    evidence TEXT,                     -- Description of evidence
    references VARCHAR[],              -- Scholarly citations

    -- Phonological change (for EVOLVED_TO relationships)
    phonological_change VARCHAR,       -- e.g., "jnk > ⲁⲛⲟⲕ: j > ⲁ, k > ⲕ"

    metadata JSON,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(source_lemma_id, target_lemma_id, relation_type)
);

CREATE INDEX idx_etymology_source ON etymology_relations(source_lemma_id);
CREATE INDEX idx_etymology_target ON etymology_relations(target_lemma_id);
CREATE INDEX idx_etymology_type ON etymology_relations(relation_type);


-- ==============================================================================
-- OPTION 3 READY: SEMANTIC_RELATIONS TABLE (for future knowledge graph)
-- ==============================================================================
-- Explicit semantic relationships between lemmas

CREATE TABLE semantic_relations (
    relation_id VARCHAR PRIMARY KEY,
    source_lemma_id VARCHAR NOT NULL REFERENCES lemmas(lemma_id),
    target_lemma_id VARCHAR NOT NULL REFERENCES lemmas(lemma_id),

    -- Relationship type
    relation_type VARCHAR NOT NULL,    -- SYNONYM, ANTONYM, HYPERNYM, HYPONYM, MERONYM, HOLONYM
    confidence DECIMAL(3,2),

    -- Context
    context VARCHAR,                   -- In which semantic context this holds
    notes TEXT,

    metadata JSON,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(source_lemma_id, target_lemma_id, relation_type)
);

CREATE INDEX idx_semantic_source ON semantic_relations(source_lemma_id);
CREATE INDEX idx_semantic_target ON semantic_relations(target_lemma_id);
CREATE INDEX idx_semantic_type ON semantic_relations(relation_type);


-- ==============================================================================
-- COLLOCATIONS TABLE (frequently co-occurring lemmas)
-- ==============================================================================
-- Statistical collocations (for phraseology research)

CREATE TABLE collocations (
    collocation_id VARCHAR PRIMARY KEY,
    lemma1_id VARCHAR NOT NULL REFERENCES lemmas(lemma_id),
    lemma2_id VARCHAR NOT NULL REFERENCES lemmas(lemma_id),

    -- Statistics
    frequency INTEGER DEFAULT 0,       -- How often they co-occur
    mi_score DECIMAL(10,4),            -- Mutual Information score
    t_score DECIMAL(10,4),             -- T-score
    dice_coefficient DECIMAL(5,4),     -- Dice coefficient

    -- Context
    typical_distance INTEGER,          -- Average distance in tokens
    pattern VARCHAR,                   -- Common pattern (e.g., "PREP + NOUN")

    -- Aggregation context
    period VARCHAR,                    -- Specific to period/dialect
    dialect VARCHAR,

    metadata JSON,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(lemma1_id, lemma2_id, period, dialect)
);

CREATE INDEX idx_collocations_lemma1 ON collocations(lemma1_id);
CREATE INDEX idx_collocations_lemma2 ON collocations(lemma2_id);
CREATE INDEX idx_collocations_frequency ON collocations(frequency DESC);
CREATE INDEX idx_collocations_mi_score ON collocations(mi_score DESC);


-- ==============================================================================
-- VIEWS
-- ==============================================================================

-- Comprehensive lemma view with all related data
CREATE VIEW lemmas_full AS
SELECT
    l.*,
    COUNT(DISTINCT f.form_id) as form_count,
    COUNT(DISTINCT a.dimension_value) as attestation_contexts,
    COUNT(DISTINCT er.target_lemma_id) as etymology_relations_count,
    COUNT(DISTINCT sr.target_lemma_id) as semantic_relations_count
FROM lemmas l
LEFT JOIN forms f ON l.lemma_id = f.lemma_id
LEFT JOIN lemma_attestations a ON l.lemma_id = a.lemma_id
LEFT JOIN etymology_relations er ON l.lemma_id = er.source_lemma_id
LEFT JOIN semantic_relations sr ON l.lemma_id = sr.source_lemma_id
GROUP BY l.lemma_id;


-- High-frequency lemmas (for pedagogical use)
CREATE VIEW lemmas_high_frequency AS
SELECT *
FROM lemmas
WHERE frequency >= 100
ORDER BY frequency DESC;


-- Lemmas by language/script
CREATE VIEW lemmas_coptic AS
SELECT * FROM lemmas WHERE language = 'cop';

CREATE VIEW lemmas_egyptian AS
SELECT * FROM lemmas WHERE language = 'egy';


-- ==============================================================================
-- LEXICON STATISTICS TABLE
-- ==============================================================================

CREATE TABLE lexicon_statistics (
    stat_key VARCHAR PRIMARY KEY,
    stat_value JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Example entries:
-- ('total_lemmas', '{"count": 21755}')
-- ('lemmas_by_language', '{"cop": 15000, "egy": 6500, "grc": 255}')
-- ('lemmas_with_etymology', '{"count": 1250}')
-- ('average_frequency', '{"value": 46.2}')


-- ==============================================================================
-- MATERIALIZED VIEW FOR FAST DICTIONARY ACCESS
-- ==============================================================================
-- Pre-joined view for typical dictionary queries

CREATE VIEW dictionary_entries AS
SELECT
    l.lemma_id,
    l.lemma,
    l.language,
    l.script,
    l.pos,
    l.gloss_en,
    l.gloss_de,
    l.frequency,
    l.document_count,
    l.hieroglyphic_writing,
    l.mdc_transcription,
    l.transliteration,
    l.bohairic_form,
    l.sahidic_form,
    ARRAY_AGG(DISTINCT f.form ORDER BY f.frequency DESC) as common_forms,
    ARRAY_AGG(DISTINCT a.dimension_value ORDER BY a.frequency DESC) FILTER (WHERE a.dimension_type = 'PERIOD') as attested_periods,
    l.etymology_source_lemma_id,
    l.created_at,
    l.updated_at
FROM lemmas l
LEFT JOIN forms f ON l.lemma_id = f.lemma_id
LEFT JOIN lemma_attestations a ON l.lemma_id = a.lemma_id
GROUP BY l.lemma_id;
