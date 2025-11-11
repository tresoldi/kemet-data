# KEMET Data: Database Documentation

**Version 0.1.0**

This document provides comprehensive technical documentation for the KEMET Data dual database architecture, including detailed schema descriptions, query examples, and usage patterns.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Corpus Database (`corpus.duckdb`)](#corpus-database-corpusduckdb)
3. [Lexicon Database (`lexicon.duckdb`)](#lexicon-database-lexiconduckdb)
4. [Query Examples](#query-examples)
5. [Performance Tips](#performance-tips)
6. [Cross-Database Queries](#cross-database-queries)

---

## Architecture Overview

KEMET Data uses a **dual database architecture** that separates concerns between corpus-level analysis and lexicon-level research:

```
┌─────────────────────────────────────────────────────────────┐
│                    KEMET Data Architecture                  │
├─────────────────────────┬───────────────────────────────────┤
│   corpus.duckdb (642MB) │   lexicon.duckdb (83MB)          │
├─────────────────────────┼───────────────────────────────────┤
│ • Documents (1,552)     │ • Lemmas (33,259)                │
│ • Segments (252,826)    │ • Forms (104,659)                │
│ • Tokens (1,558,260)    │ • Attestations (103,908)         │
│                         │ • Etymology Relations (1,750)    │
│                         │ • CDO Mappings (9,493)           │
├─────────────────────────┼───────────────────────────────────┤
│ Use Cases:              │ Use Cases:                       │
│ • Language modeling     │ • Dictionary lookups             │
│ • NLP training          │ • Frequency analysis             │
│ • Concordance (KWIC)    │ • Lexicography                   │
│ • Text extraction       │ • Form-to-lemma mapping          │
│ • Syntactic analysis    │ • Etymology research             │
└─────────────────────────┴───────────────────────────────────┘
```

### Design Principles

1. **Separation of Concerns**: Corpus data (tokens in context) vs. lexical data (unique lemmas)
2. **Optimized Indexing**: Different index strategies for different query patterns
3. **No Duplication**: Lexical properties stored once in lexicon, referenced by `lemma_id`
4. **Cross-Database Joins**: DuckDB supports `ATTACH` for cross-database queries
5. **Pre-Computed Statistics**: Both databases include statistics tables for fast aggregations

---

## Corpus Database (`corpus.duckdb`)

**Purpose**: Sequential text analysis, language modeling, concordance, dependency parsing

**Size**: 635 MB

### Schema

#### 1. `documents` Table

Document-level metadata for all texts in the corpus.

| Column | Type | Description |
|--------|------|-------------|
| `document_id` | VARCHAR (PK) | Unique document identifier |
| `source` | VARCHAR | Data source (ramses, scriptorium, aes, tla, ud_coptic, horner) |
| `collection` | VARCHAR | Collection name (e.g., sahidic.nt, martyrdoms) |
| `stage` | VARCHAR | Language stage (COPTIC, EGYPTIAN) |
| `substage` | VARCHAR | Dialect/period (SAHIDIC, BOHAIRIC, MIDDLE_EGYPTIAN, etc.) |
| `script` | VARCHAR | Writing system (Coptic, Hieroglyphic, Hieratic, Demotic) |
| `language` | VARCHAR | ISO 639-3 code (cop, egy, grc) |
| `genre` | VARCHAR[] | Genre tags (biblical, literary, funerary, etc.) |
| `date_from` | INTEGER | Start date (negative for BCE) |
| `date_to` | INTEGER | End date (negative for BCE) |
| `century` | INTEGER | Century (negative for BCE) |
| `title` | VARCHAR | Document title |
| `authors` | VARCHAR[] | Author names |
| `license` | VARCHAR | License information |
| `num_segments` | INTEGER | Number of segments in document |
| `num_tokens` | INTEGER | Number of tokens in document |
| `metadata` | JSON | Additional metadata |
| `provenance` | JSON | Source attribution |
| `created_at` | TIMESTAMP | Creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |

**Indexes**:
- `idx_documents_source` on `source`
- `idx_documents_collection` on `collection`
- `idx_documents_stage` on `stage`
- `idx_documents_substage` on `substage`
- `idx_documents_language` on `language`

**Example Rows**:

```sql
SELECT document_id, source, collection, substage, title, num_tokens
FROM documents LIMIT 3;
```

| document_id | source | collection | substage | title | num_tokens |
|-------------|--------|------------|----------|-------|------------|
| horner:work:sample_horner | horner | nt | SAHIDIC | Sample Horner | 0 |
| ud_coptic:work:apophthegmata.patrum_ap.024.isaac_cells.07 | ud_coptic | scriptorium | SAHIDIC | Apophthegmata Patrum Sahidic 024: Isaac | 32 |
| ud_coptic:work:besa.letters_on_lack_of_food | ud_coptic | scriptorium | SAHIDIC | On Lack of Food | 97 |

---

#### 2. `segments` Table

Text segments (sentences, verses, paragraphs) with multiple text representations.

| Column | Type | Description |
|--------|------|-------------|
| `segment_id` | VARCHAR (PK) | Unique segment identifier |
| `document_id` | VARCHAR (FK) | Parent document |
| `order` | INTEGER | Sequence number within document |
| `text_canonical` | VARCHAR | Primary text representation |
| `text_stripped` | VARCHAR | Text without diacritics |
| `text_display` | VARCHAR | Display text with formatting |
| `text_hieroglyphs` | VARCHAR | Unicode hieroglyphs from TLA (16,379 segments: 12,773 earlier_egyptian + 3,606 late_egyptian). Contains both Unicode hieroglyphs (𓂋𓐍𓏛) and Manuel de Codage tags (`<g>M12B</g>`) |
| `text_en` | VARCHAR | English translation |
| `text_de` | VARCHAR | German translation from TLA (29,762 segments) |
| `translation_language` | VARCHAR | Translation language code (e.g., 'de', 'en') |
| `dialect` | VARCHAR | Segment dialect (may differ from document) |
| `script` | VARCHAR | Script variant |
| `genre` | VARCHAR[] | Genre tags |
| `passage_ref` | VARCHAR | Biblical/textual reference (e.g., "Matt 5:3") |
| `metadata` | JSON | Additional metadata |
| `content_hash` | VARCHAR | SHA-256 hash for deduplication |
| `created_at` | TIMESTAMP | Creation timestamp |

**Indexes**:
- `idx_segments_document` on `(document_id, order)`
- `idx_segments_dialect` on `dialect`
- `idx_segments_content_hash` on `content_hash`

**Example Rows**:

```sql
SELECT segment_id, text_canonical, passage_ref
FROM segments
WHERE document_id = 'horner:work:sample_horner'
LIMIT 3;
```

| segment_id | text_canonical | passage_ref |
|------------|----------------|-------------|
| v_Mt_1_2 | ⲁⲃⲣⲁϩⲁⲙ ⲁϥϫⲡⲟ ⲛⲓⲥⲁⲁⲕ ⲓⲥⲁⲁⲕ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲓⲁⲕⲱⲃ ⲓⲁⲕⲱⲃ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲓⲟⲩⲇⲁⲥ ⲙⲛ ⲛⲉϥⲥⲛⲏⲩ | Mt 1:2 |
| v_Mt_1_1 | ⲡϫⲱⲱⲙⲉ ⲙⲡⲉϫⲓⲛⲉ ⲛⲓⲏⲥⲟⲩⲥ ⲡⲉⲭⲣⲓⲥⲧⲟⲥ ⲡϣⲏⲣⲉ ⲛⲇⲁⲩⲉⲓⲇ ⲡϣⲏⲣⲉ ⲛⲁⲃⲣⲁϩⲁⲙ | Mt 1:1 |
| v_Mt_1_3 | ⲓⲟⲩⲇⲁⲥ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲫⲁⲣⲉⲥ ⲙⲛ ⲍⲁⲣⲁ ⲉⲃⲟⲗ ϩⲛ ⲑⲁⲙⲁⲣ ⲫⲁⲣⲉⲥ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲉⲥⲣⲱⲙ ⲉⲥⲣⲱⲙ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲁⲣⲁⲙ | Mt 1:3 |

---

#### 3. `token_instances` Table

Individual token occurrences with linguistic annotations and lemma references.

| Column | Type | Description |
|--------|------|-------------|
| `token_id` | VARCHAR (PK) | Unique token identifier |
| `segment_id` | VARCHAR (FK) | Parent segment |
| `document_id` | VARCHAR (FK) | Parent document |
| `order` | INTEGER | Position within segment |
| `form` | VARCHAR | Surface form as it appears in text |
| `lemma_id` | VARCHAR | Cross-DB reference to lexicon.lemmas |
| `lang` | VARCHAR | Language code (cop, egy, grc) |
| `head` | INTEGER | Dependency head token index |
| `deprel` | VARCHAR | Dependency relation (nsubj, obj, etc.) |
| `metadata` | JSON | Additional contextual metadata |
| `content_hash` | VARCHAR | Content hash |

**Indexes**:
- `idx_tokens_segment` on `(segment_id, order)`
- `idx_tokens_document` on `(document_id, order)`
- `idx_tokens_lemma` on `lemma_id`
- `idx_tokens_lang` on `lang`

**Lemma ID Format**:
```
{language}:lemma:{lemma}
```

Examples:
- `cop:lemma:ⲁⲛⲟⲕ` (Coptic "I")
- `egy:lemma:nḏ` (Egyptian "protect")
- `grc:lemma:θεός` (Greek "god")

**Example Rows**:

```sql
SELECT token_id, form, lemma_id, lang, head, deprel
FROM token_instances
WHERE segment_id = 'IBUBd4PsV5DB0UvgiHvQFiOo9JY'
ORDER BY "order"
LIMIT 5;
```

| token_id | form | lemma_id | lang | head | deprel |
|----------|------|----------|------|------|--------|
| IBUBd1YhBgmcpkR8lYUJwLQQIAc | nn | egy:lemma:nn | egy | NULL | NULL |
| IBUBd3PQ1F8pI0WxlYVkapLN7sk | gmi̯ | egy:lemma:gmi̯ | egy | NULL | NULL |
| IBUBd0qLAWKlsEg7uYZGHGS4LIg | =tw | egy:lemma:=tw | egy | NULL | NULL |
| IBUBdW2TjgRl5kQyvYfBGQPcOFY | 2.nw | egy:lemma:sn.nw | egy | NULL | NULL |
| IBUBdwW6E2T0hkrIopY5V9pjq9A | =k | egy:lemma:=k | egy | NULL | NULL |

---

#### 4. Views

##### `segments_enriched`

Segments with document metadata joined in.

```sql
CREATE VIEW segments_enriched AS
SELECT
    s.*,
    d.source,
    d.collection,
    d.stage,
    d.substage,
    d.language,
    d.title,
    d.authors
FROM segments s
JOIN documents d ON s.document_id = d.document_id;
```

##### `tokens_with_context`

Tokens with segment and document context (useful for concordance).

```sql
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
```

---

#### 5. `corpus_statistics` Table

Pre-computed corpus statistics for fast queries.

| Column | Type | Description |
|--------|------|-------------|
| `stat_key` | VARCHAR (PK) | Statistic identifier |
| `stat_value` | JSON | Statistic value (structured) |
| `updated_at` | TIMESTAMP | Last update |

**Example Entries**:

```sql
SELECT * FROM corpus_statistics;
```

| stat_key | stat_value |
|----------|------------|
| total_collections | 92 |
| total_documents | 1552 |
| total_segments | 252826 |
| total_tokens | 1558260 |

---

## Lexicon Database (`lexicon.duckdb`)

**Purpose**: Dictionary lookups, frequency analysis, lexicography, etymological research

**Size**: 83 MB

### Schema

#### 1. `lemmas` Table

Unique lexical entries with linguistic properties and corpus-derived statistics.

| Column | Type | Description |
|--------|------|-------------|
| `lemma_id` | VARCHAR (PK) | Unique lemma identifier (`lang:lemma:form`) |
| `lemma` | VARCHAR | Canonical citation form |
| `language` | VARCHAR | ISO 639-3 code (cop, egy, grc) |
| `script` | VARCHAR | Writing system |
| `period` | VARCHAR | Historical period (for Egyptian) |
| `pos` | VARCHAR | Part of speech (NOUN, VERB, ADJ, etc.) |
| `pos_detail` | VARCHAR | Detailed POS (VERB_TRANSITIVE, etc.) |
| `gloss_en` | VARCHAR | English gloss/translation |
| `gloss_de` | VARCHAR | German gloss/translation |
| `gloss_fr` | VARCHAR | French gloss (future) |
| `semantic_domain` | VARCHAR[] | Semantic classification |
| `semantic_field` | VARCHAR | Broader semantic category |
| `hieroglyphic_writing` | VARCHAR | Unicode hieroglyphs (𓅓𓈖𓏥) |
| `mdc_transcription` | VARCHAR | Manuel de Codage (i-n:k) |
| `gardiner_codes` | VARCHAR[] | Sign list codes (["A1", "N35"]) |
| `transliteration` | VARCHAR | Egyptian transliteration (jnk, ꜥnḫ) |
| `bohairic_form` | VARCHAR | Bohairic variant |
| `sahidic_form` | VARCHAR | Sahidic variant |
| `other_dialects` | JSON | Other dialectal forms |
| `frequency` | INTEGER | Total occurrences in corpus |
| `document_count` | INTEGER | Documents where lemma appears |
| `collection_count` | INTEGER | Collections where lemma appears |
| `first_attested_date` | INTEGER | Earliest attestation |
| `last_attested_date` | INTEGER | Latest attestation |
| `first_attested_period` | VARCHAR | Earliest period |
| `last_attested_period` | VARCHAR | Latest period |
| `attested_regions` | VARCHAR[] | Geographic attestations |
| `etymology_source_lemma_id` | VARCHAR | Parent lemma (etymology) |
| `etymology_type` | VARCHAR | INHERITED, BORROWED, DERIVED, COMPOUND |
| `etymology_notes` | TEXT | Free-form etymology notes |
| `synonyms` | VARCHAR[] | Synonym lemma_ids |
| `antonyms` | VARCHAR[] | Antonym lemma_ids |
| `hypernyms` | VARCHAR[] | Broader terms |
| `hyponyms` | VARCHAR[] | Narrower terms |
| `related_lemmas` | VARCHAR[] | Related lemma_ids |
| `phonetic_form` | VARCHAR | IPA or phonological representation |
| `phonological_notes` | TEXT | Phonological notes |
| `source` | VARCHAR | Data source (scriptorium, tla, aes, etc.) |
| `source_id` | VARCHAR | Original ID in source |
| `confidence` | DECIMAL(3,2) | Quality score (0.0-1.0) |
| `metadata` | JSON | Additional metadata |
| `created_at` | TIMESTAMP | Creation timestamp |
| `updated_at` | TIMESTAMP | Last update |

**Indexes**:
- `idx_lemmas_lemma` on `lemma`
- `idx_lemmas_language` on `language`
- `idx_lemmas_script` on `script`
- `idx_lemmas_pos` on `pos`
- `idx_lemmas_frequency` on `frequency DESC`
- `idx_lemmas_period` on `period`

**Constraint**: `UNIQUE(lemma, language, pos)` - Same lemma can have multiple POS entries

**Example Rows**:

```sql
SELECT lemma_id, lemma, language, pos, gloss_en, frequency
FROM lemmas
WHERE frequency > 10000
ORDER BY frequency DESC
LIMIT 5;
```

| lemma_id | lemma | language | pos | gloss_en | frequency |
|----------|-------|----------|-----|----------|-----------|
| egy:lemma:=f | =f | egy | NULL | NULL | 50,258 |
| egy:lemma:m | m | egy | NULL | NULL | 39,763 |
| egy:lemma:n | n | egy | NULL | NULL | 36,523 |
| egy:lemma:=k | =k | egy | NULL | NULL | 36,043 |
| egy:lemma:r | r | egy | NULL | NULL | 20,546 |

---

#### 2. `forms` Table

Attested surface forms with morphological information.

| Column | Type | Description |
|--------|------|-------------|
| `form_id` | VARCHAR (PK) | Unique form identifier |
| `lemma_id` | VARCHAR (FK) | Parent lemma |
| `form` | VARCHAR | Surface form as attested |
| `form_normalized` | VARCHAR | Normalized orthography |
| `form_transliterated` | VARCHAR | Transliteration (for Egyptian) |
| `morphology` | VARCHAR | Compact string (Gender=Masc\|Number=Sing) |
| `morphology_detailed` | JSON | Structured morphology |
| `tense` | VARCHAR | For verbs |
| `aspect` | VARCHAR | For verbs |
| `mood` | VARCHAR | For verbs |
| `voice` | VARCHAR | For verbs |
| `person` | VARCHAR | 1, 2, 3 |
| `number` | VARCHAR | Singular, Plural, Dual |
| `gender` | VARCHAR | Masculine, Feminine, Neuter |
| `case_marking` | VARCHAR | Nominative, Accusative, etc. |
| `frequency` | INTEGER | Occurrences of this form |
| `relative_frequency` | DECIMAL(5,4) | Frequency within lemma (0.0-1.0) |
| `metadata` | JSON | Additional metadata |
| `created_at` | TIMESTAMP | Creation timestamp |

**Indexes**:
- `idx_forms_lemma` on `lemma_id`
- `idx_forms_form` on `form`
- `idx_forms_frequency` on `frequency DESC`

**Constraint**: `UNIQUE(lemma_id, form, morphology)`

**Example Rows**:

```sql
SELECT form_id, form, morphology, frequency
FROM forms
WHERE lemma_id = 'egy:lemma:=f'
ORDER BY frequency DESC
LIMIT 5;
```

| form_id | form | morphology | frequency |
|---------|------|------------|-----------|
| egy:form:=f:45978 | =f | NULL | 45,978 |
| egy:form:=[f]:2344 | =[f] | NULL | 2,344 |
| egy:form:=⸢f⸣:508 | =⸢f⸣ | NULL | 508 |
| egy:form:=〈f〉:431 | =〈f〉 | NULL | 431 |
| egy:form:={f}:127 | ={f} | NULL | 127 |

---

#### 3. `lemma_attestations` Table

Period/dialect-specific statistics for lemmas.

| Column | Type | Description |
|--------|------|-------------|
| `attestation_id` | VARCHAR (PK) | Unique attestation identifier |
| `lemma_id` | VARCHAR (FK) | Parent lemma |
| `dimension_type` | VARCHAR | PERIOD, DIALECT, COLLECTION, GENRE |
| `dimension_value` | VARCHAR | Specific value (SAHIDIC, biblical, etc.) |
| `frequency` | INTEGER | Occurrences in this dimension |
| `document_count` | INTEGER | Documents in this dimension |
| `first_occurrence` | DATE | First occurrence date |
| `last_occurrence` | DATE | Last occurrence date |
| `example_segment_ids` | VARCHAR[] | Sample segment IDs |
| `example_forms` | VARCHAR[] | Sample forms |
| `created_at` | TIMESTAMP | Creation timestamp |
| `updated_at` | TIMESTAMP | Last update |

**Indexes**:
- `idx_attestations_lemma` on `lemma_id`
- `idx_attestations_dimension` on `(dimension_type, dimension_value)`
- `idx_attestations_frequency` on `frequency DESC`

**Constraint**: `UNIQUE(lemma_id, dimension_type, dimension_value)`

**Example Rows**:

```sql
SELECT dimension_type, dimension_value, frequency
FROM lemma_attestations
WHERE lemma_id = 'egy:lemma:=f'
ORDER BY frequency DESC
LIMIT 5;
```

| dimension_type | dimension_value | frequency |
|----------------|-----------------|-----------|
| STAGE | EGYPTIAN | 50,258 |
| SUBSTAGE | MIDDLE_EGYPTIAN | 32,816 |
| COLLECTION | sawlit | 12,293 |
| SUBSTAGE | LATE_EGYPTIAN | 10,884 |
| SUBSTAGE | DEMOTIC | 6,558 |

---

#### 4. Views and Statistics Tables

The lexicon database includes several materialized views for common queries:

- **`lemmas_egyptian`**: Egyptian-only lemmas for quick filtering
- **`lemmas_coptic`**: Coptic-only lemmas for quick filtering
- **`lemmas_high_frequency`**: Pre-filtered high-frequency lemmas (freq >= 100)
- **`lexicon_statistics`**: Pre-computed statistics for dashboard queries

**Example - Lexicon Statistics**:

```sql
SELECT * FROM lexicon_statistics;
```

| stat_key | stat_value | updated_at |
|----------|------------|------------|
| total_lemmas | {"count": 33259} | 2025-11-07 13:43:58 |
| total_forms | {"count": 104659} | 2025-11-07 13:43:58 |
| total_attestations | {"count": 103908} | 2025-11-07 13:43:58 |
| lemmas_by_language | {"egy": 23784, "cop": 9459, "grc": 15, "he": 1} | 2025-11-07 13:43:58 |

---

#### 5. `etymology_relations` Table

Coptic→Egyptian etymological relationships from ORAEC digitized etymologies (Černý, Vycichl, Westendorf dictionaries).

| Column | Type | Description |
|--------|------|-------------|
| `relation_id` | VARCHAR (PK) | Unique relation identifier |
| `source_lemma_id` | VARCHAR (FK) | Coptic lemma (derived form) |
| `target_lemma_id` | VARCHAR (FK) | Egyptian lemma (source) - may be NULL if unresolved |
| `relation_type` | VARCHAR | Etymology type (DERIVED_FROM, BORROWED_FROM, etc.) |
| `confidence` | DECIMAL(3,2) | Confidence score (0.0-1.0) |
| `approximate_date` | INTEGER | Approximate date of borrowing/derivation |
| `date_range_from` | INTEGER | Date range start |
| `date_range_to` | INTEGER | Date range end |
| `evidence` | TEXT | Evidence description |
| `references` | VARCHAR[] | Source references |
| `phonological_change` | VARCHAR | Phonological change description |
| `metadata` | JSON | Additional metadata (includes external IDs: cdo_id, oraec_id, tla_id) |
| `created_at` | TIMESTAMP | Creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |

**Indexes**:
- `idx_etymology_source` on `source_lemma_id`
- `idx_etymology_target` on `target_lemma_id`
- `idx_etymology_type` on `relation_type`

**Constraint**: `UNIQUE(source_lemma_id, target_lemma_id, relation_type)`

**Example Rows**:

```sql
SELECT
    er.relation_id,
    lc.lemma as coptic_lemma,
    lc.gloss_en as coptic_gloss,
    er.confidence,
    json_extract_string(er.metadata, '$.cdo_id') as cdo_id,
    json_extract_string(er.metadata, '$.oraec_id') as oraec_id,
    json_extract_string(er.metadata, '$.tla_id') as tla_id
FROM etymology_relations er
JOIN lemmas lc ON er.source_lemma_id = lc.lemma_id
LIMIT 3;
```

| relation_id | coptic_lemma | coptic_gloss | confidence | cdo_id | oraec_id | tla_id |
|-------------|--------------|--------------|------------|--------|----------|--------|
| etym:cop:lemma:ⲁⲃⲉ:36610 | ⲁⲃⲉ | strap, chain | 0.50 | C6 | 36610 | |
| etym:cop:lemma:ⲁⲃⲱⲕ:928 | ⲁⲃⲱⲕ | raven, crow | 0.50 | C9 | | 928 |
| etym:cop:lemma:ⲁⲃⲥⲱⲛ:23930 | ⲁⲃⲥⲱⲛ | wild mint | 0.50 | C14 | 23930 | |

---

#### 6. `cdo_mappings` Table

Cross-reference table linking Coptic Dictionary Online (CDO) IDs to KEMET lemma_ids.

| Column | Type | Description |
|--------|------|-------------|
| `cdo_id` | VARCHAR (PK) | CDO identifier (e.g., "C1494") |
| `lemma_id` | VARCHAR (FK) | KEMET lemma identifier |
| `lemma` | VARCHAR | Coptic lemma form |
| `confidence` | DECIMAL(3,2) | Mapping confidence (1.0 = exact match) |
| `match_method` | VARCHAR | How the mapping was made (exact, normalized, fuzzy) |
| `created_at` | TIMESTAMP | Creation timestamp |

**Index**:
- `idx_cdo_mappings_lemma` on `lemma_id`

**Statistics**:
- **9,493 CDO ID mappings** (covers all 9,306 CDO-sourced lemmas plus some duplicates)

**Example Rows**:

```sql
SELECT cdo_id, lemma, lemma_id, confidence
FROM cdo_mappings
LIMIT 5;
```

| cdo_id | lemma | lemma_id | confidence |
|--------|-------|----------|------------|
| C2 | ⲁ- | cop:lemma:ⲁ- | 1.00 |
| C6 | ⲁⲃⲉ | cop:lemma:ⲁⲃⲉ | 1.00 |
| C7 | ⲁⲃⲱ | cop:lemma:ⲁⲃⲱ | 1.00 |
| C9 | ⲁⲃⲱⲕ | cop:lemma:ⲁⲃⲱⲕ | 1.00 |
| C14 | ⲁⲃⲥⲱⲛ | cop:lemma:ⲁⲃⲥⲱⲛ | 1.00 |

---

#### 7. Other Tables (Future-Ready)

The lexicon database includes several tables prepared for future semantic network functionality:

- **`oraec_mappings`**: ORAEC/TLA ID mappings (reserved for future Egyptian lemma resolution)
- **`semantic_relations`**: Semantic relationships (SYNONYM, ANTONYM, HYPERNYM, HYPONYM, MERONYM, HOLONYM)
- **`collocations`**: Frequently co-occurring lemmas with statistical scores

The `oraec_mappings` and `semantic_relations` tables are currently empty but have complete schemas. See `schemas/lexicon_schema.sql` for details.

---

#### 8. Views

##### `lemmas_full`

Comprehensive lemma view with counts of related data.

```sql
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
```

##### `lemmas_high_frequency`

High-frequency lemmas (frequency >= 100).

```sql
CREATE VIEW lemmas_high_frequency AS
SELECT *
FROM lemmas
WHERE frequency >= 100
ORDER BY frequency DESC;
```

##### `lemmas_coptic` / `lemmas_egyptian`

Language-specific views.

```sql
CREATE VIEW lemmas_coptic AS
SELECT * FROM lemmas WHERE language = 'cop';

CREATE VIEW lemmas_egyptian AS
SELECT * FROM lemmas WHERE language = 'egy';
```

##### `dictionary_entries`

Pre-joined view for typical dictionary queries.

```sql
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
    l.transliteration,
    l.bohairic_form,
    l.sahidic_form,
    ARRAY_AGG(DISTINCT f.form ORDER BY f.frequency DESC) as common_forms,
    ARRAY_AGG(DISTINCT a.dimension_value ORDER BY a.frequency DESC)
        FILTER (WHERE a.dimension_type = 'PERIOD') as attested_periods,
    l.etymology_source_lemma_id
FROM lemmas l
LEFT JOIN forms f ON l.lemma_id = f.lemma_id
LEFT JOIN lemma_attestations a ON l.lemma_id = a.lemma_id
GROUP BY l.lemma_id;
```

---

#### 9. `lexicon_statistics` Table

Pre-computed lexicon statistics.

| Column | Type | Description |
|--------|------|-------------|
| `stat_key` | VARCHAR (PK) | Statistic identifier |
| `stat_value` | JSON | Statistic value |
| `updated_at` | TIMESTAMP | Last update |

**Example Entries**:

```sql
SELECT * FROM lexicon_statistics;
```

| stat_key | stat_value |
|----------|------------|
| total_lemmas | {"count": 33212} |
| lemmas_by_language | {"cop": 9459, "egy": 23784, "grc": 15, "he": 1} |
| lemmas_with_etymology | {"count": 1750} |
| average_frequency | {"value": 47.2} |

---

## Query Examples

### Corpus Database Queries

#### 1. Get Continuous Text for Language Modeling

```sql
-- Extract all Sahidic Coptic text in document order
SELECT text_canonical
FROM segments
WHERE dialect = 'SAHIDIC'
ORDER BY document_id, "order";
```

#### 2. Corpus Statistics

```sql
-- Overall corpus statistics
SELECT
    COUNT(DISTINCT document_id) as total_documents,
    COUNT(DISTINCT segment_id) as total_segments,
    COUNT(*) as total_tokens
FROM token_instances;

-- Statistics by language
SELECT
    lang,
    COUNT(*) as token_count,
    COUNT(DISTINCT lemma_id) as unique_lemmas,
    COUNT(DISTINCT document_id) as document_count
FROM token_instances
GROUP BY lang
ORDER BY token_count DESC;
```

#### 3. Find Documents by Criteria

```sql
-- Find all Middle Egyptian texts
SELECT
    document_id,
    title,
    collection,
    num_tokens,
    date_from,
    date_to
FROM documents
WHERE substage = 'MIDDLE_EGYPTIAN'
ORDER BY date_from;

-- Find biblical texts in Bohairic dialect
SELECT DISTINCT
    d.document_id,
    d.title,
    d.substage,
    d.num_tokens
FROM documents d
WHERE d.substage = 'BOHAIRIC'
    AND 'biblical' = ANY(d.genre)
ORDER BY d.title;
```

#### 4. Concordance (KWIC - Key Word In Context)

```sql
-- Find all occurrences of lemma "ⲁⲛⲟⲕ" (I) with context
SELECT
    form,
    segment_text,
    passage_ref,
    title,
    dialect
FROM tokens_with_context
WHERE lemma_id = 'cop:lemma:ⲁⲛⲟⲕ'
ORDER BY title, passage_ref
LIMIT 20;
```

#### 5. Token-Level Analysis

```sql
-- Get all tokens in a specific segment with lemma info
SELECT
    t.order,
    t.form,
    t.lemma_id,
    t.lang,
    t.head,
    t.deprel
FROM token_instances t
WHERE t.segment_id = 'horner:matt:1:1'
ORDER BY t.order;
```

#### 6. Dependency Parsing Analysis

```sql
-- Find all subject-verb relations
SELECT
    t1.form as subject,
    t2.form as verb,
    s.text_canonical as sentence,
    s.passage_ref
FROM token_instances t1
JOIN token_instances t2 ON t1.segment_id = t2.segment_id AND t1.head = t2.order
JOIN segments s ON t1.segment_id = s.segment_id
WHERE t1.deprel = 'nsubj'
LIMIT 100;
```

#### 7. Collection-Level Statistics

```sql
-- Token counts by collection
SELECT
    d.collection,
    COUNT(DISTINCT d.document_id) as document_count,
    COUNT(DISTINCT t.segment_id) as segment_count,
    COUNT(t.token_id) as token_count
FROM documents d
JOIN token_instances t ON d.document_id = t.document_id
GROUP BY d.collection
ORDER BY token_count DESC;
```

#### 8. Accessing Hieroglyphic Data

```sql
-- Get segments with hieroglyphs from TLA
SELECT
    segment_id,
    text_canonical,
    text_hieroglyphs,
    text_de,
    document_id
FROM segments
WHERE text_hieroglyphs IS NOT NULL
LIMIT 10;

-- Count hieroglyphic coverage by collection
SELECT
    d.collection,
    d.substage,
    COUNT(*) as total_segments,
    COUNT(s.text_hieroglyphs) as segments_with_hieroglyphs,
    ROUND(100.0 * COUNT(s.text_hieroglyphs) / COUNT(*), 2) as coverage_percent
FROM segments s
JOIN documents d ON s.document_id = d.document_id
WHERE d.stage = 'EGYPTIAN'
GROUP BY d.collection, d.substage
HAVING COUNT(s.text_hieroglyphs) > 0
ORDER BY segments_with_hieroglyphs DESC;

-- Get segments with German translations
SELECT
    segment_id,
    text_canonical,
    text_de as german_translation,
    document_id
FROM segments
WHERE text_de IS NOT NULL
    AND translation_language = 'de'
LIMIT 10;

-- Find all earlier_egyptian segments with hieroglyphs
SELECT
    s.segment_id,
    s.text_canonical,
    s.text_hieroglyphs,
    s.text_de,
    d.substage,
    d.collection
FROM segments s
JOIN documents d ON s.document_id = d.document_id
WHERE s.text_hieroglyphs IS NOT NULL
    AND d.substage = 'EARLIER_EGYPTIAN'
ORDER BY d.collection, s."order"
LIMIT 20;
```

---

### Lexicon Database Queries

#### 1. Dictionary Lookup

```sql
-- Look up a Coptic lemma
SELECT
    lemma,
    pos,
    gloss_en,
    frequency,
    document_count,
    sahidic_form,
    bohairic_form
FROM lemmas
WHERE lemma = 'ⲁⲛⲟⲕ';

-- Look up an Egyptian lemma
SELECT
    lemma,
    pos,
    gloss_en,
    frequency,
    hieroglyphic_writing,
    transliteration
FROM lemmas
WHERE lemma = 'nḏ'
AND language = 'egy';
```

#### 2. Frequency Lists

```sql
-- Top 100 most frequent lemmas
SELECT
    lemma,
    language,
    pos,
    gloss_en,
    frequency,
    document_count
FROM lemmas
ORDER BY frequency DESC
LIMIT 100;

-- Top 50 Coptic verbs
SELECT
    lemma,
    gloss_en,
    frequency,
    sahidic_form,
    bohairic_form
FROM lemmas
WHERE language = 'cop' AND pos = 'VERB'
ORDER BY frequency DESC
LIMIT 50;
```

#### 3. Form-to-Lemma Mapping

```sql
-- Find all lemmas for a given surface form
SELECT DISTINCT
    l.lemma_id,
    l.lemma,
    l.pos,
    l.gloss_en,
    f.morphology,
    f.frequency as form_frequency
FROM forms f
JOIN lemmas l ON f.lemma_id = l.lemma_id
WHERE f.form = 'ⲡⲉ'
ORDER BY f.frequency DESC;
```

#### 4. Dialectal Variation

```sql
-- Find lemmas with different Sahidic/Bohairic forms
SELECT
    lemma_id,
    lemma,
    pos,
    gloss_en,
    sahidic_form,
    bohairic_form,
    frequency
FROM lemmas
WHERE sahidic_form IS NOT NULL
    AND bohairic_form IS NOT NULL
    AND sahidic_form != bohairic_form
ORDER BY frequency DESC
LIMIT 50;
```

#### 5. Attestation Analysis

```sql
-- Get dialectal distribution for a lemma
SELECT
    dimension_value as dialect,
    frequency,
    document_count
FROM lemma_attestations
WHERE lemma_id = 'cop:lemma:ⲛⲟⲩⲧⲉ'
    AND dimension_type = 'DIALECT'
ORDER BY frequency DESC;

-- Lemmas attested in multiple periods
SELECT
    l.lemma,
    l.language,
    l.gloss_en,
    COUNT(DISTINCT a.dimension_value) as period_count,
    ARRAY_AGG(DISTINCT a.dimension_value ORDER BY a.dimension_value) as periods
FROM lemmas l
JOIN lemma_attestations a ON l.lemma_id = a.lemma_id
WHERE a.dimension_type = 'PERIOD'
GROUP BY l.lemma_id, l.lemma, l.language, l.gloss_en
HAVING COUNT(DISTINCT a.dimension_value) > 1
ORDER BY period_count DESC;
```

#### 6. TLA Integration Queries

```sql
-- Top Egyptian lemmas (pre-filtered view)
SELECT
    lemma,
    pos,
    gloss_en,
    frequency,
    hieroglyphic_writing,
    transliteration
FROM lemmas_egyptian
ORDER BY frequency DESC
LIMIT 50;

-- Egyptian lemma coverage statistics
SELECT
    COUNT(*) as total_egyptian_lemmas,
    COUNT(CASE WHEN hieroglyphic_writing IS NOT NULL THEN 1 END) as lemmas_with_hieroglyphs,
    ROUND(100.0 * COUNT(CASE WHEN hieroglyphic_writing IS NOT NULL THEN 1 END) / COUNT(*), 2) as coverage_percent
FROM lemmas
WHERE language = 'egy';
```

#### 7. Morphological Analysis

```sql
-- Get all attested forms for a lemma with morphology
SELECT
    form,
    morphology,
    tense,
    aspect,
    person,
    number,
    frequency,
    relative_frequency
FROM forms
WHERE lemma_id = 'cop:lemma:ⲉⲓⲣⲉ'
ORDER BY frequency DESC;

-- Most morphologically diverse lemmas
SELECT
    l.lemma,
    l.pos,
    l.gloss_en,
    COUNT(DISTINCT f.form) as form_count,
    l.frequency
FROM lemmas l
JOIN forms f ON l.lemma_id = f.lemma_id
GROUP BY l.lemma_id, l.lemma, l.pos, l.gloss_en, l.frequency
ORDER BY form_count DESC
LIMIT 20;
```

#### 8. Etymology Queries

```sql
-- Look up a Coptic lemma's Egyptian etymology
SELECT
    lc.lemma as coptic_lemma,
    lc.gloss_en as coptic_gloss,
    lc.pos as coptic_pos,
    er.relation_type,
    er.confidence,
    json_extract_string(er.metadata, '$.cdo_id') as cdo_id,
    json_extract_string(er.metadata, '$.oraec_id') as oraec_id,
    json_extract_string(er.metadata, '$.tla_id') as tla_id,
    er.evidence
FROM lemmas lc
JOIN etymology_relations er ON lc.lemma_id = er.source_lemma_id
WHERE lc.lemma = 'ⲕⲁϩ';

-- Find all Coptic lemmas derived from Egyptian (with external IDs)
SELECT
    lc.lemma as coptic_lemma,
    lc.gloss_en as coptic_gloss,
    json_extract_string(er.metadata, '$.cdo_id') as cdo_id,
    json_extract_string(er.metadata, '$.oraec_id') as oraec_id,
    json_extract_string(er.metadata, '$.tla_id') as tla_id,
    er.confidence
FROM etymology_relations er
JOIN lemmas lc ON er.source_lemma_id = lc.lemma_id
ORDER BY lc.frequency DESC
LIMIT 50;

-- Count etymology relations by relation type
SELECT
    relation_type,
    COUNT(*) as relation_count
FROM etymology_relations
GROUP BY relation_type
ORDER BY relation_count DESC;

-- Find Coptic lemmas with etymology data
SELECT
    l.lemma,
    l.gloss_en,
    l.frequency,
    COUNT(er.relation_id) as etymology_count
FROM lemmas l
JOIN etymology_relations er ON l.lemma_id = er.source_lemma_id
WHERE l.language = 'cop'
GROUP BY l.lemma_id, l.lemma, l.gloss_en, l.frequency
ORDER BY l.frequency DESC
LIMIT 50;
```

#### 9. CDO Cross-Reference Queries

```sql
-- Look up a CDO ID
SELECT
    cdo.cdo_id,
    cdo.lemma,
    l.gloss_en,
    l.pos,
    l.frequency,
    l.sahidic_form,
    l.bohairic_form
FROM cdo_mappings cdo
JOIN lemmas l ON cdo.lemma_id = l.lemma_id
WHERE cdo.cdo_id = 'C1494';

-- Find all CDO-sourced lemmas
SELECT
    l.lemma,
    l.pos,
    l.gloss_en,
    l.frequency,
    cdo.cdo_id
FROM lemmas l
JOIN cdo_mappings cdo ON l.lemma_id = cdo.lemma_id
WHERE l.source = 'cdo'
ORDER BY l.lemma
LIMIT 100;

-- Count CDO mappings
SELECT
    COUNT(DISTINCT cdo_id) as total_cdo_ids,
    COUNT(DISTINCT lemma_id) as total_lemmas
FROM cdo_mappings;
```

---

### Cross-Database Queries

DuckDB supports cross-database queries using `ATTACH`:

#### 1. Setup

```sql
-- Attach lexicon database
ATTACH 'lexicon.duckdb' AS lexicon;

-- Now you can query both databases
SELECT * FROM main.documents LIMIT 5;          -- Corpus
SELECT * FROM lexicon.lemmas LIMIT 5;           -- Lexicon
```

#### 2. Token with Full Lemma Information

```sql
-- Get tokens with complete lexical information
ATTACH 'lexicon.duckdb' AS lexicon;

SELECT
    t.form,
    l.lemma,
    l.pos,
    l.gloss_en,
    l.frequency as lemma_frequency,
    s.text_canonical as sentence,
    d.title
FROM main.token_instances t
JOIN main.segments s ON t.segment_id = s.segment_id
JOIN main.documents d ON t.document_id = d.document_id
LEFT JOIN lexicon.lemmas l ON t.lemma_id = l.lemma_id
WHERE t.segment_id = 'horner:matt:1:1'
ORDER BY t.order;
```

#### 3. Concordance with Full Lemma Data

```sql
-- Enhanced concordance with lexical properties
ATTACH 'lexicon.duckdb' AS lexicon;

SELECT
    t.form,
    l.lemma,
    l.pos,
    l.gloss_en,
    s.text_canonical as context,
    s.passage_ref,
    d.title,
    s.dialect
FROM main.token_instances t
JOIN main.segments s ON t.segment_id = s.segment_id
JOIN main.documents d ON t.document_id = d.document_id
JOIN lexicon.lemmas l ON t.lemma_id = l.lemma_id
WHERE l.gloss_en LIKE '%protect%'
ORDER BY d.title, s.passage_ref
LIMIT 50;
```

#### 4. Lemma Frequency Validation

```sql
-- Compare lexicon frequency with actual corpus counts
ATTACH 'lexicon.duckdb' AS lexicon;

SELECT
    l.lemma,
    l.language,
    l.frequency as lexicon_frequency,
    COUNT(t.token_id) as actual_corpus_count,
    l.frequency - COUNT(t.token_id) as difference
FROM lexicon.lemmas l
LEFT JOIN main.token_instances t ON l.lemma_id = t.lemma_id
GROUP BY l.lemma_id, l.lemma, l.language, l.frequency
HAVING l.frequency != COUNT(t.token_id)
ORDER BY ABS(l.frequency - COUNT(t.token_id)) DESC
LIMIT 20;
```

---

## Performance Tips

### Corpus Database

1. **Use Indexed Columns**: Always filter on indexed columns when possible:
   - `document_id`, `segment_id`, `lemma_id`, `lang`, `dialect`, `substage`

2. **Sequential Access**: For language modeling, read segments in order:
   ```sql
   SELECT text_canonical
   FROM segments
   WHERE dialect = 'SAHIDIC'
   ORDER BY document_id, "order";
   ```

3. **Use Views**: Pre-defined views like `tokens_with_context` are optimized for common queries.

4. **Limit Large Queries**: Use `LIMIT` when exploring:
   ```sql
   SELECT * FROM token_instances LIMIT 1000;
   ```

5. **Filter Early**: Apply filters on documents before joining to tokens:
   ```sql
   -- Good: Filter documents first
   WITH sahidic_docs AS (
       SELECT document_id FROM documents WHERE substage = 'SAHIDIC'
   )
   SELECT t.*
   FROM token_instances t
   JOIN sahidic_docs d ON t.document_id = d.document_id;
   ```

### Lexicon Database

1. **Use Frequency Indexes**: Frequency-based queries are optimized:
   ```sql
   SELECT * FROM lemmas WHERE frequency > 100;
   ```

2. **Leverage Views**: Use pre-computed views for complex queries:
   ```sql
   SELECT * FROM dictionary_entries WHERE lemma = 'ⲁⲛⲟⲕ';
   ```

3. **Lemma ID Lookups**: Lemma ID lookups are very fast (primary key):
   ```sql
   SELECT * FROM lemmas WHERE lemma_id = 'cop:lemma:ⲁⲛⲟⲕ';
   ```

4. **Aggregate with Care**: Pre-computed statistics tables avoid expensive aggregations:
   ```sql
   -- Fast: Use statistics table
   SELECT stat_value FROM lexicon_statistics WHERE stat_key = 'total_lemmas';

   -- Slow: Count every row
   SELECT COUNT(*) FROM lemmas;
   ```

### Cross-Database Queries

1. **Attach Once**: Attach lexicon database at session start:
   ```sql
   ATTACH 'lexicon.duckdb' AS lexicon;
   ```

2. **Filter Before Join**: Filter corpus data before joining to lexicon:
   ```sql
   -- Get specific segment tokens first, then join
   WITH segment_tokens AS (
       SELECT * FROM main.token_instances WHERE segment_id = 'horner:matt:1:1'
   )
   SELECT t.*, l.gloss_en
   FROM segment_tokens t
   JOIN lexicon.lemmas l ON t.lemma_id = l.lemma_id;
   ```

3. **Use Appropriate Database**: Choose the right database for your primary query:
   - Start with **corpus** for: text extraction, concordance, sequential reading
   - Start with **lexicon** for: dictionary lookups, frequency analysis, lemma searches

---

For questions or feedback, contact:
- Tiago Tresoldi: tiago.tresoldi@lingfil.uu.se
- Marwan Kilani: marwan.kilani@unibas.ch
