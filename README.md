# KEMET Data: Annotated Corpus and Lexicon for Ancient Egyptian and Coptic

**Version 0.1.0**

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.XXXXXXX.svg)](https://doi.org/10.5281/zenodo.XXXXXXX)

A comprehensive, curated database of Ancient Egyptian and Coptic texts with linguistic annotations, designed for digital humanities research, computational linguistics, and language model development.

---

## Overview

KEMET Data provides **dual DuckDB databases** containing 1.56 million annotated tokens with **lemmatized, morphologically-annotated tokens** spanning Ancient Egyptian and Coptic language stages. The corpus includes hieroglyphic transcriptions, dialectal variations, and cross-linguistic metadata optimized for both NLP applications and scholarly research.

### Key Features

- **Dual Database Architecture**
  - `corpus.duckdb` (642 MB): Document-centric data for NLP and language modeling
  - `lexicon.duckdb` (83 MB): Lemma-centric data for lexicography and linguistic analysis

- **Comprehensive Coverage**
  - 1,552 documents across 92 collections
  - 252,826 text segments
  - 1,558,260 annotated tokens
  - 33,259 unique lemmas with 104,659 attested forms

- **Rich Linguistic Annotations**
  - Lemmatization and POS tagging
  - Morphological features (tense, aspect, mood, person, number, gender)
  - Dependency parsing (head, deprel)
  - Dialectal information (Sahidic, Bohairic, etc.)
  - Unicode hieroglyphs for 16,379 Egyptian text segments from TLA
  - German translations for 29,762 TLA segments

- **High-Quality Data**
  - Normalized Unicode (NFC) with diacritic handling
  - Content-based deduplication
  - Validated against JSON schemas
  - Reproducible build process

---

## Contents

This release includes:

- `corpus.duckdb` - Main corpus database (see [DATABASE.md](DATABASE.md))
- `lexicon.duckdb` - Lexicon database with frequency data
- `README.md` - This file
- `DATABASE.md` - Detailed schema documentation with query examples
- `cookbook.py` - Executable query examples (20 common patterns)
- `LICENSE` - License information and source attributions
- `TODO.md` - Development roadmap and future work

---

## Quick Start

### Requirements

**Software**:
- **DuckDB** 0.8.0+ (download from [duckdb.org](https://duckdb.org))
- **Python** 3.10+ (optional, for programmatic access)

**System**:
- **RAM**: 2 GB minimum, 4 GB recommended
- **Disk**: 1 GB for databases

### Basic Usage

#### 1. Command-line queries with DuckDB CLI

```bash
# Install DuckDB CLI
# See: https://duckdb.org/docs/installation/

# Open the corpus database
duckdb corpus.duckdb

# Example: Get corpus statistics
SELECT
    COUNT(DISTINCT document_id) as documents,
    COUNT(DISTINCT segment_id) as segments,
    COUNT(*) as tokens
FROM token_instances;

# Example: Find all Sahidic Coptic texts
SELECT DISTINCT title, authors, date_from
FROM documents
WHERE substage = 'SAHIDIC'
ORDER BY date_from;
```

#### 2. Python access

```python
import duckdb

# Connect to corpus
corpus = duckdb.connect('corpus.duckdb', read_only=True)

# Get continuous text for language modeling
texts = corpus.execute("""
    SELECT text_canonical
    FROM segments
    WHERE dialect = 'SAHIDIC'
    ORDER BY document_id, "order"
""").fetchall()

# Connect to lexicon
lexicon = duckdb.connect('lexicon.duckdb', read_only=True)

# Dictionary lookup
lemma_info = lexicon.execute("""
    SELECT
        l.lemma,
        l.pos,
        l.gloss_en,
        l.frequency,
        t.hieroglyphs
    FROM lemmas l
    LEFT JOIN tla_metadata t ON l.source_id = t.tla_id
    WHERE l.lemma = 'ⲁⲛⲟⲕ'
""").fetchdf()

print(lemma_info)
```

See [DATABASE.md](DATABASE.md) for comprehensive query examples and schema documentation.

#### 3. Running example queries

```bash
# Run all example queries
python cookbook.py

# Run specific query function
python -c "import cookbook; cookbook.concordance_search()"
python -c "import cookbook; cookbook.dictionary_lookup()"
```

The cookbook includes 20 working examples covering:
- Corpus queries (statistics, concordance, POS distribution, hieroglyphs)
- Lexicon queries (dictionary lookup, frequencies, etymology, dialectal variation)
- Cross-database queries (token enrichment, validation)

---

## Data Sources

This corpus integrates data from the following sources:

| Source | Description | Coverage | License |
|--------|-------------|----------|---------|
| **AES** | Ancient Egyptian Sourcebook | 101,796 segments, 815K tokens | Public Domain |
| **Ramses** | Late Egyptian corpus (Ramses Online) | 71,263 sentences, 531K tokens | CC BY-NC-SA 4.0 |
| **TLA** | Thesaurus Linguae Aegyptiae (hieroglyphs + German) | 29,762 segments, 212K tokens | CC BY-SA 4.0 |
| **Scriptorium** | Coptic SCRIPTORIUM normalized texts | 47,799 segments, 311 tokens | CC BY 4.0 |
| **UD-Coptic** | Universal Dependencies treebank | 2,203 segments, 125 tokens | CC BY-SA 4.0 |
| **Horner** | Sahidic New Testament (segments only) | 3 segments | Public Domain |
| **CDO** | Coptic Dictionary Online (lexicon) | 9,306 Coptic lemmas | CC BY-SA 4.0 |
| **ORAEC** | Coptic etymologies (Černý, Vycichl, Westendorf) | 1,750 etymology relations | CC0 (Public Domain) |

Each source retains its original license. See individual `provenance` fields in the database for attribution.

---

## Use Cases

### 1. Language Model Training
- Pre-training on historical Coptic/Egyptian texts
- Fine-tuning for specialized NLP tasks
- Cross-lingual transfer learning (Coptic ↔ Egyptian)

### 2. Computational Linguistics
- Morphological analysis and tagging
- Dependency parsing evaluation
- Historical language modeling
- Dialectal variation studies

### 3. Digital Humanities
- Corpus exploration and concordance
- Frequency analysis and lexicography
- Diachronic linguistic research
- Text alignment and translation studies

### 4. Egyptology Research
- Full-text search across collections
- Statistical analysis of vocabulary usage
- Cross-collection comparative studies
- Integration with hieroglyphic databases

---

## Database Architecture

KEMET Data uses a **dual database architecture** optimized for different use cases:

### Corpus Database (`corpus.duckdb`)
**Purpose**: NLP, language modeling, corpus linguistics

**Key Tables**:
- `documents` - Document-level metadata
- `segments` - Text segments (sentences/verses)
- `token_instances` - Individual tokens with linguistic annotations
- Views for denormalized access

**Optimized for**:
- Sequential text extraction
- Document-level queries
- Token-in-context analysis

### Lexicon Database (`lexicon.duckdb`)
**Purpose**: Lexicography, frequency analysis, dictionary applications

**Key Tables**:
- `lemmas` - Unique lexical entries with statistics
- `forms` - Attested surface forms with morphology
- `lemma_attestations` - Period/dialect-specific usage
- `tla_metadata` - Hieroglyphic writing and TLA integration

**Optimized for**:
- Dictionary lookups
- Frequency distributions
- Form-to-lemma mappings
- Cross-collection statistics

See [DATABASE.md](DATABASE.md) for complete schema documentation.

---

## Data Quality

### Strengths
- Comprehensive lemmatization and morphology
- Multiple dialects and historical periods covered
- **Unicode hieroglyphs for 16,379 TLA segments** (earlier_egyptian, late_egyptian)
- **German translations for 29,762 TLA segments**
- Unicode normalization and diacritic handling
- Dependency syntax for UD-Coptic subset
- Cross-validated against multiple sources

### Known Limitations

**Data Quality Issues**:
- **OCR Errors**: Some AES texts contain OCR artifacts (estimated 2-5% error rate)
  - Recommendation: Filter by `source != 'aes'` if high precision is required
- **Incomplete Egyptian Morphology**: Only ~12% of Egyptian tokens have full morphological annotation
  - Most Egyptian tokens have lemma and POS tags only
- **Dialectal Attribution Variability**: Dialect labels may vary across sources
  - Some sources use more granular distinctions than others
  - Recommendation: Group by broader categories when comparing across collections
- **Segment-Level Hieroglyphs Only**: Hieroglyphic data available for 16,379 text segments from TLA
  - Coverage includes earlier_egyptian (12,773 segments) and late_egyptian (3,606 segments)
  - Hieroglyphs stored as Unicode text in `text_hieroglyphs` column
  - Includes both standard Unicode hieroglyphs (𓂋𓐍𓏛) and Manuel de Codage tags (`<g>M12B</g>`)

**Missing Features**:
- No parallel text alignments (e.g., Coptic NT ↔ Greek NT)
- Etymology and semantic relation tables reserved for future work
- No collocation statistics in current release

### Recommended Practices

**For High-Quality Coptic Data**:
- Use Scriptorium collections with `source = 'scriptorium'`
- Filter to Sahidic dialect for largest, most consistent corpus
- Check `metadata` JSON field for edition and editor information

**For Egyptian Data**:
- AES texts may contain OCR errors - validate critical passages
- Morphological annotation is sparse - rely primarily on lemma and POS tags
- For hieroglyphs, filter to lemmas with TLA IDs: `source_id IS NOT NULL`

**General Best Practices**:
- Always check `provenance` field for source attribution and retrieval information
- Cross-reference uncertain readings with original sources
- Use `collection` field to compare similar text types
- Consult DATABASE.md for query examples and schema details

---

## Statistics

### Corpus Overview
- **Documents**: 1,552
- **Segments**: 252,826
- **Tokens**: 1,558,260
- **Languages**: Egyptian (egy), Coptic (cop), Greek (grc), Hebrew (he)

### Lexicon Coverage
- **Total Lemmas**: 33,259
  - Egyptian: 23,784
  - Coptic: 9,459 (153 corpus-attested + 9,306 from CDO dictionary)
  - Greek: 15
  - Hebrew: 1
- **Etymology Relations**: 1,750 Coptic→Egyptian derivations
- **Surface Forms**: 104,659
- **Attestations**: 103,908 (period/dialect-specific)

### Top Collections by Size
1. **AES Egyptian**: 815K tokens (101,796 segments)
2. **Ramses Late Egyptian**: 531K tokens (71,263 segments)
3. **TLA Egyptian**: 212K tokens (29,762 segments)
4. **Scriptorium Coptic**: 311 tokens (47,799 segments - partial tokenization)
5. **UD-Coptic**: 125 tokens (2,203 segments - partial tokenization)

---

## Citation

If you use KEMET Data in your research, please cite:

```bibtex
@dataset{tresoldi_kilani_2025_kemet,
  author = {Tresoldi, Tiago and Kilani, Marwan},
  title = {{KEMET Data: Annotated Corpus and Lexicon for
           Ancient Egyptian and Coptic}},
  year = {2025},
  publisher = {Zenodo},
  version = {0.1.0},
  doi = {10.5281/zenodo.XXXXXXX},
  url = {https://doi.org/10.5281/zenodo.XXXXXXX}
}
```

### Authors
- **Tiago Tresoldi** - Uppsala University
- **Marwan Kilani** - University of Basel

---

## License

This database is licensed under **Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0)**.

**You are free to**:
- Share and redistribute
- Adapt and build upon the material

**Under the following terms**:
- **Attribution**: Cite the authors
- **NonCommercial**: Not for commercial use
- **ShareAlike**: Derivative works must use the same license

Individual sources may have different licenses. See [LICENSE](LICENSE) for details and source-specific attributions.

---

## Reproducing the Database

The KEMET Data database is fully reproducible from source data.

### Recommended: Single Command Build

```bash
make build
```

This command:
1. Cleans all derived data (databases + curated files)
2. Downloads raw data from all enabled sources
3. Auto-curates all sources into normalized format
4. Builds fresh corpus and lexicon databases

This is the **recommended** approach for reproducibility and Zenodo releases.

### Available Make Targets

**Building:**
- `make build` - Build databases from scratch (download, curate, derive)
- `make curate` - Curate all enabled sources (for manual testing)

**Cleaning:**
- `make clean` - Remove derived databases only
- `make clean-curated` - Remove curated data only
- `make clean-all` - Remove all derived data (databases + curated)

**Quality & Testing:**
- `make test` - Run test suite
- `make validate` - Validate database integrity
- `make stats` - Display database statistics
- `make check` - Run linting and type checks

**Distribution:**
- `make zenodo` - Prepare Zenodo distribution package

**Development:**
- `make install` - Install Python dependencies
- `make install-dev` - Install development dependencies
- `make backup` - Create backup of databases

### Build Time

A full build from scratch takes approximately **45-50 minutes** on a modern system:
- Auto-curation: ~6-7 minutes (93 collections)
- Corpus database build: ~2 minutes
- Lexicon database build: ~37 minutes (forms and attestations are computationally intensive)

### Requirements for Building

- Python 3.10+
- DuckDB 0.8.0+
- Dependencies: `pip install -r requirements.txt`

---

## Contributing

We welcome feedback, bug reports, and suggestions for future enhancements. Please contact the authors:

- **Tiago Tresoldi**: [tiago.tresoldi@lingfil.uu.se](mailto:tiago.tresoldi@lingfil.uu.se)
- **Marwan Kilani**: [marwan.kilani@unibas.ch](mailto:marwan.kilani@unibas.ch)

For source code and build scripts, visit the project repository.

---

## Changelog

### Version 0.1.0 (2025-01-30)
- Initial public release
- Dual database architecture (corpus + lexicon)
- 8 data sources integrated (Ramses, Scriptorium, AES, TLA, UD-Coptic, Horner, CDO, ORAEC)
- 1.56M annotated tokens spanning Egyptian and Coptic
- TLA integration with 16,379 hieroglyphic segments and 29,762 German translations
- Coptic Dictionary Online: 9,306 lemmas imported
- Etymology relations: 1,750 Coptic→Egyptian derivations from ORAEC
- 33,259 total lemmas (23,784 Egyptian + 9,459 Coptic + 15 Greek + 1 Hebrew)
- Dynamic ingestor loading architecture
- Comprehensive documentation

---

## Acknowledgments

This work builds upon:
- **Ramses Online** (Université de Liège/Projet Ramsès)
- **Coptic SCRIPTORIUM** (Amir Zeldes et al.)
- **Thesaurus Linguae Aegyptiae** (Berlin-Brandenburgische Akademie der Wissenschaften)
- **Ancient Egyptian Online** (University of Texas at Austin)
- **Universal Dependencies** (Joakim Nivre et al.)
- **Coptic Dictionary Online** (KELLIA project)
- **ORAEC Coptic Etymologies** (digitization of Černý, Vycichl, Westendorf dictionaries)

We thank the digital humanities community for making their data openly available.

---

**For detailed schema documentation and query examples, see [DATABASE.md](DATABASE.md)**
