#!/usr/bin/env python3
"""
KEMET Data Query Cookbook

A collection of common query patterns for working with the KEMET Data corpus
and lexicon databases. Each function demonstrates a specific use case and can
be used as a template for your own queries.

Usage:
    python cookbook.py                    # Run all example queries
    python -c "import cookbook; cookbook.corpus_statistics()"  # Run specific query

Requirements:
    - DuckDB Python library: pip install duckdb
    - Run from the KEMET Data project root directory
    - Databases must exist at: data/derived/corpus.duckdb and data/derived/lexicon.duckdb

See DATABASE.md for complete schema documentation and additional query examples.
"""

import duckdb
from pathlib import Path


# ============================================================================
# CONFIGURATION
# ============================================================================

# Database paths (relative to project root)
CORPUS_DB = "data/derived/corpus.duckdb"
LEXICON_DB = "data/derived/lexicon.duckdb"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def print_header(title, subtitle=None):
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(title)
    if subtitle:
        print(subtitle)
    print("=" * 70)


def check_databases():
    """Verify that database files exist."""
    corpus_path = Path(CORPUS_DB)
    lexicon_path = Path(LEXICON_DB)

    if not corpus_path.exists():
        raise FileNotFoundError(
            f"Corpus database not found at {CORPUS_DB}\n"
            "Please run this script from the KEMET Data project root directory."
        )

    if not lexicon_path.exists():
        raise FileNotFoundError(
            f"Lexicon database not found at {LEXICON_DB}\n"
            "Please run this script from the KEMET Data project root directory."
        )


# ============================================================================
# CORPUS DATABASE QUERIES
# ============================================================================

def corpus_statistics():
    """Get overall corpus statistics."""
    print_header(
        "CORPUS STATISTICS",
        "Overview of documents, segments, and tokens"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)

    result = conn.execute("""
        SELECT
            COUNT(DISTINCT document_id) as documents,
            COUNT(DISTINCT segment_id) as segments,
            COUNT(*) as tokens
        FROM token_instances
    """).fetchall()

    print("\nOverall Statistics:")
    print(result)

    conn.close()


def find_sahidic_documents():
    """Find all Sahidic Coptic documents."""
    print_header(
        "SAHIDIC COPTIC DOCUMENTS",
        "Documents in the Sahidic dialect"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)

    result = conn.execute("""
        SELECT
            document_id,
            title,
            authors,
            date_from,
            date_to,
            collection
        FROM documents
        WHERE substage = 'SAHIDIC'
        ORDER BY date_from
        LIMIT 10
    """).fetchall()

    print("\nFirst 10 Sahidic documents:")
    print(result)

    conn.close()


def concordance_search():
    """Search for a word in context (KWIC - Key Word In Context)."""
    print_header(
        "CONCORDANCE SEARCH",
        "Find 'ⲛⲟⲩⲧⲉ' (god) in context"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)

    result = conn.execute("""
        SELECT
            t.form,
            t.lemma_id,
            s.text_canonical as context,
            d.title,
            s.passage_ref
        FROM token_instances t
        JOIN segments s ON t.segment_id = s.segment_id
        JOIN documents d ON t.document_id = d.document_id
        WHERE t.lemma_id = 'cop:lemma:ⲛⲟⲩⲧⲉ'
        ORDER BY d.document_id, s."order", t."order"
        LIMIT 10
    """).fetchall()

    print("\nFirst 10 occurrences of 'ⲛⲟⲩⲧⲉ' (god):")
    print(result)

    conn.close()


def pos_distribution():
    """Analyze part-of-speech distribution via cross-database query."""
    print_header(
        "PART-OF-SPEECH DISTRIBUTION",
        "Token counts by POS tag (via lemma join)"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)
    conn.execute(f"ATTACH '{LEXICON_DB}' AS lexicon")

    result = conn.execute("""
        SELECT
            l.pos,
            COUNT(*) as token_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM main.token_instances t
        JOIN lexicon.lemmas l ON t.lemma_id = l.lemma_id
        WHERE l.pos IS NOT NULL
        GROUP BY l.pos
        ORDER BY token_count DESC
        LIMIT 15
    """).fetchall()

    print("\nTop 15 POS tags:")
    print(result)

    conn.close()


def collection_statistics():
    """Get token counts by collection."""
    print_header(
        "COLLECTION STATISTICS",
        "Token counts per collection"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)

    result = conn.execute("""
        SELECT
            d.collection,
            COUNT(DISTINCT d.document_id) as document_count,
            COUNT(DISTINCT t.segment_id) as segment_count,
            COUNT(t.token_id) as token_count
        FROM documents d
        JOIN token_instances t ON d.document_id = t.document_id
        GROUP BY d.collection
        ORDER BY token_count DESC
        LIMIT 15
    """).fetchall()

    print("\nTop 15 collections by size:")
    print(result)

    conn.close()


def hieroglyphic_segments():
    """Access segments with hieroglyphic data from TLA."""
    print_header(
        "HIEROGLYPHIC SEGMENTS",
        "Segments with Unicode hieroglyphs and German translations"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)

    result = conn.execute("""
        SELECT
            segment_id,
            text_canonical,
            text_hieroglyphs,
            text_de as german_translation,
            document_id
        FROM segments
        WHERE text_hieroglyphs IS NOT NULL
        LIMIT 5
    """).fetchall()

    print("\nFirst 5 segments with hieroglyphs:")
    print(result)

    conn.close()


def dependency_parsing():
    """Find subject-verb relations using dependency parsing."""
    print_header(
        "DEPENDENCY PARSING",
        "Subject-verb relations (nsubj)"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)

    result = conn.execute("""
        SELECT
            t1.form as subject,
            t2.form as verb,
            s.text_canonical as sentence,
            s.passage_ref
        FROM token_instances t1
        JOIN token_instances t2 ON t1.segment_id = t2.segment_id AND t1.head = t2.order
        JOIN segments s ON t1.segment_id = s.segment_id
        WHERE t1.deprel = 'nsubj'
        LIMIT 10
    """).fetchall()

    print("\nFirst 10 subject-verb pairs:")
    print(result)

    conn.close()


def morphological_features():
    """Analyze morphological features from token metadata."""
    print_header(
        "MORPHOLOGICAL FEATURES",
        "Distribution of verb POS tags (V, VBD, VSTAT)"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)
    conn.execute(f"ATTACH '{LEXICON_DB}' AS lexicon")

    result = conn.execute("""
        SELECT
            l.pos,
            COUNT(*) as token_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM main.token_instances t
        JOIN lexicon.lemmas l ON t.lemma_id = l.lemma_id
        WHERE l.pos IN ('V', 'VBD', 'VSTAT', 'VERB')
        GROUP BY l.pos
        ORDER BY token_count DESC
    """).fetchall()

    print("\nVerb POS tag distribution:")
    print(result)

    conn.close()


# ============================================================================
# LEXICON DATABASE QUERIES
# ============================================================================

def dictionary_lookup():
    """Look up Coptic and Egyptian lemmas."""
    print_header(
        "DICTIONARY LOOKUP",
        "Look up specific lemmas"
    )

    conn = duckdb.connect(LEXICON_DB, read_only=True)

    # Coptic lemma
    print("\nCoptic lemma 'ⲁⲛⲟⲕ' (I, me):")
    result = conn.execute("""
        SELECT
            lemma,
            pos,
            gloss_en,
            frequency,
            document_count,
            sahidic_form,
            bohairic_form
        FROM lemmas
        WHERE lemma = 'ⲁⲛⲟⲕ'
    """).fetchall()
    print(result)

    # Egyptian lemma
    print("\nEgyptian lemma 'nḏ' (protect):")
    result = conn.execute("""
        SELECT
            lemma,
            pos,
            gloss_en,
            frequency,
            hieroglyphic_writing,
            transliteration
        FROM lemmas
        WHERE lemma = 'nḏ'
        AND language = 'egy'
    """).fetchall()
    print(result)

    conn.close()


def frequency_lists():
    """Get frequency-ranked lemma lists."""
    print_header(
        "FREQUENCY LISTS",
        "Most frequent lemmas overall and by category"
    )

    conn = duckdb.connect(LEXICON_DB, read_only=True)

    # Overall top lemmas
    print("\nTop 20 most frequent lemmas:")
    result = conn.execute("""
        SELECT
            lemma,
            language,
            pos,
            gloss_en,
            frequency,
            document_count
        FROM lemmas
        ORDER BY frequency DESC
        LIMIT 20
    """).fetchall()
    print(result)

    # Top Coptic verbs
    print("\nTop 10 Coptic verbs:")
    result = conn.execute("""
        SELECT
            lemma,
            gloss_en,
            frequency,
            sahidic_form,
            bohairic_form
        FROM lemmas
        WHERE language = 'cop' AND pos = 'VERB'
        ORDER BY frequency DESC
        LIMIT 10
    """).fetchall()
    print(result)

    conn.close()


def form_to_lemma_mapping():
    """Find all possible lemmas for a surface form."""
    print_header(
        "FORM-TO-LEMMA MAPPING",
        "Find lemmas for surface form 'ⲡ'"
    )

    conn = duckdb.connect(LEXICON_DB, read_only=True)

    result = conn.execute("""
        SELECT DISTINCT
            l.lemma_id,
            l.lemma,
            l.pos,
            l.gloss_en,
            f.morphology,
            f.frequency as form_frequency
        FROM forms f
        JOIN lemmas l ON f.lemma_id = l.lemma_id
        WHERE f.form = 'ⲡ'
        ORDER BY f.frequency DESC
    """).fetchall()

    print("\nPossible lemmas for 'ⲡ':")
    print(result)

    conn.close()


def dialectal_variation():
    """Find lemmas attested across multiple collections."""
    print_header(
        "COLLECTION DIVERSITY",
        "Lemmas attested in multiple collections"
    )

    conn = duckdb.connect(LEXICON_DB, read_only=True)

    result = conn.execute("""
        SELECT
            l.lemma,
            l.pos,
            l.gloss_en,
            COUNT(DISTINCT la.dimension_value) as collection_count,
            l.frequency
        FROM lemmas l
        JOIN lemma_attestations la ON l.lemma_id = la.lemma_id
        WHERE la.dimension_type = 'COLLECTION'
            AND l.language = 'cop'
        GROUP BY l.lemma_id, l.lemma, l.pos, l.gloss_en, l.frequency
        HAVING COUNT(DISTINCT la.dimension_value) > 1
        ORDER BY collection_count DESC, l.frequency DESC
        LIMIT 15
    """).fetchall()

    print("\nTop 15 lemmas by collection diversity:")
    print(result)

    conn.close()


def attestation_analysis():
    """Analyze collection distribution of a lemma."""
    print_header(
        "ATTESTATION ANALYSIS",
        "Collection distribution of 'ⲡ' (definite article)"
    )

    conn = duckdb.connect(LEXICON_DB, read_only=True)

    result = conn.execute("""
        SELECT
            dimension_value as collection,
            frequency,
            document_count
        FROM lemma_attestations
        WHERE lemma_id = 'cop:lemma:ⲡ'
            AND dimension_type = 'COLLECTION'
        ORDER BY frequency DESC
    """).fetchall()

    print("\nCollection distribution:")
    print(result)

    conn.close()


def morphological_diversity():
    """Find lemmas with the most morphological forms."""
    print_header(
        "MORPHOLOGICAL DIVERSITY",
        "Lemmas with the most attested surface forms"
    )

    conn = duckdb.connect(LEXICON_DB, read_only=True)

    result = conn.execute("""
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
        LIMIT 15
    """).fetchall()

    print("\nTop 15 most morphologically diverse lemmas:")
    print(result)

    conn.close()


def etymology_lookup():
    """Look up Coptic etymologies from Egyptian."""
    print_header(
        "ETYMOLOGY LOOKUP",
        "Coptic lemma etymologies"
    )

    conn = duckdb.connect(LEXICON_DB, read_only=True)

    # Specific etymology
    print("\nEtymology of 'ⲕⲁϩ' (earth):")
    result = conn.execute("""
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
        WHERE lc.lemma = 'ⲕⲁϩ'
    """).fetchall()
    print(result)

    # Top lemmas with etymologies
    print("\nTop 10 frequent Coptic lemmas with etymology data:")
    result = conn.execute("""
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
        LIMIT 10
    """).fetchall()
    print(result)

    conn.close()


def cdo_cross_reference():
    """Look up Coptic Dictionary Online (CDO) cross-references."""
    print_header(
        "CDO CROSS-REFERENCE",
        "Coptic Dictionary Online ID mappings"
    )

    conn = duckdb.connect(LEXICON_DB, read_only=True)

    # Specific CDO lookup
    print("\nCDO ID 'C1494' lookup:")
    result = conn.execute("""
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
        WHERE cdo.cdo_id = 'C1494'
    """).fetchall()
    print(result)

    # CDO statistics
    print("\nCDO mapping statistics:")
    result = conn.execute("""
        SELECT
            COUNT(DISTINCT cdo_id) as total_cdo_ids,
            COUNT(DISTINCT lemma_id) as total_lemmas
        FROM cdo_mappings
    """).fetchall()
    print(result)

    conn.close()


def tla_integration():
    """Query TLA (Thesaurus Linguae Aegyptiae) integrated data."""
    print_header(
        "TLA INTEGRATION",
        "Egyptian lemmas with hieroglyphic writing"
    )

    conn = duckdb.connect(LEXICON_DB, read_only=True)

    # Top Egyptian lemmas
    print("\nTop 10 Egyptian lemmas:")
    result = conn.execute("""
        SELECT
            lemma,
            pos,
            gloss_en,
            frequency,
            hieroglyphic_writing,
            transliteration
        FROM lemmas_egyptian
        ORDER BY frequency DESC
        LIMIT 10
    """).fetchall()
    print(result)

    # Hieroglyph coverage
    print("\nHieroglyphic writing coverage:")
    result = conn.execute("""
        SELECT
            COUNT(*) as total_egyptian_lemmas,
            COUNT(CASE WHEN hieroglyphic_writing IS NOT NULL THEN 1 END) as lemmas_with_hieroglyphs,
            ROUND(100.0 * COUNT(CASE WHEN hieroglyphic_writing IS NOT NULL THEN 1 END) / COUNT(*), 2) as coverage_percent
        FROM lemmas
        WHERE language = 'egy'
    """).fetchall()
    print(result)

    conn.close()


# ============================================================================
# CROSS-DATABASE QUERIES
# ============================================================================

def token_with_full_lemma_info():
    """Get tokens with complete lexical information from both databases."""
    print_header(
        "CROSS-DATABASE QUERY",
        "Tokens with full lemma information"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)
    conn.execute(f"ATTACH '{LEXICON_DB}' AS lexicon")

    result = conn.execute("""
        SELECT
            t.form,
            l.lemma,
            l.pos,
            l.gloss_en,
            l.frequency as lemma_frequency,
            SUBSTR(s.text_canonical, 1, 60) as sentence_snippet
        FROM main.token_instances t
        JOIN main.segments s ON t.segment_id = s.segment_id
        LEFT JOIN lexicon.lemmas l ON t.lemma_id = l.lemma_id
        WHERE t.segment_id = 'IBUBdWSGVy3puE0LmeTTX99EzSo'
        ORDER BY t.order
        LIMIT 10
    """).fetchall()

    print("\nFirst 10 tokens with lemma data:")
    print(result)

    conn.close()


def lemma_frequency_validation():
    """Validate lemma frequencies between corpus and lexicon."""
    print_header(
        "FREQUENCY VALIDATION",
        "Top 10 most frequent lemmas with corpus counts"
    )

    conn = duckdb.connect(CORPUS_DB, read_only=True)
    conn.execute(f"ATTACH '{LEXICON_DB}' AS lexicon")

    result = conn.execute("""
        SELECT
            l.lemma,
            l.language,
            l.frequency as lexicon_frequency,
            COUNT(*) as corpus_count,
            l.frequency - COUNT(*) as difference
        FROM main.token_instances t
        JOIN lexicon.lemmas l ON t.lemma_id = l.lemma_id
        GROUP BY l.lemma_id, l.lemma, l.language, l.frequency
        ORDER BY l.frequency DESC
        LIMIT 10
    """).fetchall()

    print("\nFrequency comparison (lexicon vs corpus):")
    print(result)

    conn.close()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Run all example queries."""
    print_header(
        "KEMET DATA QUERY COOKBOOK",
        "Demonstrating common query patterns"
    )

    print("\nChecking database availability...")
    check_databases()
    print("✓ Databases found")

    # Corpus queries
    corpus_statistics()
    find_sahidic_documents()
    concordance_search()
    pos_distribution()
    collection_statistics()
    hieroglyphic_segments()
    dependency_parsing()
    morphological_features()

    # Lexicon queries
    dictionary_lookup()
    frequency_lists()
    form_to_lemma_mapping()
    dialectal_variation()
    attestation_analysis()
    morphological_diversity()
    etymology_lookup()
    cdo_cross_reference()
    tla_integration()

    # Cross-database queries
    token_with_full_lemma_info()
    lemma_frequency_validation()

    print_header("COOKBOOK COMPLETE")
    print("\nAll example queries executed successfully!")
    print("See DATABASE.md for complete schema documentation.")
    print("\nTip: Import this module to use individual query functions:")
    print("  python -c \"import cookbook; cookbook.corpus_statistics()\"")


if __name__ == "__main__":
    main()
