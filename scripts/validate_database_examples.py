#!/usr/bin/env python3
"""
Validate all example query outputs in DATABASE.md against actual database results.
"""

import duckdb
from pathlib import Path

def test_query(db_path, query, description):
    """Execute a query and print results."""
    print(f"\n{'='*70}")
    print(f"TEST: {description}")
    print(f"{'='*70}")
    print(f"Query: {query[:100]}...")
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        result = conn.execute(query).fetchall()
        conn.close()
        print(f"Result ({len(result)} rows):")
        for row in result[:10]:  # Show first 10 rows
            print(f"  {row}")
        return result
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def main():
    corpus_db = Path("data/derived/corpus.duckdb")
    lexicon_db = Path("data/derived/lexicon.duckdb")

    print("="*70)
    print("DATABASE.md EXAMPLE QUERY VALIDATION")
    print("="*70)

    # Corpus Database Tests
    print("\n" + "="*70)
    print("CORPUS DATABASE TESTS")
    print("="*70)

    test_query(corpus_db,
        "SELECT document_id, source, collection, substage, title, num_tokens FROM documents LIMIT 3",
        "Documents table example")

    test_query(corpus_db,
        "SELECT * FROM corpus_statistics",
        "Corpus statistics table")

    # Lexicon Database Tests
    print("\n" + "="*70)
    print("LEXICON DATABASE TESTS")
    print("="*70)

    test_query(lexicon_db,
        "SELECT * FROM lexicon_statistics",
        "Lexicon statistics table")

    test_query(lexicon_db,
        "SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE source = 'cdo') as cdo_only FROM lemmas WHERE language = 'cop'",
        "Coptic lemma counts")

    test_query(lexicon_db,
        "SELECT lemma_id, lemma, language, pos, gloss_en, frequency FROM lemmas WHERE frequency > 10000 ORDER BY frequency DESC LIMIT 5",
        "Top 5 lemmas by frequency")

    test_query(lexicon_db,
        "SELECT COUNT(*) FROM etymology_relations",
        "Etymology relations count")

    test_query(lexicon_db,
        "SELECT COUNT(*) FROM cdo_mappings",
        "CDO mappings count")

    # Cross-database test
    print("\n" + "="*70)
    print("CROSS-DATABASE TESTS")
    print("="*70)

    conn = duckdb.connect(str(corpus_db), read_only=True)
    conn.execute(f"ATTACH '{lexicon_db}' AS lexicon")
    result = conn.execute("SELECT COUNT(*) FROM main.token_instances").fetchone()
    print(f"\nToken instances count: {result[0]:,}")
    conn.close()

if __name__ == "__main__":
    main()
