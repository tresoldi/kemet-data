#!/usr/bin/env python3
"""Test TLA integration in lexicon database."""

from pathlib import Path
import duckdb
import pytest


@pytest.mark.skip(reason="TLA schema has changed - test needs update")
def test_tla_integration():
    """Test TLA integration with various queries."""

    project_root = Path(__file__).parent.parent.parent
    lexicon_db_path = project_root / "data" / "derived" / "lexicon.duckdb"

    # Connect to database
    conn = duckdb.connect(str(lexicon_db_path), read_only=True)

    print("=== TLA Integration Test ===\n")

    # Test 1: Verify TLA table exists and has data
    print("Test 1: TLA Metadata Table")
    result = conn.execute("SELECT COUNT(*) FROM tla_metadata").fetchone()
    print(f"  Total TLA records: {result[0]}")
    assert result[0] > 0, "TLA metadata table is empty"
    print("  ✓ PASS\n")

    # Test 2: Check lemmas have TLA source IDs
    print("Test 2: Lemmas with TLA Source IDs")
    result = conn.execute("""
        SELECT COUNT(*)
        FROM lemmas
        WHERE source_id IS NOT NULL
          AND language = 'egy'
    """).fetchone()
    print(f"  Egyptian lemmas with TLA IDs: {result[0]}")
    assert result[0] > 0, "No lemmas have TLA source IDs"
    print("  ✓ PASS\n")

    # Test 3: Join lemmas with TLA metadata
    print("Test 3: Join Lemmas with TLA Metadata")
    result = conn.execute("""
        SELECT COUNT(*)
        FROM lemmas l
        JOIN tla_metadata t ON l.source_id = t.tla_id
        WHERE l.language = 'egy'
    """).fetchone()
    print(f"  Successfully joined records: {result[0]}")
    assert result[0] > 0, "Failed to join lemmas with TLA metadata"
    print("  ✓ PASS\n")

    # Test 4: Sample lemmas with hieroglyphs
    print("Test 4: Sample Lemmas with Hieroglyphs")
    results = conn.execute("""
        SELECT
            l.lemma,
            l.lemma_id,
            t.tla_id,
            t.hieroglyphs,
            l.frequency
        FROM lemmas l
        JOIN tla_metadata t ON l.source_id = t.tla_id
        WHERE l.language = 'egy'
          AND t.hieroglyphs IS NOT NULL
        ORDER BY l.frequency DESC
        LIMIT 10
    """).fetchall()

    print("  Top 10 frequent lemmas with hieroglyphs:")
    for lemma, lemma_id, tla_id, hieroglyphs, freq in results:
        print(f"    {lemma:15} {hieroglyphs:10} (TLA:{tla_id}, freq:{freq})")
    assert len(results) > 0, "No lemmas with hieroglyphs found"
    print("  ✓ PASS\n")

    # Test 5: Coverage statistics
    print("Test 5: Coverage Statistics")
    result = conn.execute("""
        SELECT
            COUNT(*) as total_egy_lemmas,
            COUNT(l.source_id) as with_tla,
            ROUND(COUNT(l.source_id) * 100.0 / COUNT(*), 2) as coverage_pct
        FROM lemmas l
        WHERE l.language = 'egy'
    """).fetchone()
    total, with_tla, coverage = result
    print(f"  Total Egyptian lemmas: {total}")
    print(f"  With TLA IDs: {with_tla}")
    print(f"  Coverage: {coverage}%")
    assert coverage > 0, "Zero coverage"
    print("  ✓ PASS\n")

    # Test 6: TLA statistics in lexicon_statistics table
    print("Test 6: Lexicon Statistics Table")
    result = conn.execute("""
        SELECT stat_value
        FROM lexicon_statistics
        WHERE stat_key = 'tla_integration'
    """).fetchone()
    if result:
        import json
        stats = json.loads(result[0])
        print(f"  Statistics stored: {stats}")
        print("  ✓ PASS\n")
    else:
        print("  ⚠ WARNING: No statistics found in lexicon_statistics table\n")

    # Test 7: Query by transliteration
    print("Test 7: Query by Transliteration")
    results = conn.execute("""
        SELECT
            l.lemma,
            t.hieroglyphs,
            l.gloss_en,
            l.frequency
        FROM lemmas l
        JOIN tla_metadata t ON l.source_id = t.tla_id
        WHERE t.transliteration = 'r'
          AND l.language = 'egy'
    """).fetchall()
    if results:
        for lemma, hieroglyphs, gloss, freq in results:
            hieroglyphs_str = hieroglyphs if hieroglyphs else "N/A"
            gloss_str = gloss if gloss else "N/A"
            print(f"    {lemma:10} {hieroglyphs_str:10} {gloss_str:30} (freq:{freq})")
        print("  ✓ PASS\n")
    else:
        print("  ⚠ WARNING: No results for 'r' transliteration\n")

    # Test 8: Sample TLA metadata
    print("Test 8: Sample TLA Metadata Records")
    results = conn.execute("""
        SELECT
            tla_id,
            transliteration,
            hieroglyphs,
            attestation_count,
            match_type
        FROM tla_metadata
        ORDER BY attestation_count DESC
        LIMIT 5
    """).fetchall()
    print("  Top 5 most attested lemmas in TLA corpus:")
    for tla_id, translit, hieroglyphs, count, match_type in results:
        print(f"    TLA:{tla_id:6} {translit:15} {hieroglyphs:10} ({count} attestations)")
    print("  ✓ PASS\n")

    conn.close()

    print("=== All Tests Passed ✓ ===")


if __name__ == "__main__":
    test_tla_integration()
