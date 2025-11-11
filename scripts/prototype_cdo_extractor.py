"""
Prototype script to extract CDO ID → lemma mappings from alpha_kyima_rc1.db

This script:
1. Connects to the CDO SQLite database
2. Extracts clean lemma forms from the complex Name field markup
3. Queries KEMET lexicon for matching Coptic lemmas
4. Generates mapping statistics
"""

import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb


def extract_primary_lemma(name_field: str) -> Optional[str]:
    """
    Extract the primary (first) lemma from CDO Name field markup.

    Example inputs:
        "Subst. m.\nⲕⲁϩ~^^CF4416\n"
        "Verbalpräfix\nⲁ-~S^^CF4\n|||Verbalpräfix\nⲁ-~B^^CF5\n"

    Strategy:
        1. Split on newlines and |||
        2. Find first line with Coptic text
        3. Remove dialectal markers (~S, ~B, etc.)
        4. Remove internal codes (^^CF...)
        5. Return normalized Coptic Unicode

    Args:
        name_field: Raw Name field from CDO database

    Returns:
        Cleaned primary lemma or None if extraction fails
    """
    if not name_field:
        return None

    # Split into lines
    lines = name_field.split('\n')

    for line in lines:
        line = line.strip()

        # Skip German/descriptor lines (contain only Latin chars, spaces, dots)
        if not line or re.match(r'^[A-Za-zäöüÄÖÜß\s\.\-]+$', line):
            continue

        # Found a line with Coptic text
        # Remove dialectal markers: ~S, ~B, ~A, ~L, ~F, ~K (Sahidic, Bohairic, etc.)
        lemma = re.sub(r'~[SBKALFM]', '', line)

        # Remove internal reference codes: ^^CF123
        lemma = re.sub(r'\^\^CF\d+', '', lemma)

        # Remove other markup
        lemma = re.sub(r'~+', '', lemma)  # Remove remaining tildes
        lemma = lemma.strip()

        # Check if we have valid Coptic text (contains Coptic Unicode)
        # Coptic Unicode block: U+2C80 to U+2CFF
        if re.search(r'[\u2C80-\u2CFF]', lemma):
            return lemma

    return None


def load_cdo_entries(db_path: Path) -> List[Dict]:
    """
    Load all CDO entries with their IDs, lemmas, POS, and glosses.

    Args:
        db_path: Path to alpha_kyima_rc1.db

    Returns:
        List of dicts with CDO entry data
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            xml_id,
            Name,
            POS,
            En,
            De
        FROM entries
        WHERE xml_id LIKE 'C%'
        ORDER BY CAST(SUBSTR(xml_id, 2) AS INTEGER)
    """)

    entries = []
    for row in cursor.fetchall():
        lemma = extract_primary_lemma(row['Name'])
        if lemma:
            entries.append({
                'cdo_id': row['xml_id'],
                'lemma': lemma,
                'pos': row['POS'],
                'gloss_en': row['En'],
                'gloss_de': row['De']
            })

    conn.close()
    return entries


def load_kemet_coptic_lemmas(lexicon_db_path: Path) -> Dict[str, str]:
    """
    Load all Coptic lemmas from KEMET lexicon database.

    Args:
        lexicon_db_path: Path to lexicon.duckdb

    Returns:
        Dict mapping normalized lemma → lemma_id
    """
    conn = duckdb.connect(str(lexicon_db_path), read_only=True)

    result = conn.execute("""
        SELECT lemma_id, lemma
        FROM lemmas
        WHERE language = 'cop'
    """).fetchall()

    conn.close()

    # Create mapping of normalized lemma to lemma_id
    mapping = {}
    for lemma_id, lemma in result:
        # Normalize: lowercase, strip
        normalized = lemma.lower().strip()
        mapping[normalized] = lemma_id

    return mapping


def match_cdo_to_kemet(
    cdo_entries: List[Dict],
    kemet_mapping: Dict[str, str]
) -> Tuple[List[Dict], Dict]:
    """
    Match CDO entries to KEMET lemmas.

    Args:
        cdo_entries: List of CDO entry dicts
        kemet_mapping: Dict mapping normalized lemma → lemma_id

    Returns:
        Tuple of (matched_entries, statistics)
    """
    matched = []
    stats = {
        'total_cdo': len(cdo_entries),
        'matched': 0,
        'unmatched': 0,
        'multiple_matches': 0
    }

    for entry in cdo_entries:
        normalized = entry['lemma'].lower().strip()

        if normalized in kemet_mapping:
            matched.append({
                **entry,
                'lemma_id': kemet_mapping[normalized],
                'confidence': 1.0
            })
            stats['matched'] += 1
        else:
            stats['unmatched'] += 1

    return matched, stats


def main():
    """Run CDO extraction and matching prototype."""
    print("CDO → KEMET Lemma Matching Prototype")
    print("=" * 60)

    # Paths
    cdo_db = Path("/home/tiagot/kemet-data/data/raw/cdo/alpha_kyima_rc1.db")
    lexicon_db = Path("/home/tiagot/kemet-data/data/derived/lexicon.duckdb")

    if not cdo_db.exists():
        print(f"ERROR: CDO database not found at {cdo_db}")
        return

    if not lexicon_db.exists():
        print(f"ERROR: KEMET lexicon not found at {lexicon_db}")
        return

    # Step 1: Load CDO entries
    print("\n[1/3] Loading CDO entries...")
    cdo_entries = load_cdo_entries(cdo_db)
    print(f"  ✓ Loaded {len(cdo_entries)} CDO entries with extractable lemmas")

    # Sample output
    print("\n  Sample CDO entries:")
    for entry in cdo_entries[:5]:
        print(f"    {entry['cdo_id']}: {entry['lemma']} ({entry['pos']}) - {entry['gloss_en'][:50] if entry['gloss_en'] else 'N/A'}...")

    # Step 2: Load KEMET Coptic lemmas
    print("\n[2/3] Loading KEMET Coptic lemmas...")
    kemet_mapping = load_kemet_coptic_lemmas(lexicon_db)
    print(f"  ✓ Loaded {len(kemet_mapping)} unique Coptic lemmas from KEMET")

    # Step 3: Match
    print("\n[3/3] Matching CDO → KEMET...")
    matched, stats = match_cdo_to_kemet(cdo_entries, kemet_mapping)

    # Statistics
    print("\n" + "=" * 60)
    print("MATCHING STATISTICS")
    print("=" * 60)
    print(f"Total CDO entries:        {stats['total_cdo']:>6}")
    print(f"Matched to KEMET:         {stats['matched']:>6} ({stats['matched']/stats['total_cdo']*100:.1f}%)")
    print(f"Unmatched:                {stats['unmatched']:>6} ({stats['unmatched']/stats['total_cdo']*100:.1f}%)")

    # Sample matched entries
    print("\n" + "=" * 60)
    print("SAMPLE MATCHED ENTRIES (first 20)")
    print("=" * 60)
    for entry in matched[:20]:
        print(f"{entry['cdo_id']}: {entry['lemma']:<15} → {entry['lemma_id']}")

    # Check specific test case (C1494 = ⲕⲁϩ from etymology example)
    print("\n" + "=" * 60)
    print("TEST CASE: C1494 (ⲕⲁϩ - earth/land)")
    print("=" * 60)
    c1494 = next((e for e in matched if e['cdo_id'] == 'C1494'), None)
    if c1494:
        print("  ✓ MATCHED!")
        print(f"    CDO ID:    {c1494['cdo_id']}")
        print(f"    Lemma:     {c1494['lemma']}")
        print(f"    KEMET ID:  {c1494['lemma_id']}")
        print(f"    POS:       {c1494['pos']}")
        print(f"    Gloss:     {c1494['gloss_en'][:80] if c1494['gloss_en'] else 'N/A'}")
    else:
        c1494_raw = next((e for e in cdo_entries if e['cdo_id'] == 'C1494'), None)
        print("  ✗ NOT MATCHED")
        if c1494_raw:
            print(f"    CDO ID:    {c1494_raw['cdo_id']}")
            print(f"    Lemma:     {c1494_raw['lemma']}")
            print(f"    Gloss:     {c1494_raw['gloss_en'][:80] if c1494_raw['gloss_en'] else 'N/A'}")

    print("\n" + "=" * 60)
    print("Prototype complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
