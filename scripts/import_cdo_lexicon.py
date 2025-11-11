"""
Import full CDO (Coptic Dictionary Online) lexicon into KEMET lexicon database.

This script:
1. Extracts all lemmas from alpha_kyima_rc1.db (CDO SQLite database)
2. Creates lemma entries in lexicon.duckdb with source='cdo'
3. Creates cdo_mappings table for CDO ID cross-references
4. Handles duplicates and validates data integrity

License: CDO data is CC BY-SA 4.0 licensed
"""

import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

import duckdb


def extract_primary_lemma(name_field: str) -> Optional[str]:
    """
    Extract the primary (first) lemma from CDO Name field markup.

    Reuses validated extraction logic from prototype.
    """
    if not name_field:
        return None

    lines = name_field.split('\n')

    for line in lines:
        line = line.strip()

        # Skip German/descriptor lines
        if not line or re.match(r'^[A-Za-zäöüÄÖÜß\s\.\-]+$', line):
            continue

        # Remove dialectal markers: ~S, ~B, ~A, ~L, ~F, ~K
        lemma = re.sub(r'~[SBKALFM]', '', line)

        # Remove internal reference codes: ^^CF123
        lemma = re.sub(r'\^\^CF\d+', '', lemma)

        # Remove other markup
        lemma = re.sub(r'~+', '', lemma)
        lemma = lemma.strip()

        # Check if we have valid Coptic text (Coptic Unicode block: U+2C80-U+2CFF)
        if re.search(r'[\u2C80-\u2CFF]', lemma):
            return lemma

    return None


def normalize_pos(cdo_pos: Optional[str]) -> Optional[str]:
    """
    Normalize CDO POS tags to KEMET conventions.

    CDO uses: N, V, A, ADV, PREP, etc.
    KEMET uses: NOUN, VERB, ADJ, ADV, ADP, etc.
    """
    if not cdo_pos:
        return None

    mapping = {
        'N': 'NOUN',
        'V': 'VERB',
        'A': 'ADJ',
        'ADV': 'ADV',
        'PREP': 'ADP',
        'PRON': 'PRON',
        'NUM': 'NUM',
        'PTC': 'PART',
        'NEG': 'PART',
        'CONJ': 'CCONJ',
    }

    return mapping.get(cdo_pos.upper(), cdo_pos)


def extract_first_gloss(gloss_field: Optional[str]) -> Optional[str]:
    """
    Extract first/primary English gloss from CDO gloss field.

    CDO glosses have format: "1|~~~gloss1;;;refs|||2|~~~gloss2;;;refs"
    We want just the first clean gloss.
    """
    if not gloss_field:
        return None

    # Split on |||
    parts = gloss_field.split('|||')
    if not parts:
        return None

    first = parts[0].strip()

    # Remove numbering and markup: "1|~~~gloss;;;refs"
    # Extract text between ~~~ and ;;;
    match = re.search(r'~~~([^;]+)', first)
    if match:
        gloss = match.group(1).strip()
        return gloss[:500]  # Limit length

    return None


def load_cdo_lexicon(db_path: Path) -> List[Dict]:
    """
    Load all CDO entries with cleaned data.

    Returns:
        List of dicts ready for database insertion
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
            De,
            Fr,
            Etym
        FROM entries
        WHERE xml_id LIKE 'C%'
        ORDER BY CAST(SUBSTR(xml_id, 2) AS INTEGER)
    """)

    entries = []
    skipped = 0

    for row in cursor.fetchall():
        lemma = extract_primary_lemma(row['Name'])

        if not lemma:
            skipped += 1
            continue

        # Extract clean glosses
        gloss_en = extract_first_gloss(row['En'])
        gloss_de = extract_first_gloss(row['De'])
        gloss_fr = extract_first_gloss(row['Fr'])

        entries.append({
            'cdo_id': row['xml_id'],
            'lemma': lemma,
            'pos': normalize_pos(row['POS']),
            'gloss_en': gloss_en,
            'gloss_de': gloss_de,
            'gloss_fr': gloss_fr,
            'etymology_notes': row['Etym'][:1000] if row['Etym'] else None
        })

    conn.close()

    print(f"  Extracted {len(entries)} lemmas ({skipped} skipped - no extractable lemma)")

    return entries


def create_cdo_mappings_table(conn: duckdb.DuckDBPyConnection):
    """Create table for CDO ID → lemma_id mappings."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cdo_mappings (
            cdo_id VARCHAR PRIMARY KEY,
            lemma_id VARCHAR NOT NULL REFERENCES lemmas(lemma_id),
            lemma VARCHAR NOT NULL,
            pos VARCHAR,
            confidence DECIMAL(3,2) DEFAULT 1.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_cdo_mappings_lemma ON cdo_mappings(lemma_id)")


def check_existing_lemmas(conn: duckdb.DuckDBPyConnection, lemmas: List[str]) -> Dict[str, str]:
    """
    Check which lemmas already exist in the database.

    Returns:
        Dict mapping lemma → existing lemma_id
    """
    placeholders = ','.join(['?' for _ in lemmas])

    result = conn.execute(f"""
        SELECT lemma, lemma_id
        FROM lemmas
        WHERE language = 'cop' AND lemma IN ({placeholders})
    """, lemmas).fetchall()

    return {lemma: lemma_id for lemma, lemma_id in result}


def import_cdo_lemmas(
    conn: duckdb.DuckDBPyConnection,
    cdo_entries: List[Dict]
) -> Dict:
    """
    Import CDO lemmas into lexicon database.

    Returns:
        Statistics dict
    """
    stats = {
        'total': len(cdo_entries),
        'inserted': 0,
        'existing': 0,
        'failed': 0
    }

    timestamp = datetime.utcnow().isoformat() + 'Z'

    # Check for existing lemmas
    all_lemmas = [e['lemma'] for e in cdo_entries]
    existing = check_existing_lemmas(conn, all_lemmas)

    print(f"  Found {len(existing)} lemmas already in database")

    for entry in cdo_entries:
        lemma = entry['lemma']
        cdo_id = entry['cdo_id']

        # Check if lemma already exists
        if lemma in existing:
            lemma_id = existing[lemma]
            stats['existing'] += 1

            # Just create CDO mapping
            try:
                conn.execute("""
                    INSERT INTO cdo_mappings (cdo_id, lemma_id, lemma, pos)
                    VALUES (?, ?, ?, ?)
                """, [cdo_id, lemma_id, lemma, entry['pos']])
            except Exception:
                # Mapping might already exist
                pass

            continue

        # Create new lemma entry
        lemma_id = f"cop:lemma:{lemma}"

        try:
            conn.execute("""
                INSERT INTO lemmas (
                    lemma_id, lemma, language, script, pos,
                    gloss_en, gloss_de, gloss_fr,
                    etymology_notes,
                    frequency, document_count, collection_count,
                    source, source_id,
                    confidence,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                lemma_id,
                lemma,
                'cop',
                'Coptic',
                entry['pos'],
                entry['gloss_en'],
                entry['gloss_de'],
                entry['gloss_fr'],
                entry['etymology_notes'],
                0,  # frequency - not attested in corpus
                0,  # document_count
                0,  # collection_count
                'cdo',
                cdo_id,
                1.0,  # confidence - high quality dictionary data
                timestamp,
                timestamp
            ])

            # Create CDO mapping
            conn.execute("""
                INSERT INTO cdo_mappings (cdo_id, lemma_id, lemma, pos)
                VALUES (?, ?, ?, ?)
            """, [cdo_id, lemma_id, lemma, entry['pos']])

            stats['inserted'] += 1

        except Exception as e:
            stats['failed'] += 1
            if stats['failed'] <= 10:  # Show first 10 errors
                print(f"    ERROR inserting {cdo_id} ({lemma}): {e}")

    return stats


def validate_import(conn: duckdb.DuckDBPyConnection):
    """Validate the CDO import with queries."""
    print("\n" + "="*60)
    print("VALIDATION QUERIES")
    print("="*60)

    # Total Coptic lemmas
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM lemmas
        WHERE language = 'cop'
    """).fetchone()
    print(f"Total Coptic lemmas:        {result[0]:>6}")

    # CDO-sourced lemmas
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM lemmas
        WHERE source = 'cdo'
    """).fetchone()
    print(f"CDO-sourced lemmas:         {result[0]:>6}")

    # Corpus-attested lemmas
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM lemmas
        WHERE language = 'cop' AND frequency > 0
    """).fetchone()
    print(f"Corpus-attested (freq>0):   {result[0]:>6}")

    # CDO mappings
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM cdo_mappings
    """).fetchone()
    print(f"CDO ID mappings:            {result[0]:>6}")

    # Test C1494 (ⲕⲁϩ)
    print("\n" + "="*60)
    print("TEST CASE: C1494 (ⲕⲁϩ)")
    print("="*60)

    result = conn.execute("""
        SELECT l.lemma_id, l.lemma, l.pos, l.gloss_en, l.source, l.frequency
        FROM cdo_mappings cm
        JOIN lemmas l ON cm.lemma_id = l.lemma_id
        WHERE cm.cdo_id = 'C1494'
    """).fetchone()

    if result:
        print("  ✓ FOUND!")
        print(f"    Lemma ID:   {result[0]}")
        print(f"    Lemma:      {result[1]}")
        print(f"    POS:        {result[2]}")
        print(f"    Gloss:      {result[3][:80] if result[3] else 'N/A'}")
        print(f"    Source:     {result[4]}")
        print(f"    Frequency:  {result[5]} (corpus attestation)")
    else:
        print("  ✗ NOT FOUND - Import may have failed")


def main():
    """Execute CDO lexicon import."""
    print("="*60)
    print("CDO LEXICON IMPORT")
    print("="*60)
    print("Importing Coptic Dictionary Online lexicon into KEMET")
    print("License: CC BY-SA 4.0")
    print("="*60)

    # Paths
    cdo_db = Path("/home/tiagot/kemet-data/data/raw/cdo/alpha_kyima_rc1.db")
    lexicon_db = Path("/home/tiagot/kemet-data/data/derived/lexicon.duckdb")

    if not cdo_db.exists():
        print(f"ERROR: CDO database not found at {cdo_db}")
        return 1

    if not lexicon_db.exists():
        print(f"ERROR: KEMET lexicon not found at {lexicon_db}")
        return 1

    # Step 1: Load CDO lexicon
    print("\n[1/4] Loading CDO lexicon...")
    cdo_entries = load_cdo_lexicon(cdo_db)
    print(f"  ✓ Loaded {len(cdo_entries)} CDO entries")

    # Step 2: Connect to lexicon database
    print("\n[2/4] Connecting to lexicon database...")
    conn = duckdb.connect(str(lexicon_db))
    print("  ✓ Connected")

    # Step 3: Create CDO mappings table
    print("\n[3/4] Creating CDO mappings table...")
    create_cdo_mappings_table(conn)
    print("  ✓ Table created")

    # Step 4: Import lemmas
    print("\n[4/4] Importing CDO lemmas...")
    stats = import_cdo_lemmas(conn, cdo_entries)

    print("\n" + "="*60)
    print("IMPORT STATISTICS")
    print("="*60)
    print(f"Total CDO entries:          {stats['total']:>6}")
    print(f"New lemmas inserted:        {stats['inserted']:>6}")
    print(f"Already existing:           {stats['existing']:>6}")
    print(f"Failed:                     {stats['failed']:>6}")

    # Validate
    validate_import(conn)

    conn.close()

    print("\n" + "="*60)
    print("✓ CDO LEXICON IMPORT COMPLETE")
    print("="*60)

    return 0


if __name__ == "__main__":
    exit(main())
