"""
Import Coptic Etymologies from ORAEC digitized dictionary data.

This script:
1. Loads ORAEC etymology CSV (CDO_ID, ORAEC_ID, TLA_ID)
2. Resolves CDO IDs → Coptic lemma_ids via cdo_mappings table
3. Resolves TLA/ORAEC IDs → Egyptian lemmas via fuzzy string matching
4. Creates etymology_relations entries
5. Validates and reports statistics

Data source: https://github.com/oraec/coptic_etymologies
License: CC0 (public domain)
"""

import csv
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime

import duckdb


def load_etymology_csv(csv_path: Path) -> List[Dict]:
    """
    Load ORAEC etymology CSV.

    Format: CDO_ID,ORAEC_ID,TLA_ID
    Example: C1494,159410,6439

    Returns:
        List of etymology relationship dicts
    """
    etymologies = []

    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or len(row) < 3:
                continue

            cdo_id = row[0].strip() if row[0] else None
            oraec_id = row[1].strip() if len(row) > 1 and row[1] else None
            tla_id = row[2].strip() if len(row) > 2 and row[2] else None

            if cdo_id:  # Must have Coptic side
                etymologies.append({
                    'cdo_id': cdo_id,
                    'oraec_id': oraec_id,
                    'tla_id': tla_id
                })

    print(f"  Loaded {len(etymologies)} etymology relationships")
    return etymologies


def resolve_cdo_ids(
    conn: duckdb.DuckDBPyConnection,
    cdo_ids: List[str]
) -> Dict[str, str]:
    """
    Resolve CDO IDs to Coptic lemma_ids via cdo_mappings table.

    Returns:
        Dict mapping CDO ID → lemma_id
    """
    placeholders = ','.join(['?' for _ in cdo_ids])

    result = conn.execute(f"""
        SELECT cdo_id, lemma_id
        FROM cdo_mappings
        WHERE cdo_id IN ({placeholders})
    """, cdo_ids).fetchall()

    mapping = {cdo_id: lemma_id for cdo_id, lemma_id in result}
    print(f"  Resolved {len(mapping)}/{len(cdo_ids)} CDO IDs to Coptic lemmas")

    return mapping


def load_egyptian_lemmas(conn: duckdb.DuckDBPyConnection) -> Dict[str, str]:
    """
    Load all Egyptian lemmas from KEMET lexicon.

    Returns:
        Dict mapping normalized lemma → lemma_id
    """
    result = conn.execute("""
        SELECT lemma_id, lemma
        FROM lemmas
        WHERE language = 'egy'
    """).fetchall()

    mapping = {}
    for lemma_id, lemma in result:
        # Normalize: lowercase, strip whitespace
        normalized = lemma.lower().strip()
        mapping[normalized] = lemma_id

    print(f"  Loaded {len(mapping)} Egyptian lemmas from KEMET")
    return mapping


def create_tables(conn: duckdb.DuckDBPyConnection):
    """Create etymology_relations and oraec_mappings tables."""

    # Create etymology_relations table (from schema)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etymology_relations (
            relation_id VARCHAR PRIMARY KEY,
            source_lemma_id VARCHAR NOT NULL,
            target_lemma_id VARCHAR,  -- NULL allowed for unresolved
            relation_type VARCHAR NOT NULL,
            confidence DECIMAL(3,2),
            approximate_date INTEGER,
            date_range_from INTEGER,
            date_range_to INTEGER,
            evidence TEXT,
            "references" VARCHAR[],  -- Quoted because 'references' is reserved
            phonological_change VARCHAR,
            metadata JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_lemma_id, target_lemma_id, relation_type)
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_etymology_source ON etymology_relations(source_lemma_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_etymology_target ON etymology_relations(target_lemma_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_etymology_type ON etymology_relations(relation_type)")

    # Create ORAEC mappings table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oraec_mappings (
            external_id VARCHAR PRIMARY KEY,
            id_type VARCHAR NOT NULL,  -- 'oraec' or 'tla'
            lemma_id VARCHAR NOT NULL,
            lemma VARCHAR NOT NULL,
            confidence DECIMAL(3,2) DEFAULT 1.0,
            match_method VARCHAR,  -- 'exact', 'normalized', 'fuzzy'
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_oraec_mappings_lemma ON oraec_mappings(lemma_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_oraec_mappings_type ON oraec_mappings(id_type)")


def resolve_egyptian_ids_fuzzy(
    etymologies: List[Dict],
    egyptian_lemmas: Dict[str, str],
    cdo_resolved: Dict[str, str]
) -> Tuple[Dict[str, str], Dict]:
    """
    Attempt fuzzy matching for Egyptian IDs using Coptic-Egyptian lemma patterns.

    Since we don't have direct ORAEC/TLA ID → lemma mappings, we try:
    1. Look for Egyptian lemmas that are phonetically similar to resolved Coptic lemmas
    2. Use common Coptic←Egyptian sound correspondences

    This is a HEURISTIC approach with lower confidence.

    Returns:
        (egyptian_id_mapping, stats)
    """
    egyptian_mapping = {}
    stats = {
        'total_egyptian_ids': 0,
        'resolved': 0,
        'unresolved': 0
    }

    # Count unique Egyptian IDs
    egyptian_ids = set()
    for etym in etymologies:
        if etym['oraec_id']:
            egyptian_ids.add(('oraec', etym['oraec_id']))
        if etym['tla_id']:
            egyptian_ids.add(('tla', etym['tla_id']))

    stats['total_egyptian_ids'] = len(egyptian_ids)

    # For now, we'll mark all as unresolved since we don't have direct mappings
    # The etymology relations will be created with NULL target_lemma_id
    # and stored metadata about the external IDs for future resolution
    stats['unresolved'] = len(egyptian_ids)

    print(f"  ⚠ Egyptian ID resolution: {stats['resolved']}/{stats['total_egyptian_ids']} (fuzzy matching not yet implemented)")
    print("  → Etymology relations will store external IDs in metadata for future resolution")

    return egyptian_mapping, stats


def import_etymologies(
    conn: duckdb.DuckDBPyConnection,
    etymologies: List[Dict],
    cdo_mapping: Dict[str, str],
    egyptian_mapping: Dict[str, str]
) -> Dict:
    """
    Import etymology relations into database.

    Creates entries in etymology_relations table with:
    - source_lemma_id: Coptic lemma (from CDO)
    - target_lemma_id: Egyptian lemma (if resolved, else NULL)
    - relation_type: 'DERIVED_FROM'
    - confidence: Based on resolution quality
    - metadata: Stores external IDs (ORAEC, TLA) for future resolution

    Returns:
        Statistics dict
    """
    stats = {
        'total': len(etymologies),
        'inserted': 0,
        'coptic_unresolved': 0,
        'egyptian_unresolved': 0,
        'failed': 0,
        'duplicates': 0
    }

    timestamp = datetime.utcnow().isoformat() + 'Z'

    for etym in etymologies:
        cdo_id = etym['cdo_id']
        oraec_id = etym['oraec_id']
        tla_id = etym['tla_id']

        # Resolve Coptic side
        coptic_lemma_id = cdo_mapping.get(cdo_id)
        if not coptic_lemma_id:
            stats['coptic_unresolved'] += 1
            continue

        # Resolve Egyptian side (try both ORAEC and TLA)
        egyptian_lemma_id = None
        if oraec_id:
            egyptian_lemma_id = egyptian_mapping.get(('oraec', oraec_id))
        if not egyptian_lemma_id and tla_id:
            egyptian_lemma_id = egyptian_mapping.get(('tla', tla_id))

        if not egyptian_lemma_id:
            stats['egyptian_unresolved'] += 1

        # Determine confidence based on resolution quality
        if egyptian_lemma_id:
            confidence = 1.0  # Both sides resolved
        else:
            confidence = 0.5  # Only Coptic side resolved

        # Build metadata
        metadata = {
            'source': 'oraec_coptic_etymologies',
            'cdo_id': cdo_id,
            'oraec_id': oraec_id,
            'tla_id': tla_id,
            'dictionary_references': [
                'Černý, J., Coptic Etymological Dictionary',
                'Vycichl, W., Dictionnaire étymologique de la langue copte',
                'Westendorf, W., Koptisches Handwörterbuch'
            ]
        }

        # Create relation_id
        relation_id = f"etym:{coptic_lemma_id}:{egyptian_lemma_id or oraec_id or tla_id}"

        try:
            # Insert etymology relation
            # Note: If egyptian_lemma_id is NULL, we still insert the relationship
            # with metadata containing the external IDs for future resolution
            conn.execute("""
                INSERT INTO etymology_relations (
                    relation_id,
                    source_lemma_id,
                    target_lemma_id,
                    relation_type,
                    confidence,
                    evidence,
                    "references",
                    metadata,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                relation_id,
                coptic_lemma_id,
                egyptian_lemma_id,  # May be NULL
                'DERIVED_FROM',
                confidence,
                'Coptic Etymological Dictionary (ORAEC digitization)',
                ['https://github.com/oraec/coptic_etymologies'],
                metadata,
                timestamp,
                timestamp
            ])

            stats['inserted'] += 1

        except Exception as e:
            error_str = str(e)
            if 'Constraint Error' in error_str and 'UNIQUE' in error_str:
                stats['duplicates'] += 1
            else:
                stats['failed'] += 1
                if stats['failed'] <= 10:
                    print(f"    ERROR inserting {cdo_id}: {e}")

    return stats


def validate_import(conn: duckdb.DuckDBPyConnection):
    """Validate the etymology import with queries."""
    print("\n" + "="*60)
    print("VALIDATION QUERIES")
    print("="*60)

    # Total etymology relations
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM etymology_relations
    """).fetchone()
    print(f"Total etymology relations:     {result[0]:>6}")

    # ORAEC-sourced relations
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM etymology_relations
        WHERE json_extract_string(metadata, '$.source') = 'oraec_coptic_etymologies'
    """).fetchone()
    print(f"ORAEC-sourced relations:       {result[0]:>6}")

    # Relations with both sides resolved
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM etymology_relations
        WHERE target_lemma_id IS NOT NULL
          AND json_extract_string(metadata, '$.source') = 'oraec_coptic_etymologies'
    """).fetchone()
    print(f"Both sides resolved:           {result[0]:>6}")

    # Relations with only Coptic side
    result = conn.execute("""
        SELECT COUNT(*) as count
        FROM etymology_relations
        WHERE target_lemma_id IS NULL
          AND json_extract_string(metadata, '$.source') = 'oraec_coptic_etymologies'
    """).fetchone()
    print(f"Only Coptic side resolved:     {result[0]:>6}")

    # Test C1494 → qꜣḥ (earth)
    print("\n" + "="*60)
    print("TEST CASE: C1494 (ⲕⲁϩ ← qꜣḥ 'earth')")
    print("="*60)

    result = conn.execute("""
        SELECT
            er.relation_id,
            l_cop.lemma as coptic_lemma,
            l_cop.gloss_en as coptic_gloss,
            l_egy.lemma as egyptian_lemma,
            l_egy.gloss_en as egyptian_gloss,
            er.confidence,
            json_extract_string(er.metadata, '$.cdo_id') as cdo_id,
            json_extract_string(er.metadata, '$.oraec_id') as oraec_id,
            json_extract_string(er.metadata, '$.tla_id') as tla_id
        FROM etymology_relations er
        JOIN lemmas l_cop ON er.source_lemma_id = l_cop.lemma_id
        LEFT JOIN lemmas l_egy ON er.target_lemma_id = l_egy.lemma_id
        WHERE json_extract_string(er.metadata, '$.cdo_id') = 'C1494'
    """).fetchone()

    if result:
        print("  ✓ FOUND!")
        print(f"    Coptic:      {result[1]} ({result[2]})")
        print(f"    Egyptian:    {result[3] or 'NOT RESOLVED'} ({result[4] or 'N/A'})")
        print(f"    Confidence:  {result[5]}")
        print(f"    CDO ID:      {result[6]}")
        print(f"    ORAEC ID:    {result[7] or 'N/A'}")
        print(f"    TLA ID:      {result[8] or 'N/A'}")
    else:
        print("  ✗ NOT FOUND - Import may have failed")


def main():
    """Execute Coptic etymology import."""
    print("="*60)
    print("COPTIC ETYMOLOGY IMPORT")
    print("="*60)
    print("Importing ORAEC digitized Coptic etymologies")
    print("Source: Černý, Vycichl, Westendorf dictionaries")
    print("License: CC0 (Public Domain)")
    print("="*60)

    # Paths
    csv_path = Path("/home/tiagot/kemet-data/data/raw/oraec/coptic_etymologies.csv")
    lexicon_db = Path("/home/tiagot/kemet-data/data/derived/lexicon.duckdb")

    if not csv_path.exists():
        print(f"ERROR: Etymology CSV not found at {csv_path}")
        return 1

    if not lexicon_db.exists():
        print(f"ERROR: KEMET lexicon not found at {lexicon_db}")
        return 1

    # Step 1: Load CSV
    print("\n[1/7] Loading ORAEC etymology CSV...")
    etymologies = load_etymology_csv(csv_path)
    print(f"  ✓ Loaded {len(etymologies)} relationships")

    # Step 2: Connect to database
    print("\n[2/7] Connecting to lexicon database...")
    conn = duckdb.connect(str(lexicon_db))
    print("  ✓ Connected")

    # Step 3: Create tables
    print("\n[3/7] Creating etymology and mapping tables...")
    create_tables(conn)
    print("  ✓ Tables created")

    # Step 4: Resolve Coptic side (CDO IDs)
    print("\n[4/7] Resolving CDO IDs to Coptic lemmas...")
    all_cdo_ids = [e['cdo_id'] for e in etymologies if e['cdo_id']]
    cdo_mapping = resolve_cdo_ids(conn, all_cdo_ids)

    # Step 5: Load Egyptian lemmas
    print("\n[5/7] Loading Egyptian lemmas from KEMET...")
    egyptian_lemmas = load_egyptian_lemmas(conn)

    # Step 6: Resolve Egyptian side (ORAEC/TLA IDs)
    print("\n[6/7] Resolving ORAEC/TLA IDs to Egyptian lemmas...")
    egyptian_mapping, egyptian_stats = resolve_egyptian_ids_fuzzy(
        etymologies, egyptian_lemmas, cdo_mapping
    )

    # Step 7: Import etymology relations
    print("\n[7/7] Importing etymology relations...")
    stats = import_etymologies(conn, etymologies, cdo_mapping, egyptian_mapping)

    print("\n" + "="*60)
    print("IMPORT STATISTICS")
    print("="*60)
    print(f"Total relationships:           {stats['total']:>6}")
    print(f"Successfully inserted:         {stats['inserted']:>6}")
    print(f"Coptic side unresolved:        {stats['coptic_unresolved']:>6}")
    print(f"Egyptian side unresolved:      {stats['egyptian_unresolved']:>6}")
    print(f"Duplicates skipped:            {stats['duplicates']:>6}")
    print(f"Failed:                        {stats['failed']:>6}")

    # Validate
    validate_import(conn)

    conn.close()

    print("\n" + "="*60)
    print("✓ COPTIC ETYMOLOGY IMPORT COMPLETE")
    print("="*60)
    print("\nNOTE: Egyptian lemmas are currently unresolved (target_lemma_id = NULL).")
    print("External IDs (ORAEC, TLA) are stored in metadata for future resolution.")
    print("Future enhancement: Implement ORAEC/TLA ID → KEMET lemma matching.")

    return 0


if __name__ == "__main__":
    exit(main())
