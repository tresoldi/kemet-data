#!/usr/bin/env python3
"""Populate lexicon database with TLA metadata.

This script takes the matched TLA lemmas from the cache and:
1. Adds TLA metadata table to lexicon database
2. Updates existing lemmas with TLA IDs (source_id field)
3. Populates TLA metadata table with hieroglyphs and other TLA-specific data
"""

from pathlib import Path
import logging
import json

import duckdb


class TLADatabasePopulator:
    """Populate lexicon database with TLA metadata."""

    def __init__(
        self,
        lexicon_db_path: Path,
        matches_path: Path,
        logger: logging.Logger = None
    ):
        self.lexicon_db_path = Path(lexicon_db_path)
        self.matches_path = Path(matches_path)
        self.logger = logger or logging.getLogger(__name__)
        self.conn = None

    def create_tla_tables(self):
        """Create TLA metadata table if it doesn't exist."""
        self.logger.info("Creating TLA metadata table...")

        # Create TLA metadata table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tla_metadata (
                tla_id VARCHAR PRIMARY KEY,
                lemma_id VARCHAR REFERENCES lemmas(lemma_id),

                -- TLA-specific data
                transliteration VARCHAR NOT NULL,
                hieroglyphs VARCHAR,
                attestation_count INTEGER DEFAULT 0,  -- From HF dataset

                -- Match quality
                match_type VARCHAR,  -- exact, fuzzy, no_match
                num_corpus_matches INTEGER,  -- How many corpus lemmas matched

                -- Metadata
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create indexes
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tla_lemma_id
            ON tla_metadata(lemma_id);
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tla_transliteration
            ON tla_metadata(transliteration);
        """)

        self.logger.info("TLA metadata table created")

    def load_matches(self):
        """Load TLA lemma matches from cache."""
        self.logger.info(f"Loading matches from {self.matches_path}")

        with self.matches_path.open() as f:
            matches = json.load(f)

        self.logger.info(f"Loaded {len(matches)} TLA lemma matches")
        return matches

    def update_lemmas_with_tla_ids(self, matches: dict):
        """Update lemmas table with TLA source IDs.

        Sets source_id to TLA ID for all matched lemmas.
        """
        self.logger.info("Updating lemmas with TLA source IDs...")

        updates = []
        for tla_id, match_data in matches.items():
            if match_data['match_type'] == 'exact':
                lemma_id = match_data['lemma_id']
                updates.append((tla_id, lemma_id))

        # Batch update using SQL
        for tla_id, lemma_id in updates:
            self.conn.execute("""
                UPDATE lemmas
                SET source_id = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE lemma_id = ?
            """, [tla_id, lemma_id])

        self.logger.info(f"Updated {len(updates)} lemmas with TLA source IDs")

    def populate_tla_metadata(self, matches: dict):
        """Populate TLA metadata table."""
        self.logger.info("Populating TLA metadata table...")

        rows = []
        for tla_id, match_data in matches.items():
            rows.append({
                'tla_id': tla_id,
                'lemma_id': match_data.get('lemma_id'),
                'transliteration': match_data['transliteration'],
                'hieroglyphs': match_data.get('hieroglyphs'),
                'attestation_count': match_data.get('attestation_count', 0),
                'match_type': match_data.get('match_type'),
                'num_corpus_matches': match_data.get('num_corpus_matches', 0)
            })

        # Insert using DuckDB's from_dict
        import pandas as pd
        df = pd.DataFrame(rows)  # noqa: F841 - used in DuckDB SQL query below

        # Select only the columns that match the table schema (excluding timestamps)
        self.conn.execute("""
            INSERT OR REPLACE INTO tla_metadata (
                tla_id, lemma_id, transliteration, hieroglyphs,
                attestation_count, match_type, num_corpus_matches
            )
            SELECT
                tla_id, lemma_id, transliteration, hieroglyphs,
                attestation_count, match_type, num_corpus_matches
            FROM df
        """)

        self.logger.info(f"Inserted {len(rows)} TLA metadata records")

    def compute_statistics(self):
        """Compute and log TLA integration statistics."""
        self.logger.info("Computing TLA integration statistics...")

        stats = {}

        # Total TLA records
        stats['total_tla_records'] = self.conn.execute(
            "SELECT COUNT(*) FROM tla_metadata"
        ).fetchone()[0]

        # Matched vs unmatched
        stats['matched_to_corpus'] = self.conn.execute(
            "SELECT COUNT(*) FROM tla_metadata WHERE lemma_id IS NOT NULL"
        ).fetchone()[0]

        stats['not_matched'] = self.conn.execute(
            "SELECT COUNT(*) FROM tla_metadata WHERE lemma_id IS NULL"
        ).fetchone()[0]

        # Lemmas with TLA IDs
        stats['lemmas_with_tla'] = self.conn.execute(
            "SELECT COUNT(*) FROM lemmas WHERE source_id IS NOT NULL"
        ).fetchone()[0]

        # Total Egyptian lemmas
        stats['total_egyptian_lemmas'] = self.conn.execute(
            "SELECT COUNT(*) FROM lemmas WHERE language = 'egy'"
        ).fetchone()[0]

        # Coverage percentage
        stats['tla_coverage_pct'] = (
            stats['lemmas_with_tla'] / stats['total_egyptian_lemmas'] * 100
            if stats['total_egyptian_lemmas'] > 0 else 0
        )

        # Update lexicon statistics table
        self.conn.execute("""
            INSERT OR REPLACE INTO lexicon_statistics (stat_key, stat_value)
            VALUES ('tla_integration', ?)
        """, [json.dumps(stats)])

        # Log statistics
        self.logger.info("=== TLA Integration Statistics ===")
        for key, value in stats.items():
            self.logger.info(f"  {key}: {value}")

        return stats

    def run(self):
        """Run complete TLA database population."""
        try:
            # Connect to database
            self.logger.info(f"Connecting to {self.lexicon_db_path}")
            self.conn = duckdb.connect(str(self.lexicon_db_path))

            # Load matches
            matches = self.load_matches()

            # Create TLA tables
            self.create_tla_tables()

            # Update lemmas with TLA IDs
            self.update_lemmas_with_tla_ids(matches)

            # Populate TLA metadata
            self.populate_tla_metadata(matches)

            # Compute statistics
            stats = self.compute_statistics()

            self.logger.info("TLA database population complete")

            return stats

        finally:
            if self.conn:
                self.conn.close()


def main():
    """Run TLA database population."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Paths
    project_root = Path(__file__).parent.parent.parent
    lexicon_db_path = project_root / "data_derived" / "lexicon.duckdb"
    matches_path = Path.home() / ".cache" / "kemet" / "tla" / "tla_lemma_matches.json"

    # Validate paths
    if not lexicon_db_path.exists():
        print(f"ERROR: Lexicon database not found: {lexicon_db_path}")
        exit(1)

    if not matches_path.exists():
        print(f"ERROR: TLA matches file not found: {matches_path}")
        print("Run tla_huggingface.py first to download and match TLA data")
        exit(1)

    # Run population
    populator = TLADatabasePopulator(
        lexicon_db_path=lexicon_db_path,
        matches_path=matches_path
    )
    stats = populator.run()

    print("\n=== TLA Database Population Complete ===")
    print(f"Total TLA records: {stats['total_tla_records']}")
    print(f"Matched to corpus: {stats['matched_to_corpus']}")
    print(f"Egyptian lemmas with TLA IDs: {stats['lemmas_with_tla']}")
    print(f"TLA coverage: {stats['tla_coverage_pct']:.1f}%")


if __name__ == "__main__":
    main()
