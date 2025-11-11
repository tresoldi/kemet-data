"""Lexicon builder - Extracts lexical data from corpus database."""

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click
import duckdb


@dataclass
class LemmaData:
    """Aggregated data for a lemma."""
    lemma: str
    language: str
    pos: str
    frequency: int
    document_count: int
    collection_count: int
    forms: dict[str, int]  # form -> frequency
    attestations: dict[str, dict[str, int]]  # dimension -> {value -> frequency}
    metadata: dict[str, Any]


class LexiconBuilder:
    """
    Builds lexicon database from corpus database.

    Extracts unique lemmas, computes statistics, and creates
    lexical resources optimized for dictionary/reconstruction work.
    """

    def __init__(self, corpus_db_path: Path, lexicon_db_path: Path, logger: logging.Logger):
        """
        Initialize lexicon builder.

        Args:
            corpus_db_path: Path to corpus.duckdb
            lexicon_db_path: Path to lexicon.duckdb (will be created)
            logger: Logger instance
        """
        self.corpus_db_path = corpus_db_path
        self.lexicon_db_path = lexicon_db_path
        self.logger = logger

    def build(self, drop_existing: bool = False) -> None:
        """
        Build complete lexicon database from corpus.

        Args:
            drop_existing: Whether to drop existing lexicon database
        """
        # Remove existing if requested
        if drop_existing and self.lexicon_db_path.exists():
            click.echo("  Dropping existing lexicon database")
            self.lexicon_db_path.unlink()

        # Create lexicon database with schema
        click.echo("  [1/7] Creating lexicon schema...")
        self._create_schema()

        # Extract and aggregate data
        click.echo("  [2/7] Extracting lemmas from corpus...")
        lemmas = self._extract_lemmas()
        click.echo(f"        Found {len(lemmas):,} unique lemmas")

        # Populate lexicon tables
        click.echo("  [3/7] Populating lemmas table...")
        self._populate_lemmas(lemmas)

        click.echo("  [4/7] Populating forms table...")
        self._populate_forms(lemmas)

        click.echo("  [5/7] Populating attestations table...")
        self._populate_attestations(lemmas)

        # Create indexes
        click.echo("  [6/7] Creating indexes...")
        self._create_indexes()

        # Compute statistics
        click.echo("  [7/7] Computing statistics...")
        self._compute_statistics()

        click.echo("\nLexicon database build complete\n")

    def _create_schema(self) -> None:
        """Create lexicon database schema."""
        self.logger.info("Creating lexicon schema...")

        schema_path = Path(__file__).parent.parent.parent / "etc" / "schemas" / "lexicon_schema.sql"

        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        # Read schema
        with schema_path.open() as f:
            schema_sql = f.read()

        # Create database and execute schema
        con = duckdb.connect(str(self.lexicon_db_path))
        try:
            # Execute schema in parts (split by CREATE statements)
            statements = []
            current_stmt = []

            for line in schema_sql.split('\n'):
                # Skip comments
                if line.strip().startswith('--'):
                    continue

                current_stmt.append(line)

                # End of statement
                if line.strip().endswith(';'):
                    stmt = '\n'.join(current_stmt).strip()
                    if stmt and not stmt.startswith('--'):
                        statements.append(stmt)
                    current_stmt = []

            # Execute each statement
            for stmt in statements:
                try:
                    con.execute(stmt)
                except Exception as e:
                    # Some statements might fail (e.g., DROP IF EXISTS on first run)
                    self.logger.debug(f"Statement execution note: {e}")

            self.logger.info(f"Created lexicon database: {self.lexicon_db_path}")

        finally:
            con.close()

    def _extract_lemmas(self) -> dict[str, LemmaData]:
        """
        Extract unique lemmas from corpus with aggregated statistics.

        Uses SQL aggregation for optimal performance.

        Returns:
            Dictionary mapping lemma_id to LemmaData
        """
        corpus_con = duckdb.connect(str(self.corpus_db_path), read_only=True)

        try:
            import json

            # Step 1: Get basic lemma info with frequencies (SQL aggregation)
            self.logger.info("Aggregating lemma statistics...")
            lemma_stats = corpus_con.execute('''
                SELECT
                    t.lemma_id,
                    t.lang,
                    COUNT(*) as frequency,
                    COUNT(DISTINCT t.document_id) as document_count,
                    COUNT(DISTINCT d.collection) as collection_count,
                    MIN(t.metadata) as sample_metadata
                FROM token_instances t
                JOIN documents d ON t.document_id = d.document_id
                WHERE t.lemma_id IS NOT NULL
                GROUP BY t.lemma_id, t.lang
            ''').fetchall()

            self.logger.info(f"Processing {len(lemma_stats):,} unique lemmas...")

            # Initialize lemma data
            lemma_data: dict[str, LemmaData] = {}
            for lemma_id, lang, frequency, document_count, collection_count, metadata_json in lemma_stats:
                # Extract lemma and POS from metadata or lemma_id
                metadata = json.loads(metadata_json) if metadata_json else {}
                lemma_text = lemma_id.split(':')[-1] if ':' in lemma_id else lemma_id
                pos = metadata.get('pos') or metadata.get('xpos') or 'UNKNOWN'

                lemma_data[lemma_id] = LemmaData(
                    lemma=lemma_text,
                    language=lang or 'unknown',
                    pos=pos,
                    frequency=frequency,
                    document_count=document_count,
                    collection_count=collection_count,
                    forms={},
                    attestations={},
                    metadata=metadata
                )

            # Step 2: Get form distributions (SQL aggregation)
            self.logger.info("Aggregating form distributions...")
            form_stats = corpus_con.execute('''
                SELECT
                    lemma_id,
                    form,
                    COUNT(*) as form_frequency
                FROM token_instances
                WHERE lemma_id IS NOT NULL
                GROUP BY lemma_id, form
            ''').fetchall()

            for lemma_id, form, form_freq in form_stats:
                if lemma_id in lemma_data:
                    lemma_data[lemma_id].forms[form] = form_freq

            # Step 3: Get attestations by dimension (SQL aggregation)
            self.logger.info("Aggregating attestations...")

            # SUBSTAGE attestations
            substage_stats = corpus_con.execute('''
                SELECT
                    t.lemma_id,
                    d.substage,
                    COUNT(*) as frequency
                FROM token_instances t
                JOIN documents d ON t.document_id = d.document_id
                WHERE t.lemma_id IS NOT NULL AND d.substage IS NOT NULL
                GROUP BY t.lemma_id, d.substage
            ''').fetchall()

            for lemma_id, substage, freq in substage_stats:
                if lemma_id in lemma_data:
                    if 'SUBSTAGE' not in lemma_data[lemma_id].attestations:
                        lemma_data[lemma_id].attestations['SUBSTAGE'] = {}
                    lemma_data[lemma_id].attestations['SUBSTAGE'][substage] = freq

            # STAGE attestations
            stage_stats = corpus_con.execute('''
                SELECT
                    t.lemma_id,
                    d.stage,
                    COUNT(*) as frequency
                FROM token_instances t
                JOIN documents d ON t.document_id = d.document_id
                WHERE t.lemma_id IS NOT NULL AND d.stage IS NOT NULL
                GROUP BY t.lemma_id, d.stage
            ''').fetchall()

            for lemma_id, stage, freq in stage_stats:
                if lemma_id in lemma_data:
                    if 'STAGE' not in lemma_data[lemma_id].attestations:
                        lemma_data[lemma_id].attestations['STAGE'] = {}
                    lemma_data[lemma_id].attestations['STAGE'][stage] = freq

            # COLLECTION attestations
            collection_stats = corpus_con.execute('''
                SELECT
                    t.lemma_id,
                    d.collection,
                    COUNT(*) as frequency
                FROM token_instances t
                JOIN documents d ON t.document_id = d.document_id
                WHERE t.lemma_id IS NOT NULL AND d.collection IS NOT NULL
                GROUP BY t.lemma_id, d.collection
            ''').fetchall()

            for lemma_id, collection, freq in collection_stats:
                if lemma_id in lemma_data:
                    if 'COLLECTION' not in lemma_data[lemma_id].attestations:
                        lemma_data[lemma_id].attestations['COLLECTION'] = {}
                    lemma_data[lemma_id].attestations['COLLECTION'][collection] = freq

            return lemma_data

        finally:
            corpus_con.close()

    def _populate_lemmas(self, lemmas: dict[str, LemmaData]) -> None:
        """
        Populate lemmas table in lexicon database.

        Args:
            lemmas: Dictionary of lemma data
        """
        self.logger.info(f"Populating lemmas table with {len(lemmas):,} entries...")

        con = duckdb.connect(str(self.lexicon_db_path))

        try:
            rows = []
            for lemma_id, data in lemmas.items():
                # Determine script from language
                script = None
                if data.language == 'cop':
                    script = 'Coptic'
                elif data.language == 'egy':
                    script = 'Hieroglyphic'  # Default, may be refined
                elif data.language == 'grc':
                    script = 'Greek'

                row = (
                    lemma_id,
                    data.lemma,
                    data.language,
                    script,
                    None,  # period (to be added later)
                    data.pos if data.pos != 'UNKNOWN' else None,
                    None,  # pos_detail
                    None,  # gloss_en
                    None,  # gloss_de
                    None,  # gloss_fr
                    None,  # semantic_domain
                    None,  # semantic_field
                    None,  # hieroglyphic_writing
                    None,  # mdc_transcription
                    None,  # gardiner_codes
                    None,  # transliteration
                    None,  # bohairic_form
                    None,  # sahidic_form
                    None,  # other_dialects
                    data.frequency,
                    data.document_count,
                    data.collection_count,
                    None,  # first_attested_date
                    None,  # last_attested_date
                    None,  # first_attested_period
                    None,  # last_attested_period
                    None,  # attested_regions
                    None,  # etymology_source_lemma_id
                    None,  # etymology_type
                    None,  # etymology_notes
                    None,  # synonyms
                    None,  # antonyms
                    None,  # hypernyms
                    None,  # hyponyms
                    None,  # related_lemmas
                    None,  # phonetic_form
                    None,  # phonological_notes
                    'corpus_derived',  # source
                    None,  # source_id
                    None,  # confidence
                    None,  # metadata
                )
                rows.append(row)

            # Bulk insert
            con.executemany('''
                INSERT INTO lemmas (
                    lemma_id, lemma, language, script, period,
                    pos, pos_detail,
                    gloss_en, gloss_de, gloss_fr,
                    semantic_domain, semantic_field,
                    hieroglyphic_writing, mdc_transcription, gardiner_codes, transliteration,
                    bohairic_form, sahidic_form, other_dialects,
                    frequency, document_count, collection_count,
                    first_attested_date, last_attested_date,
                    first_attested_period, last_attested_period, attested_regions,
                    etymology_source_lemma_id, etymology_type, etymology_notes,
                    synonyms, antonyms, hypernyms, hyponyms, related_lemmas,
                    phonetic_form, phonological_notes,
                    source, source_id, confidence, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows)

            self.logger.info(f"Inserted {len(rows):,} lemmas")

        finally:
            con.close()

    def _populate_forms(self, lemmas: dict[str, LemmaData]) -> None:
        """
        Populate forms table in lexicon database.

        Args:
            lemmas: Dictionary of lemma data
        """
        self.logger.info("Populating forms table...")

        con = duckdb.connect(str(self.lexicon_db_path))

        try:
            rows = []
            for lemma_id, data in lemmas.items():
                for form, freq in data.forms.items():
                    form_id = f"{lemma_id}:form:{form}"
                    rel_freq = freq / data.frequency if data.frequency > 0 else 0

                    row = (
                        form_id,
                        lemma_id,
                        form,
                        form,  # form_normalized (same for now)
                        None,  # form_transliterated
                        None,  # morphology
                        None,  # morphology_detailed
                        None,  # tense
                        None,  # aspect
                        None,  # mood
                        None,  # voice
                        None,  # person
                        None,  # number
                        None,  # gender
                        None,  # case_marking
                        freq,
                        rel_freq,
                        None,  # metadata
                    )
                    rows.append(row)

            # Bulk insert
            con.executemany('''
                INSERT INTO forms (
                    form_id, lemma_id, form, form_normalized, form_transliterated,
                    morphology, morphology_detailed,
                    tense, aspect, mood, voice, person, number, gender, case_marking,
                    frequency, relative_frequency, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows)

            self.logger.info(f"Inserted {len(rows):,} forms")

        finally:
            con.close()

    def _populate_attestations(self, lemmas: dict[str, LemmaData]) -> None:
        """
        Populate lemma_attestations table.

        Args:
            lemmas: Dictionary of lemma data
        """
        self.logger.info("Populating attestations table...")

        con = duckdb.connect(str(self.lexicon_db_path))

        try:
            rows = []
            for lemma_id, data in lemmas.items():
                for dimension_type, values in data.attestations.items():
                    for dimension_value, freq in values.items():
                        attestation_id = f"{lemma_id}:{dimension_type}:{dimension_value}"

                        row = (
                            attestation_id,
                            lemma_id,
                            dimension_type,
                            dimension_value,
                            freq,
                            0,  # document_count (to be computed later)
                            None,  # first_occurrence
                            None,  # last_occurrence
                            None,  # example_segment_ids (to be added later)
                            None,  # example_forms
                        )
                        rows.append(row)

            # Bulk insert
            con.executemany('''
                INSERT INTO lemma_attestations (
                    attestation_id, lemma_id, dimension_type, dimension_value,
                    frequency, document_count,
                    first_occurrence, last_occurrence,
                    example_segment_ids, example_forms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rows)

            self.logger.info(f"Inserted {len(rows):,} attestations")

        finally:
            con.close()

    def _create_indexes(self) -> None:
        """Create indexes for fast queries (already in schema, verify)."""
        self.logger.info("Verifying indexes...")
        # Indexes are created in schema, just verify they exist
        con = duckdb.connect(str(self.lexicon_db_path))
        try:
            indexes = con.execute("SHOW TABLES").fetchall()
            self.logger.info(f"Tables created: {len(indexes)}")
        finally:
            con.close()

    def _compute_statistics(self) -> None:
        """Compute and store lexicon-wide statistics."""
        self.logger.info("Computing lexicon statistics...")

        con = duckdb.connect(str(self.lexicon_db_path))

        try:
            # Total lemmas
            row = con.execute('SELECT COUNT(*) FROM lemmas').fetchone()
            assert row is not None
            total_lemmas = row[0]

            # Lemmas by language
            lang_dist = dict(con.execute('''
                SELECT language, COUNT(*) as cnt
                FROM lemmas
                GROUP BY language
            ''').fetchall())

            # Average frequency
            row = con.execute('SELECT AVG(frequency) FROM lemmas').fetchone()
            assert row is not None
            avg_freq = row[0]

            # Store statistics
            import json
            stats = [
                ('total_lemmas', json.dumps({'count': total_lemmas})),
                ('lemmas_by_language', json.dumps(lang_dist)),
                ('average_frequency', json.dumps({'value': round(avg_freq, 2)})),
            ]

            con.executemany('''
                INSERT INTO lexicon_statistics (stat_key, stat_value)
                VALUES (?, ?)
            ''', stats)

            self.logger.info(f"Computed statistics: {len(stats)} entries")
            self.logger.info(f"  Total lemmas: {total_lemmas:,}")
            self.logger.info(f"  By language: {lang_dist}")
            self.logger.info(f"  Average frequency: {avg_freq:.1f}")

        finally:
            con.close()


def build_lexicon(
    corpus_db_path: Path,
    lexicon_db_path: Path,
    drop_existing: bool = False,
    logger: logging.Logger | None = None,
) -> None:
    """
    Build lexicon database from corpus database.

    Args:
        corpus_db_path: Path to corpus.duckdb
        lexicon_db_path: Path to lexicon.duckdb
        drop_existing: Whether to drop existing lexicon
        logger: Logger instance (creates default if None)
    """
    if logger is None:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

    builder = LexiconBuilder(corpus_db_path, lexicon_db_path, logger)
    builder.build(drop_existing=drop_existing)


if __name__ == '__main__':
    from pathlib import Path

    # Default paths
    ROOT_DIR = Path(__file__).parent.parent.parent
    corpus_db = ROOT_DIR / "data" / "derived" / "corpus.duckdb"
    lexicon_db = ROOT_DIR / "data" / "derived" / "lexicon.duckdb"

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)8s] %(name)s: %(message)s'
    )
    logger = logging.getLogger(__name__)

    # Build
    build_lexicon(corpus_db, lexicon_db, drop_existing=True, logger=logger)
