"""Build dual DuckDB databases (corpus + lexicon) from curated Parquet/JSONL files."""

import json
import logging
from pathlib import Path
from typing import Any

import click
import duckdb
import pandas as pd
from tqdm import tqdm

from scripts.lexicon import build_lexicon


class DatabaseBuilder:
    """Build dual DuckDB databases (corpus + lexicon) from curated data."""

    def __init__(
        self,
        curated_dir: Path,
        corpus_db_path: Path,
        lexicon_db_path: Path,
        logger: logging.Logger | None = None
    ):
        self.curated_dir = Path(curated_dir)
        self.corpus_db_path = Path(corpus_db_path)
        self.lexicon_db_path = Path(lexicon_db_path)
        self.logger = logger or logging.getLogger(__name__)
        self.conn: duckdb.DuckDBPyConnection | None = None

    def build(self, drop_existing: bool = False) -> None:
        """Build complete dual database (corpus + lexicon)."""
        # Drop existing databases if requested
        if drop_existing:
            if self.corpus_db_path.exists():
                click.echo("  Dropping existing corpus database")
                self.corpus_db_path.unlink()
            if self.lexicon_db_path.exists():
                click.echo("  Dropping existing lexicon database")
                self.lexicon_db_path.unlink()

        # Build corpus database
        click.echo("\n" + "="*80)
        click.echo("BUILDING CORPUS DATABASE")
        click.echo("="*80 + "\n")
        self.conn = duckdb.connect(str(self.corpus_db_path))

        try:
            click.echo("  [1/5] Creating schema...")
            self._create_corpus_schema()

            click.echo("  [2/5] Importing collections...")
            self._import_all_collections()

            click.echo("  [3/5] Creating views...")
            self._create_views()

            click.echo("  [4/5] Creating indexes...")
            self._create_indexes()

            click.echo("  [5/5] Computing statistics...")
            self._compute_stats()

            click.echo("\nCorpus database build complete\n")
        finally:
            if self.conn:
                self.conn.close()

        # Build lexicon database from corpus
        click.echo("="*80)
        click.echo("BUILDING LEXICON DATABASE")
        click.echo("="*80 + "\n")
        build_lexicon(
            corpus_db_path=self.corpus_db_path,
            lexicon_db_path=self.lexicon_db_path,
            drop_existing=True,
            logger=self.logger
        )

        self.logger.info("Dual database build complete")

    def _create_corpus_schema(self) -> None:
        """Create corpus database schema from SQL file."""
        self.logger.info("Creating corpus schema...")
        assert self.conn is not None

        # Load corpus schema
        schema_path = Path(__file__).parent.parent.parent / "etc" / "schemas" / "corpus_schema.sql"
        with schema_path.open() as f:
            schema_sql = f.read()

        # Parse and execute SQL statements
        statements = self._parse_sql_statements(schema_sql)
        for stmt in statements:
            try:
                self.conn.execute(stmt)
            except Exception as e:
                # Log errors but continue (some statements like DROP may fail)
                if "does not exist" not in str(e):
                    self.logger.warning(f"Schema statement error: {e}")

        self.logger.info("Corpus schema created")

    def _parse_sql_statements(self, sql: str) -> list[str]:
        """Parse SQL file into individual statements."""
        statements = []
        current_stmt = []

        for line in sql.split('\n'):
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

        return statements

    def _import_all_collections(self) -> None:
        """Import all curated collections."""
        # Count collections first
        collections = []
        for source_dir in self.curated_dir.iterdir():
            if not source_dir.is_dir():
                continue
            for collection_dir in source_dir.iterdir():
                if collection_dir.is_dir():
                    collections.append((source_dir.name, collection_dir.name, collection_dir))

        # Import with progress bar
        for source, collection, collection_dir in tqdm(collections, desc="     Importing", unit="coll"):
            try:
                self._import_collection(
                    source=source,
                    collection=collection,
                    collection_dir=collection_dir
                )
            except Exception as e:
                tqdm.write(f"        ERROR: Failed to import {source}/{collection}: {e}")
                raise

        click.echo(f"        Imported {len(collections)} collections")

    def _import_collection(
        self,
        source: str,
        collection: str,
        collection_dir: Path
    ) -> None:
        """Import single collection."""
        assert self.conn is not None
        self.logger.debug(f"Importing {source}/{collection}")

        # Import documents
        docs_path = collection_dir / 'documents.jsonl'
        if docs_path.exists():
            df = pd.read_json(docs_path, lines=True)

            # Skip if no documents
            if len(df) == 0:
                self.logger.warning(f"No documents in {source}/{collection}, skipping")
                return

            # Check for duplicates and warn
            if 'document_id' in df.columns and df['document_id'].duplicated().any():
                duplicates = df[df['document_id'].duplicated(keep=False)]
                self.logger.warning(
                    f"Found {len(duplicates)} duplicate document_ids in {source}/{collection}, "
                    f"keeping first occurrence"
                )
                df = df.drop_duplicates(subset=['document_id'], keep='first')

            df = self._normalize_documents(df)
            self.conn.execute("INSERT OR IGNORE INTO documents SELECT * FROM df")

        # Import segments
        segs_path = collection_dir / 'segments.parquet'
        if segs_path.exists():
            df = pd.read_parquet(segs_path)

            # Check for duplicates and warn
            if df['segment_id'].duplicated().any():
                duplicates = df[df['segment_id'].duplicated(keep=False)]
                self.logger.warning(
                    f"Found {len(duplicates)} duplicate segment_ids in {source}/{collection}, "
                    f"keeping first occurrence"
                )
                df = df.drop_duplicates(subset=['segment_id'], keep='first')

            # Filter out orphaned segments (segments without valid document_id)
            # Get valid document IDs from database
            valid_docs = set(self.conn.execute("SELECT document_id FROM documents").fetchdf()['document_id'])
            orphaned = df[~df['document_id'].isin(valid_docs)]
            if len(orphaned) > 0:
                self.logger.warning(
                    f"Found {len(orphaned)} orphaned segments in {source}/{collection}, skipping"
                )
                df = df[df['document_id'].isin(valid_docs)]

            df = self._normalize_segments(df)
            self.conn.execute("INSERT OR IGNORE INTO segments SELECT * FROM df")

        # Import tokens
        tokens_path = collection_dir / 'tokens.parquet'
        if tokens_path.exists():
            df = pd.read_parquet(tokens_path)

            # Check for duplicates and warn
            if df['token_id'].duplicated().any():
                duplicates = df[df['token_id'].duplicated(keep=False)]
                self.logger.warning(
                    f"Found {len(duplicates)} duplicate token_ids in {source}/{collection}, "
                    f"keeping first occurrence"
                )
                df = df.drop_duplicates(subset=['token_id'], keep='first')

            # Filter out orphaned tokens (tokens without valid segment_id)
            # Get valid segment IDs from database
            valid_segs = set(self.conn.execute("SELECT segment_id FROM segments").fetchdf()['segment_id'])
            orphaned = df[~df['segment_id'].isin(valid_segs)]
            if len(orphaned) > 0:
                self.logger.warning(
                    f"Found {len(orphaned)} orphaned tokens in {source}/{collection}, skipping"
                )
                df = df[df['segment_id'].isin(valid_segs)]

            # Get document language mapping for inference
            doc_lang_map = self.conn.execute(
                "SELECT document_id, language FROM documents"
            ).fetchdf().set_index('document_id')['language'].to_dict()

            df = self._normalize_tokens(df, doc_lang_map)
            self.conn.execute("INSERT OR IGNORE INTO token_instances SELECT * FROM df")

    def _fix_json_field(self, value: Any) -> str | None:
        """Ensure value is proper JSON string."""
        if pd.isna(value):
            return None
        if isinstance(value, str):
            # Try parsing as JSON
            try:
                json.loads(value)
                return value
            except json.JSONDecodeError:
                # Not valid JSON, skip it
                return None
        if isinstance(value, dict):
            # Convert dict to JSON string
            return json.dumps(value)
        return None

    def _normalize_documents(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize document schema for database."""
        # Fix JSON fields
        if 'metadata' in df.columns:
            df['metadata'] = df['metadata'].apply(self._fix_json_field)
        if 'provenance' in df.columns:
            df['provenance'] = df['provenance'].apply(self._fix_json_field)

        # Compute century from date_from
        if 'date_from' in df.columns:
            df['century'] = df['date_from'].apply(
                lambda x: int(x // 100) if pd.notna(x) else None
            )
        else:
            df['century'] = None

        # Ensure genre is list
        if 'genre' in df.columns:
            df['genre'] = df['genre'].apply(
                lambda x: x if isinstance(x, list) else ([x] if pd.notna(x) else [])
            )
        else:
            df['genre'] = [[] for _ in range(len(df))]

        # Ensure authors is list
        if 'authors' in df.columns:
            df['authors'] = df['authors'].apply(
                lambda x: x if isinstance(x, list) else ([x] if pd.notna(x) else [])
            )
        else:
            df['authors'] = [[] for _ in range(len(df))]

        # Extract counts
        if 'counts' in df.columns:
            df['num_segments'] = df['counts'].apply(
                lambda x: x.get('segments', 0) if isinstance(x, dict) else 0
            )
            df['num_tokens'] = df['counts'].apply(
                lambda x: x.get('tokens', 0) if isinstance(x, dict) else 0
            )
            # Drop counts column - it's now split into num_segments/num_tokens
            df = df.drop(columns=['counts'])
        else:
            df['num_segments'] = 0
            df['num_tokens'] = 0

        # Add script field if missing (for Coptic texts)
        if 'script' not in df.columns:
            # Infer from substage
            df['script'] = df.get('substage', pd.Series([None] * len(df))).apply(
                lambda x: 'COPTIC' if x in ['BOHAIRIC', 'SAHIDIC'] else None
            )

        # Add timestamps if missing
        if 'created_at' not in df.columns:
            df['created_at'] = pd.Timestamp.now()
        if 'updated_at' not in df.columns:
            df['updated_at'] = pd.Timestamp.now()

        # Select only columns that match the schema, in the correct order
        expected_columns = [
            'document_id', 'source', 'collection', 'stage', 'substage',
            'script', 'language', 'genre', 'date_from', 'date_to', 'century',
            'title', 'authors', 'license', 'num_segments', 'num_tokens',
            'metadata', 'provenance', 'created_at', 'updated_at'
        ]

        # Add missing columns as None
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        # Select and reorder columns
        df = df[expected_columns]

        return df

    def _normalize_segments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize segment schema for database."""
        # Extract text fields from metadata BEFORE converting to JSON
        # (Some older data might have these in metadata)
        if 'metadata' in df.columns:
            # Extract text_en if not already a column
            if 'text_en' not in df.columns:
                df['text_en'] = df['metadata'].apply(
                    lambda x: x.get('text_en') if isinstance(x, dict) else None
                )
            # Extract text_de if not already a column
            if 'text_de' not in df.columns:
                df['text_de'] = df['metadata'].apply(
                    lambda x: x.get('text_de') if isinstance(x, dict) else None
                )
            # Extract text_hieroglyphs if not already a column
            if 'text_hieroglyphs' not in df.columns:
                df['text_hieroglyphs'] = df['metadata'].apply(
                    lambda x: x.get('text_hieroglyphs') if isinstance(x, dict) else None
                )
            # Extract translation_language if not already a column
            if 'translation_language' not in df.columns:
                df['translation_language'] = df['metadata'].apply(
                    lambda x: x.get('translation_language') if isinstance(x, dict) else None
                )
            # Now fix JSON field
            df['metadata'] = df['metadata'].apply(self._fix_json_field)

        # Add missing columns with defaults
        if 'text_en' not in df.columns:
            df['text_en'] = None
        if 'text_de' not in df.columns:
            df['text_de'] = None
        if 'text_hieroglyphs' not in df.columns:
            df['text_hieroglyphs'] = None
        if 'translation_language' not in df.columns:
            # Infer from text_en/text_de if available
            df['translation_language'] = df.apply(
                lambda row: 'en' if pd.notna(row.get('text_en')) else ('de' if pd.notna(row.get('text_de')) else None),
                axis=1
            )

        # Add script field if missing
        if 'script' not in df.columns:
            df['script'] = df.get('dialect', pd.Series([None] * len(df))).apply(
                lambda x: 'COPTIC' if x in ['BOHAIRIC', 'SAHIDIC'] else None
            )

        # Ensure genre is list
        if 'genre' not in df.columns:
            df['genre'] = [[] for _ in range(len(df))]
        else:
            df['genre'] = df['genre'].apply(
                lambda x: x if isinstance(x, list) else ([x] if pd.notna(x) else [])
            )

        # Add created_at if missing
        if 'created_at' not in df.columns:
            df['created_at'] = pd.Timestamp.now()

        # Select only columns that match the schema, in the correct order
        expected_columns = [
            'segment_id', 'document_id', 'order', 'text_canonical', 'text_stripped',
            'text_display', 'text_hieroglyphs', 'text_en', 'text_de', 'translation_language',
            'dialect', 'script', 'genre', 'passage_ref', 'metadata', 'content_hash',
            'created_at'
        ]

        # Add missing columns as None
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        # Select and reorder columns
        df = df[expected_columns]

        return df

    def _normalize_tokens(self, df: pd.DataFrame, doc_lang_map: dict[str, Any] | None = None) -> pd.DataFrame:
        """Normalize token schema for corpus database (dual architecture).

        Args:
            df: Token dataframe
            doc_lang_map: Optional mapping of document_id -> language for inference
        """
        if doc_lang_map is None:
            doc_lang_map = {}
        # Extract fields from metadata BEFORE converting to JSON
        if 'metadata' in df.columns:
            def extract_head(x: Any) -> int | None:
                if isinstance(x, dict):
                    head_val = x.get('head')
                    if head_val is not None and head_val != '':
                        return int(head_val)
                return None
            df['head'] = df['metadata'].apply(extract_head)
            df['deprel'] = df['metadata'].apply(
                lambda x: x.get('deprel') if isinstance(x, dict) else None
            )
            # Now fix JSON field
            df['metadata'] = df['metadata'].apply(self._fix_json_field)
        else:
            df['head'] = None
            df['deprel'] = None

        # Infer language from parent document when lang is None/NaN
        if doc_lang_map is not None and 'lang' in df.columns and 'document_id' in df.columns:
            # Count tokens with missing language before inference
            missing_before = df['lang'].isna().sum()
            if missing_before > 0:
                # Infer from document
                df['lang'] = df.apply(
                    lambda row: doc_lang_map.get(row['document_id'])
                    if pd.isna(row.get('lang')) else row.get('lang'),
                    axis=1
                )
                missing_after = df['lang'].isna().sum()
                inferred = missing_before - missing_after
                if inferred > 0:
                    self.logger.info(f"Inferred language from parent document for {inferred} tokens")

        # Generate lemma_id from lemma + lang
        # Format: "{lang}:lemma:{lemma}"
        df['lemma_id'] = df.apply(
            lambda row: f"{row.get('lang', 'unknown')}:lemma:{row.get('lemma')}"
            if pd.notna(row.get('lemma')) else None,
            axis=1
        )

        # Corpus schema: only contextual data, no lexical fields (lemma, pos, morph)
        # Those are in the lexicon database
        expected_columns = [
            'token_id', 'segment_id', 'document_id', 'order', 'form',
            'head', 'deprel', 'lemma_id', 'lang', 'metadata', 'content_hash'
        ]

        # Add missing columns as None
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        # Select and reorder columns to match corpus schema
        df = df[expected_columns]

        return df

    def _create_views(self) -> None:
        """Create denormalized views (already in corpus schema)."""
        self.logger.info("Views already created with schema")

    def _create_indexes(self) -> None:
        """Create indexes for performance (already in corpus schema)."""
        self.logger.info("Indexes already created with schema")

    def _compute_stats(self) -> None:
        """Compute and store corpus statistics."""
        assert self.conn is not None

        row = self.conn.execute(
            "SELECT COUNT(DISTINCT collection) FROM documents"
        ).fetchone()
        assert row is not None
        total_collections = row[0]

        row = self.conn.execute(
            "SELECT COUNT(*) FROM documents"
        ).fetchone()
        assert row is not None
        total_documents = row[0]

        row = self.conn.execute(
            "SELECT COUNT(*) FROM segments"
        ).fetchone()
        assert row is not None
        total_segments = row[0]

        row = self.conn.execute(
            "SELECT COUNT(*) FROM token_instances"
        ).fetchone()
        assert row is not None
        total_tokens = row[0]

        stats = {
            'total_collections': total_collections,
            'total_documents': total_documents,
            'total_segments': total_segments,
            'total_tokens': total_tokens,
        }

        for key, value in stats.items():
            self.conn.execute(
                "INSERT OR REPLACE INTO corpus_statistics (stat_key, stat_value) VALUES (?, ?)",
                [key, str(value)]
            )

        # Populate collection statistics table (pre-computed for fast queries)
        # OPTIMIZED V3: Use separate simple queries to avoid Cartesian explosion
        self.logger.info("Computing per-collection statistics...")

        # Step 1a: Document counts (no joins - fastest)
        self.logger.info("  Counting documents per collection...")
        doc_counts_raw = self.conn.execute('''
            SELECT collection, COUNT(*) as document_count
            FROM documents
            GROUP BY collection
        ''').fetchall()
        doc_counts = {collection: count for collection, count in doc_counts_raw}

        # Step 1b: Segment counts (simple 1:N join)
        self.logger.info("  Counting segments per collection...")
        seg_counts_raw = self.conn.execute('''
            SELECT d.collection, COUNT(*) as segment_count
            FROM documents d
            JOIN segments s ON d.document_id = s.document_id
            GROUP BY d.collection
        ''').fetchall()
        seg_counts = {collection: count for collection, count in seg_counts_raw}

        # Step 1c: Token counts (simple 1:N join)
        self.logger.info("  Counting tokens per collection...")
        tok_counts_raw = self.conn.execute('''
            SELECT d.collection, COUNT(*) as token_count
            FROM documents d
            JOIN token_instances t ON d.document_id = t.document_id
            GROUP BY d.collection
        ''').fetchall()
        tok_counts = {collection: count for collection, count in tok_counts_raw}

        # Step 1d: Unique lemma counts (simple 1:N join with DISTINCT)
        self.logger.info("  Counting unique lemmas per collection...")
        lemma_counts_raw = self.conn.execute('''
            SELECT d.collection, COUNT(DISTINCT t.lemma_id) as unique_lemma_count
            FROM documents d
            JOIN token_instances t ON d.document_id = t.document_id
            WHERE t.lemma_id IS NOT NULL
            GROUP BY d.collection
        ''').fetchall()
        lemma_counts = {collection: count for collection, count in lemma_counts_raw}

        # Combine all collections from all queries
        all_collections = set(doc_counts.keys())

        # Build collection_stats list in same format as before
        collection_stats = [
            (collection,
             doc_counts.get(collection, 0),
             seg_counts.get(collection, 0),
             tok_counts.get(collection, 0),
             lemma_counts.get(collection, 0))
            for collection in sorted(all_collections)
        ]

        # Step 2: Get language distribution for ALL collections (single bulk query)
        lang_dist_bulk = self.conn.execute('''
            SELECT d.collection, t.lang, COUNT(*) as count
            FROM token_instances t
            JOIN documents d ON t.document_id = d.document_id
            WHERE t.lang IS NOT NULL
            GROUP BY d.collection, t.lang
        ''').fetchall()

        # Pivot language results into dict of dicts: {collection: {lang: count}}
        lang_by_collection = {}
        for collection, lang, count in lang_dist_bulk:
            if collection not in lang_by_collection:
                lang_by_collection[collection] = {}
            lang_by_collection[collection][lang] = count

        # Step 3: Get POS distribution for ALL collections (single bulk query)
        pos_dist_bulk = self.conn.execute('''
            SELECT
                d.collection,
                JSON_EXTRACT(t.metadata, '$.pos') as pos,
                COUNT(*) as count
            FROM token_instances t
            JOIN documents d ON t.document_id = d.document_id
            WHERE t.metadata IS NOT NULL
              AND JSON_EXTRACT(t.metadata, '$.pos') IS NOT NULL
            GROUP BY d.collection, JSON_EXTRACT(t.metadata, '$.pos')
        ''').fetchall()

        # Pivot POS results into dict of dicts: {collection: {pos: count}}
        pos_by_collection = {}
        for collection, pos, count in pos_dist_bulk:
            if pos:  # Filter out null POS values
                if collection not in pos_by_collection:
                    pos_by_collection[collection] = {}
                pos_by_collection[collection][pos] = count

        # Step 4: Insert all collection statistics (batch inserts)
        for collection, doc_count, seg_count, tok_count, lemma_count in collection_stats:
            lang_dist = lang_by_collection.get(collection, {})
            pos_dist = pos_by_collection.get(collection, {})

            self.conn.execute('''
                INSERT INTO collection_statistics (
                    collection, document_count, segment_count, token_count,
                    unique_lemma_count, language_distribution, pos_distribution
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', [
                collection,
                doc_count,
                seg_count,
                tok_count,
                lemma_count,
                json.dumps(lang_dist),
                json.dumps(pos_dist)
            ])

        self.logger.info(f"Computed statistics for {len(collection_stats)} collections")

        click.echo(f"        Collections: {stats['total_collections']:,}")
        click.echo(f"        Documents: {stats['total_documents']:,}")
        click.echo(f"        Segments: {stats['total_segments']:,}")
        click.echo(f"        Tokens: {stats['total_tokens']:,}")
