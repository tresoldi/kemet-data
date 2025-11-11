"""KEMET CLI - Main entry point."""

import asyncio
import importlib
import logging
import sys
from pathlib import Path
from typing import Any

import click
import yaml  # type: ignore[import-untyped]
from tqdm import tqdm

from scripts.ingest.base import IngestorConfig
from scripts.qc.dedup import detect_duplicates
from scripts.qc.unicode_sanity import check_segments_unicode
from scripts.qc.validate_schema import validate_collection
from scripts.utils.io import write_json
from scripts.utils.log import setup_logging


# Root directory
ROOT_DIR = Path(__file__).parent.parent


def load_settings() -> dict[str, Any]:
    """Load settings.yaml."""
    settings_path = ROOT_DIR / "etc" / "settings.yaml"
    if not settings_path.exists():
        click.echo(f"Error: settings.yaml not found at {settings_path}", err=True)
        sys.exit(1)

    with settings_path.open() as f:
        result: dict[str, Any] = yaml.safe_load(f)
        return result


def load_sources() -> dict[str, Any]:
    """Load sources.yaml."""
    sources_path = ROOT_DIR / "etc" / "sources.yaml"
    if not sources_path.exists():
        click.echo("Warning: sources.yaml not found, using empty config", err=True)
        return {"sources": {}}

    with sources_path.open() as f:
        result: dict[str, Any] = yaml.safe_load(f)
        return result


def get_ingestor(
    source: str, settings: dict[str, Any], sources_config: dict[str, Any], logger: logging.Logger
) -> Any:
    """
    Get ingestor instance for source using dynamic import.

    Convention: source name maps to scripts/ingest/{source}.py with {Source}Ingestor class.
    Example: "ramses" -> scripts.ingest.ramses.RamsesIngestor
             "ud_coptic" -> scripts.ingest.ud_coptic.UDCopticIngestor
    """
    source_config = sources_config.get("sources", {}).get(source, {})

    if not source_config.get("enabled", False):
        raise ValueError(f"Source {source} is not enabled in sources.yaml")

    # Build paths
    paths = {
        "raw": ROOT_DIR / settings["paths"]["raw"],
        "curated": ROOT_DIR / settings["paths"]["curated"],
        "derived": ROOT_DIR / settings["paths"]["derived"],
    }

    schema_dir = ROOT_DIR / "etc" / "schemas"

    config = IngestorConfig(
        source_name=source,
        source_config=source_config,
        settings=settings,
        paths=paths,
        schema_dir=schema_dir,
    )

    # Dynamically import ingestor module
    try:
        module = importlib.import_module(f"scripts.ingest.{source}")
    except ImportError as e:
        raise ValueError(f"No ingestor module found for source '{source}': {e}") from e

    # Find the ingestor class in the module (must inherit from BaseIngestor)
    from scripts.ingest.base import BaseIngestor

    ingestor_class = None
    for name in dir(module):
        obj = getattr(module, name)
        if (
            isinstance(obj, type)
            and issubclass(obj, BaseIngestor)
            and obj is not BaseIngestor
            and name.endswith("Ingestor")
        ):
            ingestor_class = obj
            break

    if not ingestor_class:
        raise ValueError(
            f"No ingestor class found in module 'scripts.ingest.{source}'. "
            f"Module should contain a class that inherits from BaseIngestor."
        )

    return ingestor_class(config, logger)


def _auto_curate_all_sources(
    settings: dict[str, Any], sources_config: dict[str, Any], logger: logging.Logger
) -> None:
    """Automatically curate all enabled sources with raw data available."""
    click.echo("\n" + "="*80)
    click.echo("AUTO-CURATION: Curating all enabled sources")
    click.echo("="*80 + "\n")

    raw_dir = ROOT_DIR / settings["paths"]["raw"]
    curated_dir = ROOT_DIR / settings["paths"]["curated"]

    # Ensure curated directory exists
    curated_dir.mkdir(parents=True, exist_ok=True)

    # Get list of enabled sources
    enabled_sources = [
        source_name
        for source_name, source_cfg in sources_config.get("sources", {}).items()
        if source_cfg.get("enabled", False)
    ]

    if not enabled_sources:
        click.echo("WARNING: No enabled sources found in sources.yaml", err=True)
        return

    click.echo(f"Found {len(enabled_sources)} enabled sources: {', '.join(enabled_sources)}\n")

    # Curate each enabled source
    curated_count = 0
    skipped_count = 0

    for source_name in tqdm(enabled_sources, desc="Processing sources", unit="source"):
        try:
            # Check if raw data exists
            source_raw_dir = raw_dir / source_name
            if not source_raw_dir.exists() or not list(source_raw_dir.iterdir()):
                tqdm.write(f"  WARNING: {source_name}: No raw data found, skipping")
                continue

            # Get ingestor
            ingestor = get_ingestor(source_name, settings, sources_config, logger)

            # Get collections
            collections = ingestor.list_collections()
            tqdm.write(f"\n  {source_name}: Processing {len(collections)} collections")

            # Curate each collection
            for coll in tqdm(collections, desc=f"  {source_name}", leave=False, unit="coll"):
                try:
                    raw_path = ingestor.get_collection_raw_dir(coll)
                    if not raw_path.exists():
                        skipped_count += 1
                        continue

                    # Check if already curated
                    curated_coll_dir = ingestor.get_collection_curated_dir(coll)
                    if curated_coll_dir.exists() and list(curated_coll_dir.iterdir()):
                        skipped_count += 1
                        continue

                    # Curate
                    curation_result = ingestor.curate_collection(coll, raw_path)
                    ingestor.write_curated_data(coll, curation_result)
                    curated_count += 1

                except Exception as e:
                    tqdm.write(f"    ERROR: Failed to curate {source_name}/{coll}: {e}")
                    continue

        except Exception as e:
            tqdm.write(f"  ERROR: Failed to process source {source_name}: {e}")
            continue

    # Handle TLA separately (needs special processing)
    if "tla" in enabled_sources:
        try:
            tla_cache_dir = Path.home() / ".cache" / "kemet" / "tla"
            tla_matches_file = tla_cache_dir / "tla_lemma_matches.json"

            if not tla_matches_file.exists():
                click.echo("\n  TLA: Downloading hieroglyph data from Hugging Face...")
                import subprocess
                result = subprocess.run(
                    ["python", "scripts/ingest/tla_huggingface.py"],
                    cwd=ROOT_DIR,
                    capture_output=True,
                    text=True
                )
                if result.returncode != 0:
                    click.echo(f"    ERROR: TLA download failed: {result.stderr}", err=True)
                else:
                    click.echo("    SUCCESS: TLA download completed")
        except Exception as e:
            click.echo(f"  ERROR: Failed to process TLA: {e}", err=True)

    click.echo("\n" + "="*80)
    click.echo("AUTO-CURATION COMPLETE")
    click.echo(f"   Curated: {curated_count} collections")
    click.echo(f"   Skipped: {skipped_count} collections (already curated or missing data)")
    click.echo("="*80 + "\n")


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """KEMET data ingestion and curation CLI."""
    # Load settings
    settings = load_settings()

    # Setup logging
    log_level = "DEBUG" if verbose else settings["logging"]["level"]
    log_format = settings["logging"].get("format", "pretty")
    log_file = ROOT_DIR / settings["logging"].get("file", "kemet.log")

    logger = setup_logging(level=log_level, format_type=log_format, log_file=log_file)

    # Store in context
    ctx.ensure_object(dict)
    ctx.obj["settings"] = settings
    ctx.obj["logger"] = logger


@cli.group()
def data() -> None:
    """Data ingestion and curation commands."""
    pass


@data.command()
@click.option("--source", required=True, help="Source name (horner, scriptorium, etc.)")
@click.option("--collection", help="Specific collection (default: all)")
@click.pass_context
def pull(ctx: click.Context, source: str, collection: str | None) -> None:
    """Pull raw data from source."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]
    sources_config = load_sources()

    try:
        ingestor = get_ingestor(source, settings, sources_config, logger)

        # Get collections to pull
        collections = [collection] if collection else ingestor.list_collections()

        logger.info(f"Pulling {len(collections)} collections from {source}")

        # Pull each collection
        for coll in collections:
            logger.info(f"Pulling collection: {coll}")
            asyncio.run(ingestor.pull_collection(coll))

        click.echo(f"Successfully pulled {len(collections)} collections from {source}")

    except Exception as e:
        logger.error(f"Pull failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@data.command()
@click.option("--source", required=True, help="Source name")
@click.option("--collection", help="Specific collection (default: all)")
@click.pass_context
def curate(ctx: click.Context, source: str, collection: str | None) -> None:
    """Curate raw data into normalized format."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]
    sources_config = load_sources()

    try:
        ingestor = get_ingestor(source, settings, sources_config, logger)

        # Get collections to curate
        collections = [collection] if collection else ingestor.list_collections()

        logger.info(f"Curating {len(collections)} collections from {source}")

        # Curate each collection
        for coll in collections:
            logger.info(f"Curating collection: {coll}")

            raw_path = ingestor.get_collection_raw_dir(coll)
            if not raw_path.exists():
                logger.warning(f"Raw data not found for {coll}, skipping")
                continue

            curation_result = ingestor.curate_collection(coll, raw_path)
            ingestor.write_curated_data(coll, curation_result)

        click.echo(f"Successfully curated {len(collections)} collections from {source}")

    except Exception as e:
        logger.error(f"Curation failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@data.command()
@click.option("--source", required=True, help="Source name")
@click.option("--collection", help="Specific collection (default: all)")
@click.pass_context
def validate(ctx: click.Context, source: str, collection: str | None) -> None:
    """Validate curated data against schemas."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]
    sources_config = load_sources()

    try:
        ingestor = get_ingestor(source, settings, sources_config, logger)
        schema_dir = ROOT_DIR / "etc" / "schemas"

        # Get collections to validate
        collections = [collection] if collection else ingestor.list_collections()

        all_valid = True

        for coll in collections:
            logger.info(f"Validating collection: {coll}")

            curated_dir = ingestor.get_collection_curated_dir(coll)
            if not curated_dir.exists():
                logger.warning(f"Curated data not found for {coll}, skipping")
                continue

            # Schema validation
            result = validate_collection(curated_dir, schema_dir, logger)

            if not result.valid:
                all_valid = False
                click.echo(f"Validation failed for {source}/{coll}:", err=True)
                for error in result.errors:
                    click.echo(f"  ERROR: {error}", err=True)

            for warning in result.warnings:
                click.echo(f"  WARNING: {warning}")

            # Unicode sanity check
            segments_path = curated_dir / "segments.parquet"
            if segments_path.exists():
                unicode_result = check_segments_unicode(segments_path, logger)
                if unicode_result.segments_with_issues > 0:
                    click.echo(
                        f"  Found {unicode_result.segments_with_issues} segments with non-Coptic characters"
                    )

            # Deduplication check
            if segments_path.exists():
                dedup_result = detect_duplicates(segments_path, logger)
                if dedup_result.duplicate_count > 0:
                    click.echo(f"  Found {dedup_result.duplicate_count} duplicate segments")

        if all_valid:
            click.echo("All validations passed")
        else:
            sys.exit(1)

    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@data.command()
@click.pass_context
def catalog(ctx: click.Context) -> None:
    """Rebuild catalog.json."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]

    try:
        from scripts.export.catalog import build_catalog

        catalog_path = ROOT_DIR / settings["paths"]["catalog"]
        curated_dir = ROOT_DIR / settings["paths"]["curated"]

        catalog_data = build_catalog(curated_dir, logger)

        write_json(catalog_path, catalog_data)
        click.echo(f"Catalog written to {catalog_path}")

    except Exception as e:
        logger.error(f"Catalog build failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def database() -> None:
    """Database operations."""
    pass


@database.command()
@click.option("--drop", is_flag=True, help="Drop existing databases first")
@click.option("--auto-curate", is_flag=True, help="Automatically curate all sources if curated data is missing")
@click.pass_context
def build(ctx: click.Context, drop: bool, auto_curate: bool) -> None:
    """Build dual DuckDB databases (corpus + lexicon) from curated data."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]
    sources_config = load_sources()

    try:
        from scripts.database.builder import DatabaseBuilder

        curated_dir = ROOT_DIR / settings["paths"]["curated"]
        derived_dir = ROOT_DIR / settings["paths"]["derived"]
        corpus_db_path = derived_dir / "corpus.duckdb"
        lexicon_db_path = derived_dir / "lexicon.duckdb"

        # Ensure output directory exists
        derived_dir.mkdir(parents=True, exist_ok=True)

        # Auto-curate if requested and curated data is missing/empty
        if auto_curate:
            _auto_curate_all_sources(settings, sources_config, logger)

        builder = DatabaseBuilder(curated_dir, corpus_db_path, lexicon_db_path, logger)
        builder.build(drop_existing=drop)

        click.echo("="*80)
        click.echo("DATABASE BUILD COMPLETE")
        click.echo("="*80)
        click.echo("\nDatabases successfully created:")
        click.echo(f"   Corpus:  {corpus_db_path}")
        click.echo(f"   Lexicon: {lexicon_db_path}\n")

    except Exception as e:
        logger.error(f"Database build failed: {e}", exc_info=True)
        click.echo(f"\nERROR: {e}\n", err=True)
        sys.exit(1)


@database.command()
@click.pass_context
def stats(ctx: click.Context) -> None:
    """Show database statistics."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]

    try:
        import duckdb

        derived_dir = ROOT_DIR / settings["paths"]["derived"]
        corpus_db_path = derived_dir / "corpus.duckdb"
        lexicon_db_path = derived_dir / "lexicon.duckdb"

        # Check if databases exist
        if not corpus_db_path.exists():
            click.echo(f"Error: Database not found: {corpus_db_path}. Run 'kemet database build' first.", err=True)
            sys.exit(1)
        if not lexicon_db_path.exists():
            click.echo(f"Error: Database not found: {lexicon_db_path}. Run 'kemet database build' first.", err=True)
            sys.exit(1)

        click.echo("\n" + "="*80)
        click.echo("CORPUS DATABASE STATISTICS")
        click.echo("="*80 + "\n")

        # Query corpus database
        with duckdb.connect(str(corpus_db_path), read_only=True) as corpus_conn:
            # Overall corpus stats
            row = corpus_conn.execute("""
                SELECT
                    COUNT(*) as total_documents,
                    SUM(num_segments) as total_segments,
                    SUM(num_tokens) as total_tokens
                FROM documents
            """).fetchone()
            assert row is not None

            click.echo("Overall Corpus:")
            click.echo(f"  Documents: {row[0]:,}")
            click.echo(f"  Segments:  {row[1]:,}")
            click.echo(f"  Tokens:    {row[2]:,}")

            # Stats by language
            click.echo("\nBy Language:")
            results: list[tuple[Any, ...]] = corpus_conn.execute("""
                SELECT
                    language,
                    COUNT(*) as docs,
                    SUM(num_segments) as segments,
                    SUM(num_tokens) as tokens
                FROM documents
                GROUP BY language
                ORDER BY tokens DESC
            """).fetchall()

            for row in results:
                click.echo(f"  {row[0]:10s}  {row[1]:6,} docs  {row[2]:8,} segments  {row[3]:10,} tokens")

            # Stats by stage
            click.echo("\nBy Stage:")
            stage_results: list[tuple[Any, ...]] = corpus_conn.execute("""
                SELECT
                    stage,
                    COUNT(*) as docs,
                    SUM(num_segments) as segments,
                    SUM(num_tokens) as tokens
                FROM documents
                GROUP BY stage
                ORDER BY tokens DESC
            """).fetchall()

            for row in stage_results:
                click.echo(f"  {row[0]:10s}  {row[1]:6,} docs  {row[2]:8,} segments  {row[3]:10,} tokens")

            # Stats by source
            click.echo("\nBy Source:")
            source_results: list[tuple[Any, ...]] = corpus_conn.execute("""
                SELECT
                    source,
                    COUNT(*) as docs,
                    SUM(num_segments) as segments,
                    SUM(num_tokens) as tokens
                FROM documents
                GROUP BY source
                ORDER BY tokens DESC
            """).fetchall()

            for row in source_results:
                click.echo(f"  {row[0]:15s}  {row[1]:6,} docs  {row[2]:8,} segments  {row[3]:10,} tokens")

        click.echo("\n" + "="*80)
        click.echo("LEXICON DATABASE STATISTICS")
        click.echo("="*80 + "\n")

        # Query lexicon database
        with duckdb.connect(str(lexicon_db_path), read_only=True) as lexicon_conn:
            # Overall lexicon stats
            row = lexicon_conn.execute("""
                SELECT
                    COUNT(*) as total_lemmas
                FROM lemmas
            """).fetchone()
            assert row is not None

            click.echo("Overall Lexicon:")
            click.echo(f"  Lemmas:    {row[0]:,}")

            # Lemma forms stats
            row = lexicon_conn.execute("""
                SELECT
                    COUNT(*) as total_forms,
                    COUNT(DISTINCT lemma_id) as lemmas_with_forms
                FROM forms
            """).fetchone()
            assert row is not None

            click.echo(f"  Forms:     {row[0]:,}")
            click.echo(f"  Lemmas with forms: {row[1]:,}")

            # Attestation stats
            row = lexicon_conn.execute("""
                SELECT
                    COUNT(*) as total_attestations,
                    COUNT(DISTINCT lemma_id) as lemmas_with_attestations
                FROM lemma_attestations
            """).fetchone()
            assert row is not None

            click.echo(f"  Attestations: {row[0]:,}")
            click.echo(f"  Lemmas with attestations: {row[1]:,}")

            # Collocation stats
            row = lexicon_conn.execute("""
                SELECT
                    COUNT(*) as total_collocations
                FROM collocations
            """).fetchone()
            assert row is not None

            click.echo(f"  Collocations: {row[0]:,}")

            # Stats by language
            click.echo("\nLemmas by Language:")
            lang_results: list[tuple[Any, ...]] = lexicon_conn.execute("""
                SELECT
                    language,
                    COUNT(*) as lemmas
                FROM lemmas
                GROUP BY language
                ORDER BY lemmas DESC
            """).fetchall()

            for row in lang_results:
                click.echo(f"  {row[0]:10s}  {row[1]:6,} lemmas")

            # Top 10 most frequent lemmas
            click.echo("\nTop 10 Most Frequent Lemmas:")
            freq_results: list[tuple[Any, ...]] = lexicon_conn.execute("""
                SELECT
                    lemma,
                    language,
                    pos,
                    frequency,
                    document_count
                FROM lemmas
                WHERE frequency > 0
                ORDER BY frequency DESC
                LIMIT 10
            """).fetchall()

            for idx, row in enumerate(freq_results, 1):
                click.echo(f"  {idx:2d}. {row[0]:20s} ({row[1]:3s}, {row[2] or 'N/A':10s})  freq: {row[3]:6,}  docs: {row[4]:5,}")

        click.echo("\n" + "="*80 + "\n")

    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        click.echo("Run 'kemet database build' first.", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Stats failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@database.command()
@click.argument("sql")
@click.option("--limit", default=100, help="Maximum rows to display")
@click.option("--database", type=click.Choice(["corpus", "lexicon"]), default="corpus", help="Database to query (default: corpus)")
@click.pass_context
def query(ctx: click.Context, sql: str, limit: int, database: str) -> None:
    """Execute SQL query against database."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]

    try:
        import duckdb

        derived_dir = ROOT_DIR / settings["paths"]["derived"]
        db_filename = f"{database}.duckdb"
        db_path = derived_dir / db_filename

        if not db_path.exists():
            click.echo(f"Error: Database not found: {db_path}. Run 'kemet database build' first.", err=True)
            sys.exit(1)

        with duckdb.connect(str(db_path), read_only=True) as conn:
            result = conn.execute(sql).fetchdf()

            if len(result) == 0:
                click.echo("No results")
                return

            # Display results
            if len(result) > limit:
                click.echo(f"Showing first {limit} of {len(result)} rows:\n")
                click.echo(result.head(limit).to_string())
            else:
                click.echo(f"Found {len(result)} rows:\n")
                click.echo(result.to_string())

    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        click.echo("Run 'kemet database build' first.", err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.group()
def export() -> None:
    """Export commands for downstream tasks."""
    pass


@export.command()
@click.option("--stage", required=True, type=click.Choice(["COPTIC", "EGYPTIAN"]))
@click.option("--substage", help="Optional substage filter (SAHIDIC, etc.)")
@click.pass_context
def tokenizer(ctx: click.Context, stage: str, substage: str | None) -> None:
    """Export tokenizer training corpus."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]

    try:
        from scripts.export.make_tokenizer_corpus import export_tokenizer_corpus

        curated_dir = ROOT_DIR / settings["paths"]["curated"]
        output_dir = ROOT_DIR / settings["paths"]["derived"] / "exports"

        output_path = export_tokenizer_corpus(curated_dir, output_dir, stage, substage, logger)

        click.echo(f"Tokenizer corpus written to {output_path}")

    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@export.command()
@click.option("--source", help="Source filter")
@click.option("--collection", help="Collection filter")
@click.pass_context
def sentences(ctx: click.Context, source: str | None, collection: str | None) -> None:
    """Export sentence lists for embeddings."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]

    try:
        from scripts.export.make_tokenizer_corpus import export_sentences

        curated_dir = ROOT_DIR / settings["paths"]["curated"]
        output_dir = ROOT_DIR / settings["paths"]["derived"] / "exports"

        output_path = export_sentences(curated_dir, output_dir, source, collection, logger)

        click.echo(f"Sentences written to {output_path}")

    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@export.command()
@click.option("--stage", required=True, type=click.Choice(["COPTIC", "EGYPTIAN"]))
@click.option("--size", default=10, help="Shard size in MB (default: 10)")
@click.pass_context
def shards(ctx: click.Context, stage: str, size: int) -> None:
    """Export sharded training text."""
    logger = ctx.obj["logger"]
    settings = ctx.obj["settings"]

    try:
        from scripts.export.make_tokenizer_corpus import export_shards

        curated_dir = ROOT_DIR / settings["paths"]["curated"]
        output_dir = ROOT_DIR / settings["paths"]["derived"] / "exports"

        shard_paths = export_shards(curated_dir, output_dir, stage, size, logger)

        click.echo(f"Created {len(shard_paths)} shards")

    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def main() -> None:
    """Entry point."""
    cli(obj={})


if __name__ == "__main__":
    main()
