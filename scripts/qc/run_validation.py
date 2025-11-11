"""Comprehensive validation suite for KEMET corpus."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from scripts.qc.dedup import DedupResult, detect_duplicates
from scripts.qc.unicode_sanity import UnicodeSanityResult, check_segments_unicode
from scripts.qc.validate_schema import ValidationResult, validate_collection
from scripts.utils.log import setup_logging


@dataclass
class DatabaseValidationResult:
    """Results from database-level validation."""

    total_collections: int
    total_documents: int
    total_segments: int
    total_tokens: int

    # Integrity checks
    orphaned_segments: int  # Segments without valid document_id
    orphaned_tokens: int  # Tokens without valid segment_id
    missing_metadata: dict[str, int]  # Fields with null/empty values

    # Data quality metrics
    avg_tokens_per_segment: float
    avg_segments_per_document: float
    collections_by_source: dict[str, int]
    tokens_by_language: dict[str, int]

    # Issues found
    issues: list[str]
    warnings: list[str]


@dataclass
class ComprehensiveValidationResult:
    """Complete validation results."""

    database_validation: DatabaseValidationResult
    collection_validations: dict[str, Any]  # collection_name -> ValidationResult
    duplicate_results: dict[str, Any]  # collection_name -> DedupResult
    unicode_results: dict[str, Any]  # collection_name -> UnicodeSanityResult

    summary: dict[str, Any]


def validate_database(
    db_path: Path,
    logger: logging.Logger,
) -> DatabaseValidationResult:
    """
    Run database-level validation checks.

    Args:
        db_path: Path to DuckDB database
        logger: Logger instance

    Returns:
        Database validation results
    """
    logger.info("=" * 80)
    logger.info("RUNNING DATABASE-LEVEL VALIDATION")
    logger.info("=" * 80)

    issues = []
    warnings = []

    # Connect to database
    con = duckdb.connect(str(db_path), read_only=True)

    try:
        # Get basic counts
        row = con.execute("SELECT COUNT(DISTINCT collection) FROM documents").fetchone()
        assert row is not None
        total_collections = row[0]
        row = con.execute("SELECT COUNT(*) FROM documents").fetchone()
        assert row is not None
        total_documents = row[0]
        row = con.execute("SELECT COUNT(*) FROM segments").fetchone()
        assert row is not None
        total_segments = row[0]
        row = con.execute("SELECT COUNT(*) FROM tokens").fetchone()
        assert row is not None
        total_tokens = row[0]

        logger.info(f"Total collections: {total_collections:,}")
        logger.info(f"Total documents: {total_documents:,}")
        logger.info(f"Total segments: {total_segments:,}")
        logger.info(f"Total tokens: {total_tokens:,}")

        # Check referential integrity: orphaned segments
        orphaned_segments_query = """
        SELECT COUNT(*)
        FROM segments s
        LEFT JOIN documents d ON s.document_id = d.document_id
        WHERE d.document_id IS NULL
        """
        row = con.execute(orphaned_segments_query).fetchone()
        assert row is not None
        orphaned_segments = row[0]

        if orphaned_segments > 0:
            issues.append(f"Found {orphaned_segments:,} orphaned segments (no matching document)")
            logger.error(f"❌ {issues[-1]}")
        else:
            logger.info("✓ No orphaned segments found")

        # Check referential integrity: orphaned tokens
        orphaned_tokens_query = """
        SELECT COUNT(*)
        FROM tokens t
        LEFT JOIN segments s ON t.segment_id = s.segment_id
        WHERE s.segment_id IS NULL
        """
        row = con.execute(orphaned_tokens_query).fetchone()
        assert row is not None
        orphaned_tokens = row[0]

        if orphaned_tokens > 0:
            issues.append(f"Found {orphaned_tokens:,} orphaned tokens (no matching segment)")
            logger.error(f"❌ {issues[-1]}")
        else:
            logger.info("✓ No orphaned tokens found")

        # Check for missing critical metadata
        missing_metadata = {}

        # Documents with missing fields
        row = con.execute(
            "SELECT COUNT(*) FROM documents WHERE title IS NULL OR title = ''"
        ).fetchone()
        assert row is not None
        missing_metadata['documents_no_title'] = row[0]

        row = con.execute(
            "SELECT COUNT(*) FROM documents WHERE stage IS NULL"
        ).fetchone()
        assert row is not None
        missing_metadata['documents_no_stage'] = row[0]

        row = con.execute(
            "SELECT COUNT(*) FROM documents WHERE substage IS NULL"
        ).fetchone()
        assert row is not None
        missing_metadata['documents_no_substage'] = row[0]

        # Segments with missing text
        row = con.execute(
            "SELECT COUNT(*) FROM segments WHERE text_canonical IS NULL OR text_canonical = ''"
        ).fetchone()
        assert row is not None
        missing_metadata['segments_no_text'] = row[0]

        # Tokens with missing form
        row = con.execute(
            "SELECT COUNT(*) FROM tokens WHERE form IS NULL OR form = ''"
        ).fetchone()
        assert row is not None
        missing_metadata['tokens_no_form'] = row[0]

        for field, count in missing_metadata.items():
            if count > 0:
                warnings.append(f"{field}: {count:,} records")
                logger.warning(f"⚠ {warnings[-1]}")

        if not warnings:
            logger.info("✓ No missing critical metadata")

        # Calculate quality metrics
        avg_tokens_per_segment = total_tokens / total_segments if total_segments > 0 else 0
        avg_segments_per_document = total_segments / total_documents if total_documents > 0 else 0

        logger.info(f"Average tokens per segment: {avg_tokens_per_segment:.2f}")
        logger.info(f"Average segments per document: {avg_segments_per_document:.2f}")

        # Get collection counts by source
        collections_by_source = {}
        source_query = "SELECT source, COUNT(DISTINCT collection) as cnt FROM documents GROUP BY source ORDER BY cnt DESC"
        for row in con.execute(source_query).fetchall():
            collections_by_source[row[0]] = row[1]
            logger.info(f"  {row[0]}: {row[1]} collections")

        # Get token counts by language
        tokens_by_language = {}
        lang_query = "SELECT lang, COUNT(*) as cnt FROM tokens GROUP BY lang ORDER BY cnt DESC"
        for row in con.execute(lang_query).fetchall():
            tokens_by_language[row[0]] = row[1]
            logger.info(f"  {row[0]}: {row[1]:,} tokens")

        # Check for duplicate document IDs
        dup_doc_ids = con.execute(
            "SELECT document_id, COUNT(*) as cnt FROM documents GROUP BY document_id HAVING cnt > 1"
        ).fetchall()

        if dup_doc_ids:
            issues.append(f"Found {len(dup_doc_ids)} duplicate document IDs")
            logger.error(f"❌ {issues[-1]}")
            for doc_id, count in dup_doc_ids[:5]:  # Show first 5
                logger.error(f"  {doc_id}: {count} occurrences")
        else:
            logger.info("✓ No duplicate document IDs")

        # Check for duplicate segment IDs
        dup_seg_ids = con.execute(
            "SELECT segment_id, COUNT(*) as cnt FROM segments GROUP BY segment_id HAVING cnt > 1"
        ).fetchall()

        if dup_seg_ids:
            issues.append(f"Found {len(dup_seg_ids)} duplicate segment IDs")
            logger.error(f"❌ {issues[-1]}")
            for seg_id, count in dup_seg_ids[:5]:  # Show first 5
                logger.error(f"  {seg_id}: {count} occurrences")
        else:
            logger.info("✓ No duplicate segment IDs")

        # Check for very long or very short segments
        row = con.execute(
            "SELECT COUNT(*) FROM segments WHERE LENGTH(text_canonical) < 3"
        ).fetchone()
        assert row is not None
        very_short = row[0]

        if very_short > total_segments * 0.05:  # More than 5%
            warnings.append(f"{very_short:,} very short segments (<3 chars) - {very_short/total_segments*100:.1f}%")
            logger.warning(f"⚠ {warnings[-1]}")

        row = con.execute(
            "SELECT COUNT(*) FROM segments WHERE LENGTH(text_canonical) > 10000"
        ).fetchone()
        assert row is not None
        very_long = row[0]

        if very_long > 0:
            warnings.append(f"{very_long:,} very long segments (>10k chars)")
            logger.warning(f"⚠ {warnings[-1]}")

        result = DatabaseValidationResult(
            total_collections=total_collections,
            total_documents=total_documents,
            total_segments=total_segments,
            total_tokens=total_tokens,
            orphaned_segments=orphaned_segments,
            orphaned_tokens=orphaned_tokens,
            missing_metadata=missing_metadata,
            avg_tokens_per_segment=avg_tokens_per_segment,
            avg_segments_per_document=avg_segments_per_document,
            collections_by_source=collections_by_source,
            tokens_by_language=tokens_by_language,
            issues=issues,
            warnings=warnings,
        )

    finally:
        con.close()

    return result


def validate_curated_data(
    curated_dir: Path,
    schema_dir: Path,
    logger: logging.Logger,
    max_collections: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """
    Run validation checks on curated data files.

    Args:
        curated_dir: Path to data_curated directory
        schema_dir: Path to schemas directory
        logger: Logger instance
        max_collections: Maximum number of collections to validate (None = all)

    Returns:
        Tuple of (collection_validations, duplicate_results, unicode_results)
    """
    logger.info("")
    logger.info("=" * 80)
    logger.info("RUNNING CURATED DATA VALIDATION")
    logger.info("=" * 80)

    collection_validations = {}
    duplicate_results = {}
    unicode_results = {}

    # Find all collection directories
    collection_dirs = []
    for source_dir in curated_dir.iterdir():
        if source_dir.is_dir() and not source_dir.name.startswith('.'):
            for collection_dir in source_dir.iterdir():
                if collection_dir.is_dir() and not collection_dir.name.startswith('.'):
                    collection_dirs.append(collection_dir)

    if max_collections:
        collection_dirs = collection_dirs[:max_collections]

    logger.info(f"Found {len(collection_dirs)} collections to validate")

    for idx, collection_dir in enumerate(collection_dirs):
        collection_name = f"{collection_dir.parent.name}/{collection_dir.name}"
        logger.info(f"\n[{idx+1}/{len(collection_dirs)}] Validating {collection_name}...")

        # Schema validation
        try:
            validation_result = validate_collection(collection_dir, schema_dir, logger)
            collection_validations[collection_name] = validation_result

            if validation_result.valid:
                logger.info("  ✓ Schema validation passed")
            else:
                logger.error(f"  ❌ Schema validation failed: {len(validation_result.errors)} errors")
                for error in validation_result.errors[:3]:  # Show first 3
                    logger.error(f"    - {error}")

            if validation_result.warnings:
                logger.warning(f"  ⚠ {len(validation_result.warnings)} warnings")

        except Exception as e:
            logger.error(f"  ❌ Schema validation error: {e}")
            # Skip this collection on error
            pass

        # Duplicate detection
        segments_file = collection_dir / "segments.parquet"
        if segments_file.exists():
            try:
                dedup_result = detect_duplicates(segments_file, logger)
                duplicate_results[collection_name] = dedup_result

                if dedup_result.duplicate_count > 0:
                    dup_pct = dedup_result.duplicate_count / dedup_result.total_segments * 100
                    logger.warning(f"  ⚠ {dedup_result.duplicate_count:,} duplicates ({dup_pct:.1f}%)")
                else:
                    logger.info("  ✓ No duplicates found")

            except Exception as e:
                logger.error(f"  ❌ Duplicate detection error: {e}")
                # Skip this collection on error
                pass

        # Unicode sanity check (for Coptic texts)
        if ('coptic' in collection_name.lower() or 'scriptorium' in collection_name.lower()) and segments_file.exists():
            try:
                unicode_result = check_segments_unicode(segments_file, logger, max_examples=5)
                unicode_results[collection_name] = unicode_result

                if unicode_result.segments_with_issues > 0:
                    issue_pct = unicode_result.segments_with_issues / unicode_result.total_segments * 100
                    logger.warning(
                        f"  ⚠ {unicode_result.segments_with_issues:,} segments "
                        f"with non-Coptic chars ({issue_pct:.1f}%)"
                    )
                else:
                    logger.info("  ✓ All segments use valid Coptic Unicode")

            except Exception as e:
                logger.error(f"  ❌ Unicode check error: {e}")
                # Skip this collection on error
                pass

    return collection_validations, duplicate_results, unicode_results


def generate_validation_report(
    result: ComprehensiveValidationResult,
    output_path: Path,
    logger: logging.Logger,
) -> None:
    """
    Generate validation report.

    Args:
        result: Comprehensive validation results
        output_path: Path to output report file
        logger: Logger instance
    """
    logger.info("")
    logger.info("=" * 80)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 80)

    db_val = result.database_validation

    # Overall statistics
    logger.info("\nCorpus Statistics:")
    logger.info(f"  Collections: {db_val.total_collections:,}")
    logger.info(f"  Documents: {db_val.total_documents:,}")
    logger.info(f"  Segments: {db_val.total_segments:,}")
    logger.info(f"  Tokens: {db_val.total_tokens:,}")
    logger.info(f"  Avg tokens/segment: {db_val.avg_tokens_per_segment:.2f}")
    logger.info(f"  Avg segments/document: {db_val.avg_segments_per_document:.2f}")

    # Critical issues
    critical_issues = len(db_val.issues)
    if critical_issues > 0:
        logger.error(f"\n❌ CRITICAL ISSUES: {critical_issues}")
        for issue in db_val.issues:
            logger.error(f"  - {issue}")
    else:
        logger.info("\n✓ No critical issues found")

    # Warnings
    if db_val.warnings:
        logger.warning(f"\n⚠ WARNINGS: {len(db_val.warnings)}")
        for warning in db_val.warnings:
            logger.warning(f"  - {warning}")

    # Collection-level results
    total_collections_validated = len(result.collection_validations)
    failed_collections = sum(
        1 if (isinstance(v, ValidationResult) and not v.valid) else 0
        for v in result.collection_validations.values()
    )

    if total_collections_validated > 0:
        logger.info("\nCollection Validation:")
        logger.info(f"  Validated: {total_collections_validated} collections")
        if failed_collections > 0:
            logger.error(f"  ❌ Failed: {failed_collections}")
        else:
            logger.info("  ✓ All collections passed")

    # Duplicate summary
    total_duplicates = sum(
        r.duplicate_count if isinstance(r, DedupResult) else 0
        for r in result.duplicate_results.values()
    )
    if total_duplicates > 0:
        logger.warning("\nDuplicates Found:")
        logger.warning(f"  Total duplicate segments: {total_duplicates:,}")
        logger.warning("  This is expected for cross-referenced texts")

    # Unicode issues summary
    total_unicode_issues = sum(
        r.segments_with_issues if isinstance(r, UnicodeSanityResult) else 0
        for r in result.unicode_results.values()
    )
    if total_unicode_issues > 0:
        logger.warning("\nUnicode Issues:")
        logger.warning(f"  Segments with non-Coptic chars: {total_unicode_issues:,}")

    # Overall assessment
    logger.info("")
    logger.info("=" * 80)
    if critical_issues == 0 and failed_collections == 0:
        logger.info("✅ VALIDATION PASSED - Corpus is ready for use")
    elif critical_issues == 0:
        logger.warning("⚠ VALIDATION PASSED WITH WARNINGS - Review warnings before production use")
    else:
        logger.error("❌ VALIDATION FAILED - Critical issues must be fixed")
    logger.info("=" * 80)

    # Write detailed JSON report
    summary = {
        'database': {
            'total_collections': db_val.total_collections,
            'total_documents': db_val.total_documents,
            'total_segments': db_val.total_segments,
            'total_tokens': db_val.total_tokens,
            'avg_tokens_per_segment': db_val.avg_tokens_per_segment,
            'avg_segments_per_document': db_val.avg_segments_per_document,
            'collections_by_source': db_val.collections_by_source,
            'tokens_by_language': db_val.tokens_by_language,
            'orphaned_segments': db_val.orphaned_segments,
            'orphaned_tokens': db_val.orphaned_tokens,
            'missing_metadata': db_val.missing_metadata,
            'issues': db_val.issues,
            'warnings': db_val.warnings,
        },
        'collections': {
            'total_validated': total_collections_validated,
            'failed': failed_collections,
            'passed': total_collections_validated - failed_collections,
        },
        'duplicates': {
            'total_duplicate_segments': total_duplicates,
        },
        'unicode': {
            'total_segments_with_issues': total_unicode_issues,
        },
        'overall_status': 'PASSED' if critical_issues == 0 and failed_collections == 0 else 'FAILED' if critical_issues > 0 else 'PASSED_WITH_WARNINGS',
    }

    import json
    with output_path.open('w') as f:
        json.dump(summary, f, indent=2)

    logger.info(f"\nDetailed report written to: {output_path}")


def run_comprehensive_validation(
    db_path: Path,
    curated_dir: Path,
    schema_dir: Path,
    output_path: Path,
    max_collections: int | None = None,
) -> ComprehensiveValidationResult:
    """
    Run comprehensive validation suite.

    Args:
        db_path: Path to DuckDB database
        curated_dir: Path to data_curated directory
        schema_dir: Path to schemas directory
        output_path: Path to output report
        max_collections: Maximum number of collections to validate

    Returns:
        Comprehensive validation results
    """
    logger = setup_logging("INFO", "pretty")

    logger.info("=" * 80)
    logger.info("KEMET CORPUS VALIDATION SUITE")
    logger.info("=" * 80)
    logger.info(f"Database: {db_path}")
    logger.info(f"Curated data: {curated_dir}")
    logger.info(f"Schema dir: {schema_dir}")

    # Run database validation
    db_validation = validate_database(db_path, logger)

    # Run curated data validation
    collection_vals, dup_results, unicode_results = validate_curated_data(
        curated_dir, schema_dir, logger, max_collections
    )

    # Compile results
    result = ComprehensiveValidationResult(
        database_validation=db_validation,
        collection_validations=collection_vals,
        duplicate_results=dup_results,
        unicode_results=unicode_results,
        summary={},
    )

    # Generate report
    generate_validation_report(result, output_path, logger)

    return result


if __name__ == "__main__":
    import sys

    # Paths
    ROOT_DIR = Path(__file__).parent.parent.parent
    db_path = ROOT_DIR / "data" / "derived" / "kemet.duckdb"
    curated_dir = ROOT_DIR / "data" / "curated"
    schema_dir = ROOT_DIR / "etc" / "schemas"
    output_path = ROOT_DIR / "validation_report.json"

    # Run validation
    max_collections = int(sys.argv[1]) if len(sys.argv) > 1 else None

    result = run_comprehensive_validation(
        db_path,
        curated_dir,
        schema_dir,
        output_path,
        max_collections,
    )

    # Exit with appropriate code
    if result.database_validation.issues:
        sys.exit(1)
    else:
        sys.exit(0)
