"""Demo script to test the pipeline with sample data."""

import tempfile
from pathlib import Path

from scripts.ingest.base import IngestorConfig
from scripts.ingest.horner import HornerIngestor
from scripts.utils.log import setup_logging


def run_demo() -> None:
    """Run a demo of the pipeline with sample data."""
    print("=" * 60)
    print("KEMET Data Pipeline Demo")
    print("=" * 60)

    # Setup logging
    logger = setup_logging(level="INFO", format_type="pretty")

    # Create temp directories
    with tempfile.TemporaryDirectory() as tmpdir_str:
        tmpdir = Path(tmpdir_str)

        # Setup paths
        raw_dir = tmpdir / "data" / "raw"
        curated_dir = tmpdir / "data" / "curated"
        derived_dir = tmpdir / "data" / "derived"
        schema_dir = Path(__file__).parent.parent / "etc" / "schemas"

        raw_dir.mkdir(parents=True)
        curated_dir.mkdir(parents=True)
        derived_dir.mkdir(parents=True)

        # Create sample source data
        source_dir = tmpdir / "sample_source"
        source_dir.mkdir()

        sample_text = """Mt 1:1 ⲡϫⲱⲱⲙⲉ ⲙⲡⲉϫⲓⲛⲉ ⲛⲓⲏⲥⲟⲩⲥ ⲡⲉⲭⲣⲓⲥⲧⲟⲥ ⲡϣⲏⲣⲉ ⲛⲇⲁⲩⲉⲓⲇ ⲡϣⲏⲣⲉ ⲛⲁⲃⲣⲁϩⲁⲙ

Mt 1:2 ⲁⲃⲣⲁϩⲁⲙ ⲁϥϫⲡⲟ ⲛⲓⲥⲁⲁⲕ ⲓⲥⲁⲁⲕ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲓⲁⲕⲱⲃ ⲓⲁⲕⲱⲃ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲓⲟⲩⲇⲁⲥ ⲙⲛ ⲛⲉϥⲥⲛⲏⲩ

Mt 1:3 ⲓⲟⲩⲇⲁⲥ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲫⲁⲣⲉⲥ ⲙⲛ ⲍⲁⲣⲁ ⲉⲃⲟⲗ ϩⲛ ⲑⲁⲙⲁⲣ ⲫⲁⲣⲉⲥ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲉⲥⲣⲱⲙ ⲉⲥⲣⲱⲙ ⲇⲉ ⲁϥϫⲡⲟ ⲛⲁⲣⲁⲙ"""

        (source_dir / "matthew.txt").write_text(sample_text, encoding="utf-8")

        # Configure ingestor
        config = IngestorConfig(
            source_name="horner",
            source_config={
                "enabled": True,
                "source_type": "files",
                "path": str(source_dir),
                "collections": ["demo"],
            },
            settings={
                "parquet": {"compression": "zstd", "compression_level": 3},
                "schema_versions": {"manifest": 1},
            },
            paths={
                "raw": raw_dir,
                "curated": curated_dir,
                "derived": derived_dir,
            },
            schema_dir=schema_dir,
        )

        ingestor = HornerIngestor(config, logger)

        print("\n1. Listing collections...")
        collections = ingestor.list_collections()
        print(f"   Found collections: {collections}")

        print("\n2. Pulling raw data...")
        import asyncio

        raw_path = asyncio.run(ingestor.pull_collection("demo"))
        print(f"   Pulled to: {raw_path}")

        print("\n3. Curating data...")
        result = ingestor.curate_collection("demo", raw_path)
        print(f"   Documents: {len(result.documents)}")
        print(f"   Segments: {len(result.segments)}")
        print(f"   Tokens: {len(result.tokens)}")

        print("\n4. Writing curated data...")
        ingestor.write_curated_data("demo", result)

        print("\n5. Sample document:")
        doc = result.documents[0]
        print(f"   ID: {doc.document_id}")
        print(f"   Title: {doc.title}")
        print(f"   Stage: {doc.stage.value}")
        print(f"   Substage: {doc.substage.value}")
        print(f"   Segments: {doc.counts.segments}")

        print("\n6. Sample segments:")
        for i, seg in enumerate(result.segments[:3]):
            print(f"   [{i}] {seg.passage_ref}: {seg.text_canonical[:50]}...")

        print("\n7. Validating schema...")
        from scripts.qc.validate_schema import validate_collection

        validation_result = validate_collection(curated_dir / "horner" / "demo", schema_dir, logger)
        if validation_result.valid:
            print("   ✓ Schema validation passed")
        else:
            print(f"   ✗ Schema validation failed: {validation_result.errors}")

        print("\n" + "=" * 60)
        print("Demo completed successfully!")
        print("=" * 60)


if __name__ == "__main__":
    run_demo()
