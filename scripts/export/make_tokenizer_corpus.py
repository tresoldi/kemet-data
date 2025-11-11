"""Export tokenizer training corpora."""

import logging
from pathlib import Path

import pandas as pd

from scripts.utils.io import ensure_dir


def export_tokenizer_corpus(
    curated_dir: Path,
    output_dir: Path,
    stage: str,
    substage: str | None,
    logger: logging.Logger,
) -> Path:
    """
    Export tokenizer corpus from curated segments.

    Args:
        curated_dir: Path to curated data directory
        output_dir: Path to output directory
        stage: Stage filter (COPTIC or EGYPTIAN)
        substage: Optional substage filter (SAHIDIC, etc.)
        logger: Logger instance

    Returns:
        Path to generated corpus file
    """
    # Build output path
    if substage:
        output_path = output_dir / "tokenizer" / stage / f"{substage}.txt"
    else:
        output_path = output_dir / "tokenizer" / stage / "corpus.txt"

    ensure_dir(output_path.parent)

    # Collect all matching segments
    segment_count = 0

    with output_path.open("w", encoding="utf-8") as out_f:
        # Walk through all sources and collections
        for source_dir in curated_dir.iterdir():
            if not source_dir.is_dir():
                continue

            for collection_dir in source_dir.iterdir():
                if not collection_dir.is_dir():
                    continue

                segments_path = collection_dir / "segments.parquet"
                if not segments_path.exists():
                    continue

                # Read segments
                df = pd.read_parquet(segments_path)

                # Filter by substage if specified
                if substage and "dialect" in df.columns:
                    df = df[df["dialect"] == substage]

                # Write each segment text on one line
                for _, row in df.iterrows():
                    text = row["text_canonical"].strip()
                    if text:
                        out_f.write(text + "\n")
                        segment_count += 1

    logger.info(f"Exported {segment_count} segments to tokenizer corpus: {output_path}")
    return output_path


def export_sentences(
    curated_dir: Path,
    output_dir: Path,
    source: str | None,
    collection: str | None,
    logger: logging.Logger,
) -> Path:
    """
    Export sentence lists for embeddings.

    Args:
        curated_dir: Path to curated data directory
        output_dir: Path to output directory
        source: Optional source filter
        collection: Optional collection filter
        logger: Logger instance

    Returns:
        Path to generated sentences file
    """
    # Build output path
    if source and collection:
        output_path = output_dir / "sentences" / source / f"{collection}.txt"
    elif source:
        output_path = output_dir / "sentences" / f"{source}.txt"
    else:
        output_path = output_dir / "sentences" / "all.txt"

    ensure_dir(output_path.parent)

    segment_count = 0

    with output_path.open("w", encoding="utf-8") as out_f:
        # Walk through matching sources and collections
        for source_dir in curated_dir.iterdir():
            if not source_dir.is_dir():
                continue

            if source and source_dir.name != source:
                continue

            for collection_dir in source_dir.iterdir():
                if not collection_dir.is_dir():
                    continue

                if collection and collection_dir.name != collection:
                    continue

                segments_path = collection_dir / "segments.parquet"
                if not segments_path.exists():
                    continue

                # Read segments
                df = pd.read_parquet(segments_path)

                # Write each segment
                for _, row in df.iterrows():
                    text = row["text_canonical"].strip()
                    if text:
                        out_f.write(text + "\n")
                        segment_count += 1

    logger.info(f"Exported {segment_count} sentences to {output_path}")
    return output_path


def export_shards(
    curated_dir: Path,
    output_dir: Path,
    stage: str,
    shard_size_mb: int,
    logger: logging.Logger,
) -> list[Path]:
    """
    Export sharded training text.

    Args:
        curated_dir: Path to curated data directory
        output_dir: Path to output directory
        stage: Stage filter (COPTIC or EGYPTIAN)
        shard_size_mb: Maximum shard size in MB
        logger: Logger instance

    Returns:
        List of generated shard paths
    """
    shard_dir = output_dir / "shards" / stage
    ensure_dir(shard_dir)

    shard_paths = []
    shard_num = 0
    current_size = 0
    max_size = shard_size_mb * 1024 * 1024  # Convert to bytes

    current_shard = shard_dir / f"{shard_num:04d}.txt"
    out_f = current_shard.open("w", encoding="utf-8")
    shard_paths.append(current_shard)

    segment_count = 0

    try:
        # Walk through all sources and collections
        for source_dir in curated_dir.iterdir():
            if not source_dir.is_dir():
                continue

            for collection_dir in source_dir.iterdir():
                if not collection_dir.is_dir():
                    continue

                segments_path = collection_dir / "segments.parquet"
                if not segments_path.exists():
                    continue

                # Read segments
                df = pd.read_parquet(segments_path)

                # Write each segment
                for _, row in df.iterrows():
                    text = row["text_canonical"].strip()
                    if not text:
                        continue

                    line = text + "\n"
                    line_bytes = line.encode("utf-8")

                    # Check if we need a new shard
                    if current_size + len(line_bytes) > max_size:
                        out_f.close()
                        shard_num += 1
                        current_shard = shard_dir / f"{shard_num:04d}.txt"
                        out_f = current_shard.open("w", encoding="utf-8")
                        shard_paths.append(current_shard)
                        current_size = 0
                        logger.info(f"Created new shard: {current_shard}")

                    out_f.write(line)
                    current_size += len(line_bytes)
                    segment_count += 1

    finally:
        out_f.close()

    logger.info(f"Exported {segment_count} segments to {len(shard_paths)} shards in {shard_dir}")
    return shard_paths
