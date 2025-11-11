"""Deduplication detection for segments."""

import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class DedupResult:
    """Result of deduplication check."""

    total_segments: int
    unique_hashes: int
    duplicate_count: int
    duplicate_groups: list[list[str]]


def detect_duplicates(
    segments_path: Path,
    logger: logging.Logger,
) -> DedupResult:
    """
    Detect duplicate segments by content hash.

    Args:
        segments_path: Path to segments.parquet
        logger: Logger instance

    Returns:
        Deduplication result
    """
    if not segments_path.exists():
        logger.warning(f"Segments file not found: {segments_path}")
        return DedupResult(
            total_segments=0,
            unique_hashes=0,
            duplicate_count=0,
            duplicate_groups=[],
        )

    # Read segments
    df = pd.read_parquet(segments_path)

    # Group by content hash
    hash_to_segments: dict[str, list[str]] = defaultdict(list)

    for _, row in df.iterrows():
        content_hash = row["content_hash"]
        segment_id = row["segment_id"]
        hash_to_segments[content_hash].append(segment_id)

    # Find duplicate groups
    duplicate_groups = [segments for segments in hash_to_segments.values() if len(segments) > 1]

    duplicate_count = sum(len(group) - 1 for group in duplicate_groups)

    logger.info(f"Found {duplicate_count} duplicate segments in {len(duplicate_groups)} groups")

    return DedupResult(
        total_segments=len(df),
        unique_hashes=len(hash_to_segments),
        duplicate_count=duplicate_count,
        duplicate_groups=duplicate_groups,
    )


def flag_duplicates(
    segments_path: Path,
    output_path: Path,
    logger: logging.Logger,
) -> None:
    """
    Flag duplicate segments in output file.

    Args:
        segments_path: Path to segments.parquet
        output_path: Path to output parquet with dup_flag
        logger: Logger instance
    """
    if not segments_path.exists():
        logger.warning(f"Segments file not found: {segments_path}")
        return

    # Read segments
    df = pd.read_parquet(segments_path)

    # Count hash occurrences
    hash_counts = Counter(df["content_hash"])

    # Add dup_flag column
    df["dup_flag"] = df["content_hash"].map(lambda h: hash_counts[h] > 1)

    # Write output
    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="zstd",
        index=False,
    )

    dup_count = df["dup_flag"].sum()
    logger.info(f"Flagged {dup_count} duplicate segments in {output_path}")
