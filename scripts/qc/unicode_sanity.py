"""Unicode sanity checks for Coptic texts."""

import logging
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from scripts.normalize.coptic_unicode import get_non_coptic_chars


@dataclass
class UnicodeSanityResult:
    """Result of Unicode sanity check."""

    total_segments: int
    segments_with_issues: int
    non_coptic_chars: Counter[str]
    examples: list[dict[str, str]]


def check_segments_unicode(
    segments_path: Path,
    logger: logging.Logger,
    max_examples: int = 10,
) -> UnicodeSanityResult:
    """
    Check Unicode sanity of segments file.

    Args:
        segments_path: Path to segments.parquet
        logger: Logger instance
        max_examples: Maximum number of examples to collect

    Returns:
        Unicode sanity check result
    """
    if not segments_path.exists():
        logger.warning(f"Segments file not found: {segments_path}")
        return UnicodeSanityResult(
            total_segments=0,
            segments_with_issues=0,
            non_coptic_chars=Counter(),
            examples=[],
        )

    # Read segments
    df = pd.read_parquet(segments_path)

    non_coptic_chars: Counter[str] = Counter()
    segments_with_issues = 0
    examples: list[dict[str, str]] = []

    # Check each segment
    for _idx, row in df.iterrows():
        text = row["text_canonical"]
        chars = get_non_coptic_chars(text)

        if chars:
            segments_with_issues += 1
            non_coptic_chars.update(chars)

            # Collect examples
            if len(examples) < max_examples:
                examples.append(
                    {
                        "segment_id": row["segment_id"],
                        "text": text[:100],  # First 100 chars
                        "non_coptic": ", ".join(sorted(chars)),
                    }
                )

    logger.info(f"Found {segments_with_issues}/{len(df)} segments with non-Coptic characters")

    return UnicodeSanityResult(
        total_segments=len(df),
        segments_with_issues=segments_with_issues,
        non_coptic_chars=non_coptic_chars,
        examples=examples,
    )
