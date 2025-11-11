"""Text segmentation utilities."""

import re


def segment_by_sentence(text: str, min_length: int = 10) -> list[str]:
    """
    Segment text into sentences using rule-based approach.

    Args:
        text: Input text
        min_length: Minimum segment length in characters

    Returns:
        List of sentence segments
    """
    # Split on common sentence terminators
    # Handles: period, question mark, exclamation, em dash
    pattern = r"[.!?;]+\s+|—\s+"

    segments = re.split(pattern, text)

    # Clean and filter segments
    cleaned = []
    for seg in segments:
        seg = seg.strip()
        if len(seg) >= min_length:
            cleaned.append(seg)

    return cleaned


def segment_by_verse(text: str) -> list[tuple[str, str]]:
    """
    Segment text by verse markers (e.g., "John 3:16").

    Args:
        text: Input text with verse markers

    Returns:
        List of (reference, text) tuples
    """
    # Pattern for biblical references like "John 3:16" or "Mt 5:3"
    # This is a simplified pattern - can be expanded
    verse_pattern = r"((?:[1-3]\s)?[A-Z][a-z]+\s+\d+:\d+)"

    # Split by verse references
    parts = re.split(f"({verse_pattern})", text)

    verses = []
    current_ref = None

    for _i, part in enumerate(parts):
        part = part.strip()
        if not part:
            continue

        # Check if this looks like a reference
        if re.match(verse_pattern, part):
            current_ref = part
        elif current_ref:
            verses.append((current_ref, part))
            current_ref = None

    return verses


def segment_by_lines(text: str, min_length: int = 5) -> list[str]:
    """
    Segment text by lines (conservative approach).

    Args:
        text: Input text
        min_length: Minimum line length

    Returns:
        List of line segments
    """
    lines = text.split("\n")
    return [line.strip() for line in lines if len(line.strip()) >= min_length]


def segment_by_blank_lines(text: str) -> list[str]:
    """
    Segment text by blank lines (paragraph breaks).

    Args:
        text: Input text

    Returns:
        List of paragraph segments
    """
    # Split on one or more blank lines
    paragraphs = re.split(r"\n\s*\n", text)
    return [p.strip() for p in paragraphs if p.strip()]
