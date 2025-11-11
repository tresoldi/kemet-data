"""Tests for Coptic Unicode normalization."""

from scripts.normalize.coptic_unicode import (
    is_coptic_codepoint,
    normalize_and_strip,
    normalize_coptic,
    strip_diacritics,
)


def test_normalize_coptic():
    """Test Coptic normalization."""
    text = "ⲁⲩⲱ"
    normalized = normalize_coptic(text, nfc=True)
    assert isinstance(normalized, str)
    assert len(normalized) > 0


def test_strip_diacritics():
    """Test diacritic stripping."""
    # Text with combining diacritics
    text_with_diacritics = "ⲁ\u0300ⲩ\u0301ⲱ"  # with grave and acute
    stripped = strip_diacritics(text_with_diacritics)

    # Should remove combining marks
    assert "\u0300" not in stripped
    assert "\u0301" not in stripped


def test_normalize_and_strip():
    """Test combined normalize and strip."""
    text = "ⲁⲩⲱ"
    canonical, stripped = normalize_and_strip(text)

    assert canonical == text
    assert isinstance(stripped, str)
    assert len(stripped) > 0


def test_is_coptic_codepoint():
    """Test Coptic codepoint detection."""
    # Coptic letter
    assert is_coptic_codepoint("ⲁ")

    # Not Coptic
    assert not is_coptic_codepoint("a")
    assert not is_coptic_codepoint("α")  # Greek
