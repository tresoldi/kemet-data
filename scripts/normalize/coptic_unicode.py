"""Coptic Unicode normalization utilities."""

import unicodedata


# Coptic Unicode block: U+2C80 to U+2CFF
COPTIC_BLOCK_START = 0x2C80
COPTIC_BLOCK_END = 0x2CFF

# Coptic diacritics to strip (combining marks commonly used in Coptic)
COPTIC_DIACRITICS = {
    "\u0300",  # Combining grave accent
    "\u0301",  # Combining acute accent
    "\u0302",  # Combining circumflex accent
    "\u0303",  # Combining tilde
    "\u0304",  # Combining macron
    "\u0305",  # Combining overline
    "\u0306",  # Combining breve
    "\u0307",  # Combining dot above
    "\u0308",  # Combining diaeresis
    "\u030a",  # Combining ring above
    "\u030b",  # Combining double acute
    "\u030c",  # Combining caron
    "\u0323",  # Combining dot below
    "\u0325",  # Combining ring below
    "\u0331",  # Combining macron below
    "\u0342",  # Combining Greek perispomeni
    "\u0343",  # Combining Greek koronis
    "\u0344",  # Combining Greek dialytika tonos
    "\u0345",  # Combining Greek ypogegrammeni
}


def normalize_coptic(text: str, nfc: bool = True) -> str:
    """
    Normalize Coptic text to canonical form.

    Args:
        text: Input text
        nfc: Apply Unicode NFC normalization (default: True)

    Returns:
        Normalized text
    """
    if nfc:
        return unicodedata.normalize("NFC", text)
    return text


def strip_diacritics(text: str) -> str:
    """
    Strip diacritics from Coptic text.

    Args:
        text: Input text with diacritics

    Returns:
        Text with diacritics removed
    """
    # Remove known Coptic diacritics
    result = text
    for diacritic in COPTIC_DIACRITICS:
        result = result.replace(diacritic, "")

    # Also use NFD decomposition to catch other combining marks
    decomposed = unicodedata.normalize("NFD", result)
    stripped = "".join(char for char in decomposed if unicodedata.category(char) != "Mn")

    # Recompose to NFC
    return unicodedata.normalize("NFC", stripped)


def normalize_and_strip(text: str) -> tuple[str, str]:
    """
    Normalize Coptic text and produce both canonical and stripped versions.

    Args:
        text: Input text

    Returns:
        Tuple of (canonical, stripped) versions
    """
    canonical = normalize_coptic(text, nfc=True)
    stripped = strip_diacritics(canonical)
    return canonical, stripped


def is_coptic_codepoint(char: str) -> bool:
    """
    Check if a character is in the Coptic Unicode block.

    Args:
        char: Single character

    Returns:
        True if in Coptic block
    """
    if len(char) != 1:
        return False

    code = ord(char)
    return COPTIC_BLOCK_START <= code <= COPTIC_BLOCK_END


def get_non_coptic_chars(text: str) -> set[str]:
    """
    Find characters not in Coptic Unicode block (excluding spaces, punctuation).

    Args:
        text: Input text

    Returns:
        Set of non-Coptic characters (excluding common ASCII)
    """
    non_coptic = set()

    for char in text:
        # Skip spaces and common ASCII punctuation
        if char.isspace() or char in ".,;:!?-—()[]{}\"'":
            continue

        # Skip Greek letters (common in Coptic texts)
        if 0x0370 <= ord(char) <= 0x03FF:
            continue

        # Check if not Coptic
        if not is_coptic_codepoint(char):
            non_coptic.add(char)

    return non_coptic


def validate_coptic_text(text: str) -> tuple[bool, set[str]]:
    """
    Validate that text is primarily Coptic.

    Args:
        text: Input text

    Returns:
        Tuple of (is_valid, set of non-Coptic characters)
    """
    non_coptic = get_non_coptic_chars(text)
    # Consider valid if less than 10% non-Coptic characters
    is_valid = len(non_coptic) < len(text) * 0.1
    return is_valid, non_coptic
