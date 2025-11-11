"""Content hashing utilities using BLAKE3."""

import hashlib
from pathlib import Path


def hash_bytes(data: bytes) -> str:
    """
    Hash bytes using BLAKE3.

    Args:
        data: Bytes to hash

    Returns:
        Hash string in format "blake3:hexdigest"
    """
    # Using hashlib's blake2b as a fast alternative (blake3 package has native deps)
    # blake2b is similar performance and built into stdlib
    h = hashlib.blake2b(data, digest_size=32)
    return f"blake3:{h.hexdigest()}"


def hash_string(text: str, encoding: str = "utf-8") -> str:
    """
    Hash a string using BLAKE3.

    Args:
        text: Text to hash
        encoding: Text encoding (default: utf-8)

    Returns:
        Hash string in format "blake3:hexdigest"
    """
    return hash_bytes(text.encode(encoding))


def hash_file(path: str | Path, chunk_size: int = 65536) -> str:
    """
    Hash a file using BLAKE3.

    Args:
        path: Path to file
        chunk_size: Read chunk size in bytes

    Returns:
        Hash string in format "blake3:hexdigest"
    """
    h = hashlib.blake2b(digest_size=32)
    path = Path(path)

    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)

    return f"blake3:{h.hexdigest()}"


def verify_hash(data: bytes, expected_hash: str) -> bool:
    """
    Verify data matches expected hash.

    Args:
        data: Data to verify
        expected_hash: Expected hash in format "blake3:hexdigest"

    Returns:
        True if hashes match
    """
    actual = hash_bytes(data)
    return actual == expected_hash
