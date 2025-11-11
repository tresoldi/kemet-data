"""Tests for hashing utilities."""

from scripts.utils.hashing import hash_bytes, hash_string, verify_hash


def test_hash_string():
    """Test string hashing."""
    text = "hello world"
    hash_result = hash_string(text)

    assert hash_result.startswith("blake3:")
    assert len(hash_result) == 71  # "blake3:" + 64 hex chars


def test_hash_bytes():
    """Test bytes hashing."""
    data = b"hello world"
    hash_result = hash_bytes(data)

    assert hash_result.startswith("blake3:")
    assert len(hash_result) == 71


def test_hash_consistency():
    """Test that same input produces same hash."""
    text = "test"
    hash1 = hash_string(text)
    hash2 = hash_string(text)

    assert hash1 == hash2


def test_verify_hash():
    """Test hash verification."""
    data = b"test data"
    hash_result = hash_bytes(data)

    assert verify_hash(data, hash_result)
    assert not verify_hash(b"wrong data", hash_result)
