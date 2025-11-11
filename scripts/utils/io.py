"""I/O utilities with atomic writes and safe file operations."""

import json
import shutil
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, TypeVar


T = TypeVar("T")


def atomic_write(
    path: Path,
    write_func: Callable[[Path], None],
) -> None:
    """
    Atomically write to a file using temp file and move.

    Args:
        path: Destination path
        write_func: Function that writes to a given path
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Create temp file in same directory (ensures same filesystem)
    with tempfile.NamedTemporaryFile(
        mode="wb",
        dir=path.parent,
        delete=False,
        prefix=f".tmp.{path.name}.",
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        # Write to temp file
        write_func(tmp_path)
        # Atomic move
        shutil.move(str(tmp_path), str(path))
    except Exception:
        # Clean up temp file on failure
        tmp_path.unlink(missing_ok=True)
        raise


def write_jsonl(path: Path, records: Iterator[dict[str, Any]]) -> int:
    """
    Write JSONL file atomically.

    Args:
        path: Destination path
        records: Iterator of dict records

    Returns:
        Number of records written
    """
    count = 0

    def _write(tmp_path: Path) -> None:
        nonlocal count
        with tmp_path.open("w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1

    atomic_write(path, _write)
    return count


def read_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """
    Read JSONL file line by line.

    Args:
        path: Path to JSONL file

    Yields:
        Parsed JSON objects
    """
    with Path(path).open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_json(path: Path, data: Any, indent: int = 2) -> None:
    """
    Write JSON file atomically.

    Args:
        path: Destination path
        data: Data to serialize
        indent: JSON indentation
    """

    def _write(tmp_path: Path) -> None:
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)

    atomic_write(path, _write)


def read_json(path: Path) -> Any:
    """
    Read JSON file.

    Args:
        path: Path to JSON file

    Returns:
        Parsed JSON data
    """
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def ensure_dir(path: Path) -> Path:
    """
    Ensure directory exists.

    Args:
        path: Directory path

    Returns:
        Path object
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def safe_copy(src: Path, dst: Path) -> None:
    """
    Safely copy file to destination.

    Args:
        src: Source path
        dst: Destination path
    """
    src = Path(src)
    dst = Path(dst)
    ensure_dir(dst.parent)
    shutil.copy2(src, dst)
