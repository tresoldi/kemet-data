"""Parallel processing utilities."""

from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import TypeVar


T = TypeVar("T")
R = TypeVar("R")


def map_parallel(
    func: Callable[[T], R],
    items: Iterable[T],
    max_workers: int = 4,
    use_processes: bool = False,
) -> Iterator[R]:
    """
    Map function over items in parallel.

    Args:
        func: Function to apply
        items: Items to process
        max_workers: Maximum parallel workers
        use_processes: Use processes instead of threads

    Yields:
        Results in completion order
    """
    executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor

    with executor_class(max_workers=max_workers) as executor:
        futures = {executor.submit(func, item): item for item in items}

        for future in as_completed(futures):
            yield future.result()


def map_parallel_ordered(
    func: Callable[[T], R],
    items: Iterable[T],
    max_workers: int = 4,
    use_processes: bool = False,
) -> Iterator[R]:
    """
    Map function over items in parallel, preserving order.

    Args:
        func: Function to apply
        items: Items to process
        max_workers: Maximum parallel workers
        use_processes: Use processes instead of threads

    Yields:
        Results in original order
    """
    executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor

    with executor_class(max_workers=max_workers) as executor:
        yield from executor.map(func, items)
