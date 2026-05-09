"""Parallel execution helpers for assets.

Two patterns:

- `parallel_cycles(fn, cycles)` — Threads. Use for I/O-bound per-cycle work
  (Arango aggregation queries). Threads share memory and bypass the GIL while
  blocked on socket I/O, which is what AQL execution is.

- `parallel_map(fn, items, initializer=...)` — Processes. Use for CPU-bound
  per-item work (e.g. each candidate's funding-channel trace). The initializer
  pattern is critical for performance: shared lookup state (committee dicts,
  edge maps) loads once into each worker's globals, and only the per-item key
  gets pickled across the boundary.

Worker count defaults to env LT_MAX_WORKERS, falling back to os.cpu_count()-2
to leave headroom for ArangoDB and the Dagster daemon. Override per call when
the workload size is small (don't spin up 24 workers for 4 cycles).
"""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable, List, Optional, Tuple, TypeVar

T = TypeVar("T")
R = TypeVar("R")


def _default_workers(cap: Optional[int] = None) -> int:
    env = os.environ.get("LT_MAX_WORKERS")
    if env:
        try:
            n = int(env)
            return max(1, n if cap is None else min(n, cap))
        except ValueError:
            pass
    n = max(1, (os.cpu_count() or 4) - 2)
    return n if cap is None else min(n, cap)


def parallel_cycles(
    fn: Callable[[str], R],
    cycles: Iterable[str],
    max_workers: int = 4,
) -> dict[str, R]:
    """Run fn(cycle) for each cycle concurrently in threads.

    Returns {cycle: result}. Re-raises the first exception encountered.
    Default 4 workers — matches typical 4-cycle workload, no point spinning
    up more than there are cycles.
    """
    cycles = list(cycles)
    workers = min(max_workers, len(cycles)) if cycles else 1
    results: dict[str, R] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fn, c): c for c in cycles}
        for fut in as_completed(futures):
            cycle = futures[fut]
            results[cycle] = fut.result()
    return results


def parallel_map(
    fn: Callable[[T], R],
    items: Iterable[T],
    workers: Optional[int] = None,
    chunksize: int = 50,
    initializer: Optional[Callable[..., None]] = None,
    initargs: Tuple[Any, ...] = (),
    ordered: bool = False,
) -> List[R]:
    """Run fn(item) for each item concurrently in processes.

    Use `initializer` to load shared state into each worker's globals once.
    Workers default to LT_MAX_WORKERS env var or os.cpu_count()-2.

    `ordered=False` (default) returns results in completion order — faster
    when work is uneven. Set True to preserve input order.
    """
    items = list(items)
    if not items:
        return []
    n = workers if workers is not None else _default_workers()
    n = min(n, len(items))
    with ProcessPoolExecutor(max_workers=n, initializer=initializer, initargs=initargs) as pool:
        if ordered:
            return list(pool.map(fn, items, chunksize=chunksize))
        # Submit in batches; collect as they finish.
        results: List[R] = []
        futures = [pool.submit(fn, it) for it in items]
        for fut in as_completed(futures):
            results.append(fut.result())
        return results
