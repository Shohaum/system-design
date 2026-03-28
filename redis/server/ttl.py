"""
Expiry strategy:

  LAZY expiry:
    On every _get_entry() call, check if the key is expired.
    If yes, delete it and return None — as if it never existed.
    This is free (no extra thread), but means stale keys linger
    in memory until they're next accessed.

  ACTIVE expiry (periodic sweep):
    A background thread wakes every `sweep_interval` seconds,
    scans all keys with a TTL, and deletes the expired ones.
    This reclaims memory for keys that are never accessed again.

TTL is stored as a Unix timestamp (float) in the entry dict:
    entry["expires_at"] = time.monotonic() + ttl_seconds
                        | None  (no expiry)

We use time.monotonic() rather than time.time() because:
- It never jumps backwards (NTP adjustments, DST, etc.)
- We only care about elapsed duration, not wall-clock time.
  (TTL COMMANDS that return remaining seconds convert back to a delta.)
"""

from __future__ import annotations
import time
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .store import Store   # avoid circular import at runtime


def is_expired(entry: dict) -> bool:
    """Return True if this entry has a TTL that has passed."""
    exp = entry.get("expires_at")
    return exp is not None and time.monotonic() > exp


def set_expiry(entry: dict, seconds: float) -> None:
    """Attach a TTL to an already-created entry dict."""
    entry["expires_at"] = time.monotonic() + seconds


def clear_expiry(entry: dict) -> None:
    """Remove TTL from an entry (PERSIST command)."""
    entry.pop("expires_at", None)


def remaining_seconds(entry: dict) -> float | None:
    """
    Return remaining TTL in seconds, or None if no expiry.
    Returns 0.0 (not negative) if already expired but not yet cleaned up.
    """
    exp = entry.get("expires_at")
    if exp is None:
        return None
    remaining = exp - time.monotonic()
    return max(0.0, remaining)


class ExpirySweeperThread(threading.Thread):
    """
    Background thread that periodically deletes expired keys.

    Why a daemon thread?
    - daemon=True means Python won't wait for it on shutdown.
    - We don't want the process to hang just because this thread is sleeping.

    Sweep strategy:
    - Collect keys with expires_at set.
    - Delete those that have passed. Simple O(n) scan.
    - Real Redis uses a smarter probabilistic approach (sample 20 random
      keys, if >25% expired repeat) to avoid blocking for large keyspaces.
      That's a fun stretch goal once the basics work.
    """

    def __init__(self, store: "Store", interval: float = 1.0) -> None:
        super().__init__(daemon=True, name="expiry-sweeper")
        self._store    = store
        self._interval = interval
        self._stop_event = threading.Event()

    def run(self) -> None:
        while not self._stop_event.wait(timeout=self._interval):
            self._sweep()

    def stop(self) -> None:
        """Signal the thread to stop. Used in tests for clean teardown."""
        self._stop_event.set()

    def _sweep(self) -> None:
        """
        One sweep pass: collect expired keys, delete them.
        We snapshot the key list first to avoid mutating the dict
        while iterating over it.
        """
        with self._store._lock:
            expired = [
                key for key, entry in self._store._data.items()
                if is_expired(entry)
            ]
        for key in expired:
            # Use the store's delete so LRU tracking stays consistent.
            self._store.delete(key)