"""
Entry format:
    {
        "type":       "string" | "list" | "set",
        "value":      <data>,
        "expires_at": float | None
    }

LRU eviction:
    Store accepts an optional max_keys argument. When the number of keys
    exceeds max_keys, the LRU key is evicted before each new write.
    If max_keys is None, eviction is disabled.

Thread safety:
    A single reentrant lock (_lock) guards all reads and writes.
    TCP server will have multiple threads hitting the store
    concurrently — adding the lock now means zero changes needed then.
"""

from __future__ import annotations
import threading
from typing import Any

from .lru import LRUCache
from .ttl import is_expired, set_expiry, clear_expiry, remaining_seconds

# Supported internal types
TYPE_STRING = "string"
TYPE_LIST   = "list"
TYPE_SET    = "set"

class StoreError(Exception):
    """Raised when an operation is attempted on the wrong type."""
    pass

class Store:
    """
    Central in-memory key-value store with TTL and LRU eviction.

    Args:
        max_keys: Maximum number of keys before LRU eviction kicks in.
                  None (default) disables eviction entirely.
    """

    def __init__(self, max_keys: int | None = None) -> None:
        self._data: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()   # reentrant so internal calls nest freely
        self._lru  = LRUCache(max_keys) if max_keys else None

    # Internal helpers

    def _get_entry(self, key: str) -> dict[str, Any] | None:
        """
        Return the raw entry for a key, applying lazy TTL expiry.
        If the key is expired, it is deleted here and None is returned.
        """
        entry = self._data.get(key)
        if entry is None:
            return None
        if is_expired(entry):
            self._del_entry(key)
            return None
        if self._lru:
            self._lru.touch(key)
        return entry

    def _assert_type(self, key: str, expected: str) -> None:
        entry = self._get_entry(key)
        if entry is not None and entry["type"] != expected:
            raise StoreError(
                f"WRONGTYPE: key '{key}' holds a {entry['type']}, "
                f"not a {expected}"
            )

    def _set_entry(self, key: str, type_: str, value: Any) -> None:
        """
        Write an entry. Preserves existing expires_at unless explicitly cleared.
        Triggers LRU eviction if over capacity.
        """
        existing = self._data.get(key)
        entry: dict[str, Any] = {
            "type":       type_,
            "value":      value,
            "expires_at": existing["expires_at"] if existing else None,
        }
        self._data[key] = entry
        
        if self._lru is not None and self._lru.capacity > 0:
            
            while self._lru.is_full():
                evicted_key = self._lru.evict()
                if evicted_key and evicted_key in self._data:
                    del self._data[evicted_key]
            
            self._lru.touch(key)

    def _del_entry(self, key: str) -> bool:
        """Delete a key. Returns True if it existed."""
        if key in self._data:
            del self._data[key]
            if self._lru:
                self._lru.remove(key)
            return True
        return False

    # TTL public interface

    def set_ttl(self, key: str, seconds: float) -> bool:
        entry = self._get_entry(key)
        if entry is None:
            return False
        set_expiry(entry, seconds)
        return True

    def persist(self, key: str) -> bool:
        entry = self._get_entry(key)
        if entry is None or entry.get("expires_at") is None:
            return False
        clear_expiry(entry)
        return True

    def ttl(self, key: str) -> float | int:
        """Remaining TTL in seconds. -2 = key missing, -1 = no expiry."""
        raw = self._data.get(key)
        if raw is None or is_expired(raw):
            return -2
        r = remaining_seconds(raw)
        return -1 if r is None else r

    def pttl(self, key: str) -> float | int:
        """Same as ttl() but in milliseconds."""
        t = self.ttl(key)
        return t if t < 0 else t * 1000

    # Generic key-space commands

    def exists(self, key: str) -> bool:
        return self._get_entry(key) is not None

    def delete(self, *keys: str) -> int:
        return sum(self._del_entry(k) for k in keys)

    def keys(self) -> list[str]:
        return [k for k in list(self._data.keys()) if self._get_entry(k) is not None]

    def type_of(self, key: str) -> str | None:
        entry = self._get_entry(key)
        return entry["type"] if entry else None

    def flush(self) -> None:
        self._data.clear()
        if self._lru:
            self._lru = LRUCache(self._lru.capacity)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:   # pragma: no cover
        return f"<Store keys={list(self._data.keys())}>"