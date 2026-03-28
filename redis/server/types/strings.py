"""
types/strings.py — String type commands.

- Values are always stored as strings internally.
- INCR / INCRBY / DECR parse the string as an integer at call time.
- APPEND concatenates and returns the new length (like Redis).

All methods operate on a Store instance passed at construction.
"""

from __future__ import annotations
from ..store import Store, TYPE_STRING, StoreError

class StringCommands:
    """
    Usage:
        store = Store()
        sc = StringCommands(store)
        sc.set("name", "alice")
        sc.get("name")  # "alice"
    """

    def __init__(self, store: Store) -> None:
        self._store = store

    # SET / GET

    def set(self, key: str, value: str) -> str:
        """
        SET key value
        Always overwrites (including a different type).
        Returns "OK".
        """
        self._store._set_entry(key, TYPE_STRING, str(value))
        return "OK"

    def get(self, key: str) -> str | None:
        """
        GET key
        Returns the string value, or None if the key doesn't exist.
        Raises StoreError if the key holds a non-string type.
        """
        self._store._assert_type(key, TYPE_STRING)
        entry = self._store._get_entry(key)
        return entry["value"] if entry else None

    def getset(self, key: str, value: str) -> str | None:
        """
        GETSET key value
        Atomically sets a new value and returns the old one.
        """
        old = self.get(key)
        self.set(key, value)
        return old

    # Multi-key

    def mset(self, mapping: dict[str, str]) -> str:
        """
        MSET key1 val1 key2 val2 ...
        Accepts a dict. Always returns "OK".
        """
        for k, v in mapping.items():
            self.set(k, v)
        return "OK"

    def mget(self, *keys: str) -> list[str | None]:
        """
        MGET key1 key2 ...
        Returns a list of values (None for missing or wrong-type keys).
        """
        results = []
        for k in keys:
            try:
                results.append(self.get(k))
            except StoreError:
                results.append(None)
        return results

    # Numeric helpers

    def incr(self, key: str, amount: int = 1) -> int:
        """
        INCR key  /  INCRBY key amount
        Parses the stored string as int, increments, writes back.
        Initialises to 0 if key is missing.
        Raises ValueError if the value can't be parsed as int.
        """
        self._store._assert_type(key, TYPE_STRING)
        entry = self._store._get_entry(key)
        current = int(entry["value"]) if entry else 0
        new_val = current + amount
        self._store._set_entry(key, TYPE_STRING, str(new_val))
        return new_val

    def decr(self, key: str, amount: int = 1) -> int:
        """DECR key  /  DECRBY key amount"""
        return self.incr(key, -amount)

    # String helpers

    def append(self, key: str, value: str) -> int:
        """
        APPEND key value
        Appends to existing string (or creates it). Returns new length.
        """
        self._store._assert_type(key, TYPE_STRING)
        entry = self._store._get_entry(key)
        current = entry["value"] if entry else ""
        new_val = current + str(value)
        self._store._set_entry(key, TYPE_STRING, new_val)
        return len(new_val)

    def strlen(self, key: str) -> int:
        """STRLEN key — returns 0 for missing keys."""
        val = self.get(key)
        return len(val) if val is not None else 0

    def setnx(self, key: str, value: str) -> bool:
        """
        SETNX key value
        Set only if key does NOT exist. Returns True if set, False otherwise.
        """
        if self._store.exists(key):
            return False
        self.set(key, value)
        return True