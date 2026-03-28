"""
Redis lists are doubly-linked lists under the hood — O(1) push/pop from
both ends, O(n) access by index. We model this with Python's collections.deque
which gives us exactly those semantics.

- LPUSH / RPUSH return the new length of the list.
- LPOP / RPOP return the removed element (or None if empty / missing).
- LRANGE supports negative indices (-1 = last element).
- Pushing to a non-existent key auto-creates the list.
- Popping the last element from a list DOES NOT auto-delete the key
  (we keep the empty list, matching Redis behaviour for simplicity —
   production Redis does remove the key, feel free to add that).
"""

from __future__ import annotations
from collections import deque
from ..store import Store, TYPE_LIST

class ListCommands:
    """
    Usage:
        store = Store()
        lc = ListCommands(store)
        lc.rpush("queue", "a", "b", "c")
        lc.lrange("queue", 0, -1)  # ["a", "b", "c"]
    """

    def __init__(self, store: Store) -> None:
        self._store = store

    # Internal helpers

    def _get_list(self, key: str) -> deque | None:
        """Return the deque for key, or None if missing."""
        self._store._assert_type(key, TYPE_LIST)
        entry = self._store._get_entry(key)
        return entry["value"] if entry else None

    def _get_or_create(self, key: str) -> deque:
        """Return existing deque or create a new empty one."""
        lst = self._get_list(key)
        if lst is None:
            lst = deque()
            self._store._set_entry(key, TYPE_LIST, lst)
        return lst

    # Push / Pop

    def lpush(self, key: str, *values: str) -> int:
        """
        LPUSH key value [value ...]
        Inserts values at the HEAD of the list (left side).
        Each value is pushed individually left-to-right, so the last
        value ends up at the head — matching Redis semantics.
        Returns the new length.
        """
        lst = self._get_or_create(key)
        for v in values:
            lst.appendleft(str(v))
        return len(lst)

    def rpush(self, key: str, *values: str) -> int:
        """
        RPUSH key value [value ...]
        Appends values to the TAIL of the list (right side).
        Returns the new length.
        """
        lst = self._get_or_create(key)
        for v in values:
            lst.append(str(v))
        return len(lst)

    def lpop(self, key: str) -> str | None:
        """
        LPOP key
        Removes and returns the head element. None if missing/empty.
        """
        lst = self._get_list(key)
        if not lst:
            return None
        return lst.popleft()

    def rpop(self, key: str) -> str | None:
        """
        RPOP key
        Removes and returns the tail element. None if missing/empty.
        """
        lst = self._get_list(key)
        if not lst:
            return None
        return lst.pop()
    
    # Query

    def llen(self, key: str) -> int:
        """LLEN key — returns 0 for missing keys."""
        lst = self._get_list(key)
        return len(lst) if lst is not None else 0

    def lrange(self, key: str, start: int, stop: int) -> list[str]:
        """
        LRANGE key start stop
        Returns a slice of the list (both ends inclusive).
        Supports negative indices: -1 is the last element.
        Returns [] for missing keys or out-of-range slices.
        """
        lst = self._get_list(key)
        if lst is None:
            return []
        items = list(lst)
        n = len(items)

        # Normalise negative indices
        if start < 0:
            start = max(0, n + start)
        if stop < 0:
            stop = n + stop

        # Out of bounds: clamp stop, bail if start is beyond end
        stop = min(stop, n - 1)
        if start > stop:
            return []

        return items[start : stop + 1]

    def lindex(self, key: str, index: int) -> str | None:
        """
        LINDEX key index
        Returns the element at the given index. Supports negative indices.
        Returns None if out of range.
        """
        lst = self._get_list(key)
        if lst is None:
            return None
        items = list(lst)
        try:
            return items[index]
        except IndexError:
            return None

    def lset(self, key: str, index: int, value: str) -> str:
        """
        LSET key index value
        Sets the element at index to value.
        Raises IndexError if index is out of range.
        """
        lst = self._get_list(key)
        if lst is None:
            raise KeyError(f"No such key: {key}")
        items = list(lst)
        items[index] = str(value)   # raises IndexError naturally
        self._store._set_entry(key, TYPE_LIST, deque(items))
        return "OK"