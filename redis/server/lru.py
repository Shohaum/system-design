"""
lru.py — LRU eviction cache, built from scratch.

Data structure: doubly linked list + hashmap

Why this combo?
- hashmap  → O(1) lookup: "does key X exist, and where is its node?"
- DLL      → O(1) move-to-front and O(1) evict-from-tail
             (a plain list would be O(n) for both)

Layout of the DLL:

    HEAD <-> [most recently used] <-> ... <-> [least recently used] <-> TAIL

HEAD and TAIL are sentinel nodes — they never hold real data.
They simplify edge-case logic (empty list, single element) by ensuring
every real node always has a prev and next.

    touch(key)   → moves node to just after HEAD   O(1)
    evict()      → removes node just before TAIL    O(1)
    put(key)     → insert after HEAD, add to map    O(1)
    remove(key)  → unlink node, remove from map     O(1)
"""

from __future__ import annotations

class _Node:
    """A node in the doubly linked list."""
    __slots__ = ("key", "prev", "next")

    def __init__(self, key: str) -> None:
        self.key: str  = key
        self.prev: _Node | None = None
        self.next: _Node | None = None

    def __repr__(self) -> str:
        return f"Node({self.key!r})"


class LRUCache:
    """
    Fixed-capacity LRU tracker.

    This class only tracks KEY ORDER — it does not store values.
    The actual values live in Store._data. LRUCache just tells the Store
    which key to evict when capacity is exceeded.

    Usage:
        lru = LRUCache(capacity=3)
        lru.touch("a")          # access / insert key
        lru.touch("b")
        lru.touch("c")
        lru.touch("a")          # "a" is now MRU
        lru.evict()             # returns "b" (LRU), removes it internally
        lru.remove("c")         # explicit removal (e.g. DEL command)
    """

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("LRU capacity must be >= 1")
        self.capacity = capacity

        # Sentinel nodes — never evicted, never in _map
        self._head = _Node("__HEAD__")   # MRU side
        self._tail = _Node("__TAIL__")   # LRU side
        self._head.next = self._tail
        self._tail.prev = self._head

        # key -> Node (for O(1) lookup)
        self._map: dict[str, _Node] = {}

    # Private DLL operations

    def _unlink(self, node: _Node) -> None:
        """Remove a node from the list (but don't touch _map)."""
        node.prev.next = node.next
        node.next.prev = node.prev
        # Null out pointers to help GC and catch bugs early
        node.prev = None
        node.next = None

    def _insert_after_head(self, node: _Node) -> None:
        """Insert node at the MRU position (just after HEAD sentinel)."""
        node.next       = self._head.next
        node.prev       = self._head
        self._head.next.prev = node
        self._head.next      = node

    def __repr__(self) -> str:
        return f"LRUCache(capacity={self.capacity}, order={self.to_list()})"

    # Public API

    def touch(self, key: str) -> None:
        if key in self._map:
            self._unlink(self._map[key])
        else:
            self._map[key] = _Node(key)   # ← move assignment here
        self._insert_after_head(self._map[key])

    def evict(self) -> str | None:
        """
        Remove and return the least-recently-used key.
        Returns None if the cache is empty.
        """
        lru_node = self._tail.prev
        if lru_node is self._head:
            return None          # cache is empty
        self._unlink(lru_node)
        del self._map[lru_node.key]
        return lru_node.key

    def remove(self, key: str) -> None:
        """Explicitly remove a key (called when Store deletes a key)."""
        if key in self._map:
            self._unlink(self._map[key])
            del self._map[key]

    def is_full(self) -> bool:
        return len(self._map) >= self.capacity

    def __len__(self) -> int:
        return len(self._map)

    def __contains__(self, key: str) -> bool:
        return key in self._map

    def peek_lru(self) -> str | None:
        """Return the LRU key without removing it (useful for debugging)."""
        node = self._tail.prev
        return node.key if node is not self._head else None

    def peek_mru(self) -> str | None:
        """Return the MRU key without removing it (useful for debugging)."""
        node = self._head.next
        return node.key if node is not self._tail else None

    def to_list(self) -> list[str]:
        """Return keys in MRU → LRU order (for debugging/testing)."""
        result = []
        node = self._head.next
        while node is not self._tail:
            result.append(node.key)
            node = node.next
        return result