"""
Redis sets are unordered collections of unique strings.
Python's built-in set maps perfectly to this.

Includes set-algebra operations: SUNION, SINTER, SDIFF which are
some of the most useful and interesting Redis commands in practice.
"""

from __future__ import annotations
from ..store import Store, TYPE_SET

class SetCommands:
    """
    Implements Redis set commands against a Store.

    Usage:
        store = Store()
        sc = SetCommands(store)
        sc.sadd("tags", "python", "redis", "dsa")
        sc.smembers("tags")  # {"python", "redis", "dsa"}
    """

    def __init__(self, store: Store) -> None:
        self._store = store

    # Internal helpers

    def _get_set(self, key: str) -> set | None:
        """Return the set for key, or None if missing."""
        self._store._assert_type(key, TYPE_SET)
        entry = self._store._get_entry(key)
        return entry["value"] if entry else None

    def _get_or_create(self, key: str) -> set:
        s = self._get_set(key)
        if s is None:
            s = set()
            self._store._set_entry(key, TYPE_SET, s)
        return s

    # Core commands

    def sadd(self, key: str, *members: str) -> int:
        """
        SADD key member [member ...]
        Adds members to the set. Returns the count of NEW members added
        (already-existing members are ignored).
        """
        s = self._get_or_create(key)
        before = len(s)
        s.update(str(m) for m in members)
        return len(s) - before

    def srem(self, key: str, *members: str) -> int:
        """
        SREM key member [member ...]
        Removes members. Returns count of members actually removed.
        """
        s = self._get_set(key)
        if s is None:
            return 0
        before = len(s)
        for m in members:
            s.discard(str(m))
        return before - len(s)

    def sismember(self, key: str, member: str) -> bool:
        """SISMEMBER key member — True if member is in the set."""
        s = self._get_set(key)
        return s is not None and str(member) in s

    def smembers(self, key: str) -> set[str]:
        """SMEMBERS key — returns a copy of all members (empty set if missing)."""
        s = self._get_set(key)
        return set(s) if s else set()

    def scard(self, key: str) -> int:
        """SCARD key — returns the cardinality (size) of the set."""
        s = self._get_set(key)
        return len(s) if s else 0

    def smove(self, src: str, dst: str, member: str) -> bool:
        """
        SMOVE source destination member
        Atomically moves a member from src to dst.
        Returns True if the member existed in src.
        """
        s_src = self._get_set(src)
        if s_src is None or str(member) not in s_src:
            return False
        s_src.discard(str(member))
        self._get_or_create(dst).add(str(member))
        return True

    # Set algebra

    def sunion(self, *keys: str) -> set[str]:
        """
        SUNION key [key ...]
        Returns the union of all sets.
        """
        result: set[str] = set()
        for k in keys:
            s = self._get_set(k)
            if s:
                result |= s
        return result

    def sinter(self, *keys: str) -> set[str]:
        """
        SINTER key [key ...]
        Returns the intersection of all sets.
        Returns empty set if any key is missing.
        """
        sets = []
        for k in keys:
            s = self._get_set(k)
            if s is None:
                return set()
            sets.append(s)
        if not sets:
            return set()
        result = sets[0].copy()
        for s in sets[1:]:
            result &= s
        return result

    def sdiff(self, *keys: str) -> set[str]:
        """
        SDIFF key [key ...]
        Returns members in the first set that are not in subsequent sets.
        """
        if not keys:
            return set()
        first = self._get_set(keys[0])
        if first is None:
            return set()
        result = first.copy()
        for k in keys[1:]:
            s = self._get_set(k)
            if s:
                result -= s
        return result

    def sunionstore(self, dest: str, *keys: str) -> int:
        """SUNIONSTORE dest key [key ...] — stores union in dest, returns size."""
        result = self.sunion(*keys)
        self._store._set_entry(dest, TYPE_SET, result)
        return len(result)

    def sinterstore(self, dest: str, *keys: str) -> int:
        """SINTERSTORE dest key [key ...] — stores intersection in dest."""
        result = self.sinter(*keys)
        self._store._set_entry(dest, TYPE_SET, result)
        return len(result)