"""
Tests for the LRUCache built from scratch.

These tests verify the DLL + hashmap implementation directly,
independent of the Store. Think of them as unit tests for the
data structure itself.
"""

import pytest
from redis.server.lru import LRUCache

class TestBasicOrdering:
    def test_single_touch(self):
        lru = LRUCache(3)
        lru.touch("a")
        assert lru.peek_mru() == "a"
        assert lru.peek_lru() == "a"

    def test_mru_moves_to_front(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        lru.touch("c")
        # c is MRU, a is LRU
        assert lru.to_list() == ["c", "b", "a"]

    def test_re_touch_moves_to_front(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        lru.touch("c")
        lru.touch("a")   # a is now MRU
        assert lru.to_list() == ["a", "c", "b"]
        assert lru.peek_lru() == "b"

    def test_order_after_multiple_retouches(self):
        lru = LRUCache(4)
        for k in ["a", "b", "c", "d"]:
            lru.touch(k)
        lru.touch("b")
        lru.touch("a")
        assert lru.to_list() == ["a", "b", "d", "c"]


class TestEviction:
    def test_evict_returns_lru(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        lru.touch("c")
        assert lru.evict() == "a"

    def test_evict_removes_from_map(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        evicted = lru.evict()
        assert evicted not in lru

    def test_evict_empty_returns_none(self):
        lru = LRUCache(3)
        assert lru.evict() is None

    def test_evict_single_element(self):
        lru = LRUCache(3)
        lru.touch("only")
        assert lru.evict() == "only"
        assert len(lru) == 0

    def test_evict_sequence(self):
        lru = LRUCache(5)
        for k in ["a", "b", "c", "d", "e"]:
            lru.touch(k)
        lru.touch("c")   # c is now MRU, a is LRU
        evicted = []
        while len(lru) > 0:
            evicted.append(lru.evict())
        # Should evict LRU → MRU: a, b, d, e, c
        assert evicted == ["a", "b", "d", "e", "c"]


class TestRemove:
    def test_remove_existing(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        lru.touch("c")
        lru.remove("b")
        assert lru.to_list() == ["c", "a"]
        assert "b" not in lru

    def test_remove_mru(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        lru.remove("b")
        assert lru.peek_mru() == "a"

    def test_remove_lru(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        lru.remove("a")
        assert lru.peek_lru() == "b"

    def test_remove_missing_no_error(self):
        lru = LRUCache(3)
        lru.remove("nonexistent")   # should not raise

    def test_remove_then_reinsert(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        lru.remove("a")
        lru.touch("a")   # re-insert
        assert lru.peek_mru() == "a"


class TestCapacityAndIsFulll:
    def test_is_full_false_under_capacity(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        assert not lru.is_full()

    def test_is_full_at_capacity(self):
        lru = LRUCache(2)
        lru.touch("a")
        lru.touch("b")
        assert lru.is_full()   # exactly at capacity: not full yet

    def test_is_full_over_capacity(self):
        lru = LRUCache(2)
        lru.touch("a")
        lru.touch("b")
        lru.touch("c")   # 3 keys, capacity 2
        assert lru.is_full()

    def test_invalid_capacity_raises(self):
        with pytest.raises(ValueError):
            LRUCache(0)


class TestLenAndContains:
    def test_len(self):
        lru = LRUCache(5)
        for k in ["a", "b", "c"]:
            lru.touch(k)
        assert len(lru) == 3

    def test_contains(self):
        lru = LRUCache(3)
        lru.touch("x")
        assert "x" in lru
        assert "y" not in lru

    def test_len_after_evict(self):
        lru = LRUCache(3)
        lru.touch("a")
        lru.touch("b")
        lru.evict()
        assert len(lru) == 1