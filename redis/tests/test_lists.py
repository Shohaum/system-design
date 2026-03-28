"""
Tests for ListCommands + list dispatch.
"""

import pytest
from redis.server.store import StoreError
from redis.server.commands import CommandError

class TestPushPop:
    def test_rpush_returns_length(self, lc):
        assert lc.rpush("q", "a") == 1
        assert lc.rpush("q", "b", "c") == 3

    def test_rpush_order(self, lc):
        lc.rpush("q", "a", "b", "c")
        assert lc.lrange("q", 0, -1) == ["a", "b", "c"]

    def test_lpush_returns_length(self, lc):
        assert lc.lpush("q", "a") == 1
        assert lc.lpush("q", "b") == 2

    def test_lpush_order(self, lc):
        # lpush pushes each value to the left in order,
        # so lpush(q, a, b, c) results in [c, b, a]
        lc.lpush("q", "a", "b", "c")
        assert lc.lrange("q", 0, -1) == ["c", "b", "a"]

    def test_lpop(self, lc):
        lc.rpush("q", "first", "second")
        assert lc.lpop("q") == "first"
        assert lc.llen("q") == 1

    def test_rpop(self, lc):
        lc.rpush("q", "first", "second")
        assert lc.rpop("q") == "second"

    def test_pop_empty_returns_none(self, lc):
        assert lc.lpop("nope") is None
        assert lc.rpop("nope") is None

    def test_wrong_type_raises(self, lc, sc):
        sc.set("str_key", "hello")
        with pytest.raises(StoreError):
            lc.lpush("str_key", "x")


class TestLrange:
    def test_full_range(self, lc):
        lc.rpush("l", "a", "b", "c", "d")
        assert lc.lrange("l", 0, -1) == ["a", "b", "c", "d"]

    def test_partial_range(self, lc):
        lc.rpush("l", "a", "b", "c", "d")
        assert lc.lrange("l", 1, 2) == ["b", "c"]

    def test_negative_indices(self, lc):
        lc.rpush("l", "a", "b", "c")
        assert lc.lrange("l", -2, -1) == ["b", "c"]

    def test_out_of_bounds_stop(self, lc):
        lc.rpush("l", "a", "b")
        assert lc.lrange("l", 0, 100) == ["a", "b"]

    def test_start_beyond_end_returns_empty(self, lc):
        lc.rpush("l", "a", "b")
        assert lc.lrange("l", 5, 10) == []

    def test_missing_key_returns_empty(self, lc):
        assert lc.lrange("nope", 0, -1) == []


class TestLindex:
    def test_positive_index(self, lc):
        lc.rpush("l", "a", "b", "c")
        assert lc.lindex("l", 0) == "a"
        assert lc.lindex("l", 2) == "c"

    def test_negative_index(self, lc):
        lc.rpush("l", "a", "b", "c")
        assert lc.lindex("l", -1) == "c"

    def test_out_of_range_returns_none(self, lc):
        lc.rpush("l", "a")
        assert lc.lindex("l", 99) is None

    def test_missing_key_returns_none(self, lc):
        assert lc.lindex("nope", 0) is None


class TestLset:
    def test_lset(self, lc):
        lc.rpush("l", "a", "b", "c")
        lc.lset("l", 1, "B")
        assert lc.lrange("l", 0, -1) == ["a", "B", "c"]

    def test_lset_missing_key_raises(self, lc):
        with pytest.raises(KeyError):
            lc.lset("nope", 0, "x")

    def test_lset_out_of_range_raises(self, lc):
        lc.rpush("l", "a")
        with pytest.raises(IndexError):
            lc.lset("l", 99, "x")


class TestDispatcher:
    def test_rpush_lrange(self, dispatcher):
        dispatcher.execute("RPUSH mylist a b c")
        assert dispatcher.execute("LRANGE mylist 0 -1") == ["a", "b", "c"]

    def test_lpush(self, dispatcher):
        dispatcher.execute("LPUSH mylist a b c")
        assert dispatcher.execute("LRANGE mylist 0 -1") == ["c", "b", "a"]

    def test_lpop_rpop(self, dispatcher):
        dispatcher.execute("RPUSH q x y z")
        assert dispatcher.execute("LPOP q") == "x"
        assert dispatcher.execute("RPOP q") == "z"

    def test_llen(self, dispatcher):
        dispatcher.execute("RPUSH l a b c")
        assert dispatcher.execute("LLEN l") == 3

    def test_type_command(self, dispatcher):
        dispatcher.execute("RPUSH l a")
        assert dispatcher.execute("TYPE l") == "list"
        assert dispatcher.execute("TYPE missing") == "none"