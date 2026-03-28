"""
Tests for StringCommands + string dispatch.

Pattern: each test function covers one command or one interesting edge case.
Tests go through the low-level StringCommands API AND through the
CommandDispatcher to verify end-to-end wiring.
"""

import pytest
from redis.server.store import StoreError
from redis.server.commands import CommandError

class TestSetGet:
    def test_set_returns_ok(self, sc):
        assert sc.set("k", "v") == "OK"

    def test_get_returns_value(self, sc):
        sc.set("k", "hello")
        assert sc.get("k") == "hello"

    def test_get_missing_key_returns_none(self, sc):
        assert sc.get("nope") is None

    def test_set_overwrites(self, sc):
        sc.set("k", "first")
        sc.set("k", "second")
        assert sc.get("k") == "second"

    def test_set_coerces_to_string(self, sc):
        sc.set("k", 42)
        assert sc.get("k") == "42"

    def test_get_wrong_type_raises(self, store, sc, lc):
        lc.rpush("mylist", "a")
        with pytest.raises(StoreError):
            sc.get("mylist")


class TestGetSet:
    def test_getset_returns_old_value(self, sc):
        sc.set("k", "old")
        old = sc.getset("k", "new")
        assert old == "old"
        assert sc.get("k") == "new"

    def test_getset_on_missing_key(self, sc):
        old = sc.getset("k", "val")
        assert old is None
        assert sc.get("k") == "val"


class TestMsetMget:
    def test_mset_mget(self, sc):
        sc.mset({"a": "1", "b": "2", "c": "3"})
        assert sc.mget("a", "b", "c") == ["1", "2", "3"]

    def test_mget_missing_key_is_none(self, sc):
        sc.set("x", "hi")
        result = sc.mget("x", "missing")
        assert result == ["hi", None]

    def test_mget_wrong_type_is_none(self, sc, lc):
        lc.rpush("lst", "a")
        result = sc.mget("lst")
        assert result == [None]


class TestIncr:
    def test_incr_missing_key_starts_at_one(self, sc):
        assert sc.incr("counter") == 1

    def test_incr_increments(self, sc):
        sc.set("n", "10")
        assert sc.incr("n") == 11

    def test_incrby(self, sc):
        sc.set("n", "5")
        assert sc.incr("n", 3) == 8

    def test_decr(self, sc):
        sc.set("n", "10")
        assert sc.decr("n") == 9

    def test_incr_non_integer_raises(self, sc):
        sc.set("k", "abc")
        with pytest.raises(ValueError):
            sc.incr("k")


class TestAppendStrlen:
    def test_append_creates_key(self, sc):
        length = sc.append("k", "hello")
        assert length == 5
        assert sc.get("k") == "hello"

    def test_append_extends(self, sc):
        sc.set("k", "hello")
        sc.append("k", " world")
        assert sc.get("k") == "hello world"

    def test_strlen(self, sc):
        sc.set("k", "hello")
        assert sc.strlen("k") == 5

    def test_strlen_missing(self, sc):
        assert sc.strlen("nope") == 0


class TestSetnx:
    def test_setnx_sets_when_missing(self, sc):
        assert sc.setnx("k", "v") is True
        assert sc.get("k") == "v"

    def test_setnx_no_op_when_exists(self, sc):
        sc.set("k", "original")
        assert sc.setnx("k", "new") is False
        assert sc.get("k") == "original"


class TestDispatcher:
    """Smoke-tests through the CommandDispatcher for string commands."""

    def test_set_get(self, dispatcher):
        assert dispatcher.execute("SET name redis") == "OK"
        assert dispatcher.execute("GET name") == "redis"

    def test_del(self, dispatcher):
        dispatcher.execute("SET x 1")
        assert dispatcher.execute("DEL x") == 1
        assert dispatcher.execute("GET x") is None

    def test_exists(self, dispatcher):
        dispatcher.execute("SET x 1")
        assert dispatcher.execute("EXISTS x") is True
        assert dispatcher.execute("EXISTS y") is False

    def test_keys(self, dispatcher):
        dispatcher.execute("SET a 1")
        dispatcher.execute("SET b 2")
        assert set(dispatcher.execute("KEYS")) == {"a", "b"}

    def test_incr_via_dispatcher(self, dispatcher):
        assert dispatcher.execute("INCR hits") == 1
        assert dispatcher.execute("INCRBY hits 9") == 10

    def test_mset_mget_via_dispatcher(self, dispatcher):
        dispatcher.execute("MSET k1 v1 k2 v2")
        assert dispatcher.execute("MGET k1 k2") == ["v1", "v2"]

    def test_unknown_command_raises(self, dispatcher):
        with pytest.raises(CommandError):
            dispatcher.execute("BLORP foo")

    def test_wrong_arg_count_raises(self, dispatcher):
        with pytest.raises(CommandError):
            dispatcher.execute("SET only_one_arg")