"""
Tests for SetCommands + set dispatch.
"""

import pytest
from redis.server.store import StoreError

class TestSaddSrem:
    def test_sadd_returns_new_count(self, setc):
        assert setc.sadd("s", "a", "b", "c") == 3

    def test_sadd_ignores_duplicates(self, setc):
        setc.sadd("s", "a", "b")
        assert setc.sadd("s", "b", "c") == 1  # only "c" is new

    def test_srem_returns_removed_count(self, setc):
        setc.sadd("s", "a", "b", "c")
        assert setc.srem("s", "a", "c") == 2

    def test_srem_missing_member(self, setc):
        setc.sadd("s", "a")
        assert setc.srem("s", "nope") == 0

    def test_srem_missing_key(self, setc):
        assert setc.srem("nope", "x") == 0

    def test_wrong_type_raises(self, setc, sc):
        sc.set("str_key", "hello")
        with pytest.raises(StoreError):
            setc.sadd("str_key", "x")


class TestSmembersCard:
    def test_smembers(self, setc):
        setc.sadd("s", "x", "y", "z")
        assert setc.smembers("s") == {"x", "y", "z"}

    def test_smembers_missing(self, setc):
        assert setc.smembers("nope") == set()

    def test_smembers_returns_copy(self, setc):
        setc.sadd("s", "a")
        copy = setc.smembers("s")
        copy.add("b")
        assert setc.smembers("s") == {"a"}   # original unchanged

    def test_scard(self, setc):
        setc.sadd("s", "a", "b", "c")
        assert setc.scard("s") == 3

    def test_scard_missing(self, setc):
        assert setc.scard("nope") == 0


class TestSismember:
    def test_true_for_existing(self, setc):
        setc.sadd("s", "python")
        assert setc.sismember("s", "python") is True

    def test_false_for_missing(self, setc):
        setc.sadd("s", "python")
        assert setc.sismember("s", "java") is False

    def test_false_for_missing_key(self, setc):
        assert setc.sismember("nope", "x") is False


class TestSmove:
    def test_smove(self, setc):
        setc.sadd("src", "a", "b")
        setc.sadd("dst", "c")
        assert setc.smove("src", "dst", "a") is True
        assert setc.smembers("src") == {"b"}
        assert setc.smembers("dst") == {"c", "a"}

    def test_smove_missing_member(self, setc):
        setc.sadd("src", "a")
        assert setc.smove("src", "dst", "x") is False

    def test_smove_missing_key(self, setc):
        assert setc.smove("nope", "dst", "x") is False


class TestSetAlgebra:
    def test_sunion(self, setc):
        setc.sadd("a", "1", "2", "3")
        setc.sadd("b", "2", "3", "4")
        assert setc.sunion("a", "b") == {"1", "2", "3", "4"}

    def test_sinter(self, setc):
        setc.sadd("a", "1", "2", "3")
        setc.sadd("b", "2", "3", "4")
        assert setc.sinter("a", "b") == {"2", "3"}

    def test_sinter_with_missing_key(self, setc):
        setc.sadd("a", "1", "2")
        assert setc.sinter("a", "missing") == set()

    def test_sdiff(self, setc):
        setc.sadd("a", "1", "2", "3")
        setc.sadd("b", "2", "3", "4")
        assert setc.sdiff("a", "b") == {"1"}

    def test_sdiff_no_overlap(self, setc):
        setc.sadd("a", "1", "2")
        setc.sadd("b", "3", "4")
        assert setc.sdiff("a", "b") == {"1", "2"}

    def test_sunionstore(self, setc):
        setc.sadd("a", "1", "2")
        setc.sadd("b", "2", "3")
        count = setc.sunionstore("result", "a", "b")
        assert count == 3
        assert setc.smembers("result") == {"1", "2", "3"}

    def test_sinterstore(self, setc):
        setc.sadd("a", "1", "2", "3")
        setc.sadd("b", "2", "3", "4")
        count = setc.sinterstore("result", "a", "b")
        assert count == 2
        assert setc.smembers("result") == {"2", "3"}


class TestDispatcher:
    def test_sadd_smembers(self, dispatcher):
        dispatcher.execute("SADD tags python redis dsa")
        members = dispatcher.execute("SMEMBERS tags")
        assert members == {"python", "redis", "dsa"}

    def test_sismember(self, dispatcher):
        dispatcher.execute("SADD s x")
        assert dispatcher.execute("SISMEMBER s x") is True
        assert dispatcher.execute("SISMEMBER s y") is False

    def test_scard(self, dispatcher):
        dispatcher.execute("SADD s a b c")
        assert dispatcher.execute("SCARD s") == 3

    def test_sunion_via_dispatcher(self, dispatcher):
        dispatcher.execute("SADD a 1 2")
        dispatcher.execute("SADD b 2 3")
        assert dispatcher.execute("SUNION a b") == {"1", "2", "3"}

    def test_srem_via_dispatcher(self, dispatcher):
        dispatcher.execute("SADD s a b c")
        assert dispatcher.execute("SREM s a c") == 2
        assert dispatcher.execute("SMEMBERS s") == {"b"}