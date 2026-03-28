"""
Tests for TTL expiry and LRU eviction via Store + Dispatcher.

Key things being tested:
  - Lazy expiry: expired keys vanish on next access
  - Active expiry: ExpirySweeperThread removes keys in the background
  - SET EX / SET PX: TTL set at write time via dispatcher
  - EXPIRE / PERSIST / TTL / PTTL commands
  - LRU eviction: correct key is evicted when store exceeds max_keys
"""

import time
from redis.server.store import Store
from redis.server.commands import CommandDispatcher
from redis.server.ttl import ExpirySweeperThread
from redis.server.types import StringCommands

# Helpers

def make_dispatcher(max_keys=None):
    store = Store(max_keys=max_keys)
    return CommandDispatcher(store=store)

# Lazy TTL expiry

class TestLazyExpiry:
    def test_key_exists_before_expiry(self):
        store = Store()
        store._set_entry("k", "string", "hello")
        store.set_ttl("k", seconds=10)
        assert store.exists("k")

    def test_key_vanishes_after_expiry(self):
        store = Store()
        store._set_entry("k", "string", "hello")
        store.set_ttl("k", seconds=0.05)   # 50ms
        time.sleep(0.1)
        assert not store.exists("k")
        assert store._get_entry("k") is None

    def test_get_returns_none_after_expiry(self):
        d = make_dispatcher()
        d.execute("SET greeting hello EX 0.05")
        time.sleep(0.1)
        assert d.execute("GET greeting") is None

    def test_expired_key_not_in_keys(self):
        store = Store()
        store._set_entry("alive", "string", "yes")
        store._set_entry("dead",  "string", "no")
        store.set_ttl("dead", seconds=0.05)
        time.sleep(0.1)
        assert "alive" in store.keys()
        assert "dead"  not in store.keys()

    def test_expired_key_counts_as_nonexistent_for_type(self):
        store = Store()
        sc = StringCommands(store)
        sc.set("k", "value")
        store.set_ttl("k", seconds=0.05)
        time.sleep(0.1)
        # After expiry, GET should return None, not raise StoreError
        assert sc.get("k") is None

# Active expiry (sweeper thread)

class TestActiveSweeper:
    def test_sweeper_removes_expired_keys(self):
        store = Store()
        store._set_entry("x", "string", "1")
        store._set_entry("y", "string", "2")
        store.set_ttl("x", seconds=0.05)

        sweeper = ExpirySweeperThread(store, interval=0.05)
        sweeper.start()
        time.sleep(0.3)   # give sweeper time to run
        sweeper.stop()
        sweeper.join(timeout=1)

        assert "x" not in store._data
        assert "y" in store._data

    def test_sweeper_doesnt_remove_live_keys(self):
        store = Store()
        store._set_entry("live", "string", "yes")
        store.set_ttl("live", seconds=60)

        sweeper = ExpirySweeperThread(store, interval=0.05)
        sweeper.start()
        time.sleep(0.2)
        sweeper.stop()
        sweeper.join(timeout=1)

        assert "live" in store._data

# SET EX / PX via dispatcher

class TestSetWithTTL:
    def test_set_ex(self):
        d = make_dispatcher()
        d.execute("SET k v EX 10")
        ttl = d.execute("TTL k")
        assert 9 <= ttl <= 10

    def test_set_px(self):
        d = make_dispatcher()
        d.execute("SET k v PX 5000")
        pttl = d.execute("PTTL k")
        assert 4900 <= pttl <= 5000

    def test_set_without_ex_clears_existing_ttl(self):
        d = make_dispatcher()
        d.execute("SET k v EX 10")
        d.execute("SET k v2")        # re-set without EX
        assert d.execute("TTL k") == -1   # -1 = no expiry

    def test_set_ex_overwrites_previous_ttl(self):
        d = make_dispatcher()
        d.execute("SET k v EX 100")
        d.execute("SET k v2 EX 5")
        ttl = d.execute("TTL k")
        assert ttl <= 5

    def test_key_expires_after_set_ex(self):
        d = make_dispatcher()
        d.execute("SET k v EX 0.05")
        time.sleep(0.15)
        assert d.execute("GET k") is None

# EXPIRE / PERSIST / TTL / PTTL commands

class TestTTLCommands:
    def test_expire_sets_ttl(self):
        d = make_dispatcher()
        d.execute("SET k v")
        d.execute("EXPIRE k 30")
        ttl = d.execute("TTL k")
        assert 29 <= ttl <= 30

    def test_expire_returns_false_for_missing_key(self):
        d = make_dispatcher()
        assert d.execute("EXPIRE missing 10") is False

    def test_ttl_no_expiry_returns_minus_one(self):
        d = make_dispatcher()
        d.execute("SET k v")
        assert d.execute("TTL k") == -1

    def test_ttl_missing_key_returns_minus_two(self):
        d = make_dispatcher()
        assert d.execute("TTL missing") == -2

    def test_pttl_returns_milliseconds(self):
        d = make_dispatcher()
        d.execute("SET k v EX 10")
        pttl = d.execute("PTTL k")
        assert 9000 <= pttl <= 10000

    def test_persist_removes_ttl(self):
        d = make_dispatcher()
        d.execute("SET k v EX 60")
        result = d.execute("PERSIST k")
        assert result is True
        assert d.execute("TTL k") == -1

    def test_persist_on_key_without_ttl(self):
        d = make_dispatcher()
        d.execute("SET k v")
        assert d.execute("PERSIST k") is False

    def test_persist_on_missing_key(self):
        d = make_dispatcher()
        assert d.execute("PERSIST missing") is False

    def test_pexpire(self):
        d = make_dispatcher()
        d.execute("SET k v")
        d.execute("PEXPIRE k 5000")
        pttl = d.execute("PTTL k")
        assert 4900 <= pttl <= 5000

# LRU eviction via Store

class TestLRUEviction:
    def test_evicts_lru_key_when_over_capacity(self):
        store = Store(max_keys=3)
        sc = StringCommands(store)
        sc.set("a", "1")
        sc.set("b", "2")
        sc.set("c", "3")
        # Access "a" to make it MRU — "b" becomes LRU
        sc.get("a")
        # Insert 4th key — should evict "b" (LRU)
        sc.set("d", "4")
        assert store.exists("a")
        assert not store.exists("b")
        assert store.exists("c")
        assert store.exists("d")

    def test_store_never_exceeds_max_keys(self):
        store = Store(max_keys=5)
        sc = StringCommands(store)
        for i in range(20):
            sc.set(f"key{i}", str(i))
        assert len(store) <= 5

    def test_touching_key_prevents_its_eviction(self):
        store = Store(max_keys=3)
        sc = StringCommands(store)
        sc.set("a", "1")
        sc.set("b", "2")
        sc.set("c", "3")
        # Keep touching "a" so it's always MRU
        for _ in range(3):
            sc.get("a")
            sc.set(f"new{_}", "x")
        assert store.exists("a")

    def test_eviction_without_max_keys_disabled(self):
        # Default store has no limit
        store = Store()
        sc = StringCommands(store)
        for i in range(100):
            sc.set(f"k{i}", str(i))
        assert len(store) == 100

    def test_correct_eviction_order(self):
        store = Store(max_keys=3)
        sc = StringCommands(store)
        sc.set("a", "1")   # LRU
        sc.set("b", "2")
        sc.set("c", "3")   # MRU
        # Adding d evicts a (LRU)
        sc.set("d", "4")
        assert not store.exists("a")
        # Adding e evicts b (now LRU)
        sc.set("e", "5")
        assert not store.exists("b")