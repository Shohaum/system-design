"""
shared pytest fixtures.

Every test module gets a fresh Store + dispatcher via these fixtures,
so tests are fully isolated from each other.
"""

import pytest
from redis.server.store import Store
from redis.server.commands import CommandDispatcher
from redis.server.types import StringCommands, ListCommands, SetCommands

@pytest.fixture
def store():
    """A clean Store for low-level type tests."""
    return Store()


@pytest.fixture
def dispatcher():
    """A CommandDispatcher with its own fresh Store."""
    return CommandDispatcher()


@pytest.fixture
def sc(store):
    """StringCommands wired to a fresh store."""
    return StringCommands(store)


@pytest.fixture
def lc(store):
    """ListCommands wired to a fresh store."""
    return ListCommands(store)


@pytest.fixture
def setc(store):
    """SetCommands wired to a fresh store."""
    return SetCommands(store)