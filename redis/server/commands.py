"""
The entry point for parsed RESP commands over TCP.
"""

from __future__ import annotations
from .store import Store
from .types import StringCommands, ListCommands, SetCommands

class CommandError(Exception):
    """Raised for unknown commands or wrong number of arguments."""
    pass

class CommandDispatcher:
    """
    Routes parsed commands to the appropriate type handler.

    Usage:
        cd = CommandDispatcher()
        cd.execute("SET name alice")   # "OK"
        cd.execute("GET name")         # "alice"
        cd.execute("LPUSH q a b c")    # 3
    """

    def __init__(self, store: Store | None = None) -> None:
        self.store   = store or Store()
        self.strings = StringCommands(self.store)
        self.lists   = ListCommands(self.store)
        self.sets    = SetCommands(self.store)

        # Dispatch table: command name -> handler function
        # Each handler receives the raw token list (excluding the command name).
        self._handlers: dict[str, callable] = {
            # Key-space
            "DEL":      self._del,
            "EXISTS":   self._exists,
            "KEYS":     self._keys,
            "TYPE":     self._type,
            "FLUSH":    self._flush,

            # TTL
            "EXPIRE":   self._expire,
            "PEXPIRE":  self._pexpire,
            "TTL":      self._ttl,
            "PTTL":     self._pttl,
            "PERSIST":  self._persist,

            # Strings
            "SET":      self._set,
            "GET":      self._get,
            "GETSET":   self._getset,
            "MSET":     self._mset,
            "MGET":     self._mget,
            "INCR":     self._incr,
            "INCRBY":   self._incrby,
            "DECR":     self._decr,
            "DECRBY":   self._decrby,
            "APPEND":   self._append,
            "STRLEN":   self._strlen,
            "SETNX":    self._setnx,

            # Lists
            "LPUSH":    self._lpush,
            "RPUSH":    self._rpush,
            "LPOP":     self._lpop,
            "RPOP":     self._rpop,
            "LLEN":     self._llen,
            "LRANGE":   self._lrange,
            "LINDEX":   self._lindex,
            "LSET":     self._lset,

            # Sets
            "SADD":     self._sadd,
            "SREM":     self._srem,
            "SISMEMBER":self._sismember,
            "SMEMBERS": self._smembers,
            "SCARD":    self._scard,
            "SMOVE":    self._smove,
            "SUNION":   self._sunion,
            "SINTER":   self._sinter,
            "SDIFF":    self._sdiff,
            "SUNIONSTORE": self._sunionstore,
            "SINTERSTORE": self._sinterstore,
        }

    # Public entry point

    def execute(self, raw: str) -> object:
        """
        Parse and execute a raw command string.
        Returns the command result (str, int, list, set, bool, or None).
        Raises CommandError for unknown commands / wrong arg count.
        Raises StoreError for type mismatches.
        """
        tokens = raw.strip().split()
        if not tokens:
            raise CommandError("Empty command")
        cmd = tokens[0].upper()
        args = tokens[1:]
        handler = self._handlers.get(cmd)
        if handler is None:
            raise CommandError(f"Unknown command: {cmd!r}")
        return handler(args)
    
    # Argument validation helper
    
    def _require(self, args: list, *, exact: int = None, min_n: int = None) -> None:
        if exact is not None and len(args) != exact:
            raise CommandError(
                f"Wrong number of arguments: expected {exact}, got {len(args)}"
            )
        if min_n is not None and len(args) < min_n:
            raise CommandError(
                f"Wrong number of arguments: expected at least {min_n}, got {len(args)}"
            )

    # Key-space handlers

    def _del(self, args):
        self._require(args, min_n=1)
        return self.store.delete(*args)

    def _exists(self, args):
        self._require(args, exact=1)
        return self.store.exists(args[0])

    def _keys(self, args):
        return self.store.keys()

    def _type(self, args):
        self._require(args, exact=1)
        t = self.store.type_of(args[0])
        return t or "none"

    def _flush(self, args):
        self.store.flush()
        return "OK"

    # TTL handlers

    def _expire(self, args):
        self._require(args, exact=2)
        return self.store.set_ttl(args[0], float(args[1]))

    def _pexpire(self, args):
        self._require(args, exact=2)
        return self.store.set_ttl(args[0], float(args[1]) / 1000.0)

    def _ttl(self, args):
        self._require(args, exact=1)
        return self.store.ttl(args[0])

    def _pttl(self, args):
        self._require(args, exact=1)
        return self.store.pttl(args[0])

    def _persist(self, args):
        self._require(args, exact=1)
        return self.store.persist(args[0])

    # String handlers

    def _set(self, args):
        """
        SET key value [EX seconds] [PX milliseconds]
        EX / PX are optional TTL flags — same syntax as real Redis.
        """
        if len(args) < 2:
            raise CommandError("SET requires at least key and value")
        key, value = args[0], args[1]
        result = self.strings.set(key, value)
        # Clear any existing TTL first (SET resets it unless EX/PX given)
        from .ttl import clear_expiry
        clear_expiry(self.store._data[key])
        # Parse optional EX / PX flags
        i = 2
        while i < len(args):
            flag = args[i].upper()
            if flag in ("EX", "PX"):
                if i + 1 >= len(args):
                    raise CommandError(f"{flag} requires a value")
                ttl_val = float(args[i + 1])
                if ttl_val <= 0:
                    raise CommandError("TTL must be positive")
                seconds = ttl_val if flag == "EX" else ttl_val / 1000.0
                self.store.set_ttl(key, seconds)
                i += 2
            else:
                raise CommandError(f"Unknown SET option: {flag!r}")
        return result

    def _get(self, args):
        self._require(args, exact=1)
        return self.strings.get(args[0])

    def _getset(self, args):
        self._require(args, exact=2)
        return self.strings.getset(args[0], args[1])

    def _mset(self, args):
        if len(args) < 2 or len(args) % 2 != 0:
            raise CommandError("MSET requires an even number of arguments")
        mapping = {args[i]: args[i+1] for i in range(0, len(args), 2)}
        return self.strings.mset(mapping)

    def _mget(self, args):
        self._require(args, min_n=1)
        return self.strings.mget(*args)

    def _incr(self, args):
        self._require(args, exact=1)
        return self.strings.incr(args[0])

    def _incrby(self, args):
        self._require(args, exact=2)
        return self.strings.incr(args[0], int(args[1]))

    def _decr(self, args):
        self._require(args, exact=1)
        return self.strings.decr(args[0])

    def _decrby(self, args):
        self._require(args, exact=2)
        return self.strings.decr(args[0], int(args[1]))

    def _append(self, args):
        self._require(args, exact=2)
        return self.strings.append(args[0], args[1])

    def _strlen(self, args):
        self._require(args, exact=1)
        return self.strings.strlen(args[0])

    def _setnx(self, args):
        self._require(args, exact=2)
        return self.strings.setnx(args[0], args[1])

    # List handlers

    def _lpush(self, args):
        self._require(args, min_n=2)
        return self.lists.lpush(args[0], *args[1:])

    def _rpush(self, args):
        self._require(args, min_n=2)
        return self.lists.rpush(args[0], *args[1:])

    def _lpop(self, args):
        self._require(args, exact=1)
        return self.lists.lpop(args[0])

    def _rpop(self, args):
        self._require(args, exact=1)
        return self.lists.rpop(args[0])

    def _llen(self, args):
        self._require(args, exact=1)
        return self.lists.llen(args[0])

    def _lrange(self, args):
        self._require(args, exact=3)
        return self.lists.lrange(args[0], int(args[1]), int(args[2]))

    def _lindex(self, args):
        self._require(args, exact=2)
        return self.lists.lindex(args[0], int(args[1]))

    def _lset(self, args):
        self._require(args, exact=3)
        return self.lists.lset(args[0], int(args[1]), args[2])

    # Set handlers

    def _sadd(self, args):
        self._require(args, min_n=2)
        return self.sets.sadd(args[0], *args[1:])

    def _srem(self, args):
        self._require(args, min_n=2)
        return self.sets.srem(args[0], *args[1:])

    def _sismember(self, args):
        self._require(args, exact=2)
        return self.sets.sismember(args[0], args[1])

    def _smembers(self, args):
        self._require(args, exact=1)
        return self.sets.smembers(args[0])

    def _scard(self, args):
        self._require(args, exact=1)
        return self.sets.scard(args[0])

    def _smove(self, args):
        self._require(args, exact=3)
        return self.sets.smove(args[0], args[1], args[2])

    def _sunion(self, args):
        self._require(args, min_n=1)
        return self.sets.sunion(*args)

    def _sinter(self, args):
        self._require(args, min_n=1)
        return self.sets.sinter(*args)

    def _sdiff(self, args):
        self._require(args, min_n=1)
        return self.sets.sdiff(*args)

    def _sunionstore(self, args):
        self._require(args, min_n=2)
        return self.sets.sunionstore(args[0], *args[1:])

    def _sinterstore(self, args):
        self._require(args, min_n=2)
        return self.sets.sinterstore(args[0], *args[1:])
