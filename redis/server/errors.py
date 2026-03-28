class RedisError(Exception):
    pass

class WrongTypeError(RedisError):
    def __init__(self, key, expected, actual):
        super().__init__(
            f"WRONGTYPE Key '{key}' expected {expected}, got {actual}"
        )

class KeyNotFoundError(RedisError):
    def __init__(self):
        super().__init__("Key not found")