class RedisString:
    def __init__(self, value: bytes) -> None:
        self._seq = bytearray(value)

    def to_bytes(self) -> bytes:
        return bytes(self._seq)
