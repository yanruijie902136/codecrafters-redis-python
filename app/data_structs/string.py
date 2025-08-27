class RedisString:
    def __init__(self, value: bytes) -> None:
        self._seq = bytearray(value)

    def incr(self) -> int:
        new = int(self._seq) + 1
        self._seq = bytearray(str(new).encode())
        return new

    def to_bytes(self) -> bytes:
        return bytes(self._seq)
