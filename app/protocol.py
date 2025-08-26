__all__ = (
    'RespArray',
    'RespBulkString',
    'RespNullArray',
    'RespNullBulkString',
    'RespSimpleString',
    'RespValue',
    'resp_decode',
)


import asyncio
from abc import ABC, abstractmethod
from typing import Any, List


class RespValue(ABC):
    @abstractmethod
    def encode(self) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def to_builtin(self) -> Any:
        raise NotImplementedError


class RespSimpleString(RespValue):
    def __init__(self, value: str) -> None:
        self._value = value

    def encode(self) -> bytes:
        return f'+{self._value}\r\n'.encode()

    def to_builtin(self) -> str:
        return self._value


class RespBulkString(RespValue):
    def __init__(self, value: bytes) -> None:
        self._value = value

    def encode(self) -> bytes:
        return f'${len(self._value)}\r\n'.encode() + self._value + b'\r\n'

    def to_builtin(self) -> bytes:
        return self._value


class _RespNullBulkString(RespValue):
    def encode(self) -> bytes:
        return b'$-1\r\n'

    def to_builtin(self) -> None:
        return None


RespNullBulkString = _RespNullBulkString()


class RespArray(RespValue):
    def __init__(self, values: List[RespValue]) -> None:
        self._values = values

    def encode(self) -> bytes:
        encoded_values = b''.join(v.encode() for v in self._values)
        return f'*{len(self._values)}\r\n'.encode() + encoded_values

    def to_builtin(self) -> List[Any]:
        return [v.to_builtin() for v in self._values]


class _RespNullArray(RespValue):
    def encode(self) -> bytes:
        return b'*-1\r\n'

    def to_builtin(self) -> None:
        return None


RespNullArray = _RespNullArray()


async def resp_decode(reader: asyncio.StreamReader) -> RespValue:
    b = await reader.readexactly(1)

    match b:
        case b'+':
            value = (await reader.readuntil(b'\r\n'))[:-2].decode()
            return RespSimpleString(value)

        case b'$':
            length = int((await reader.readuntil(b'\r\n'))[:-2])
            if length < 0:
                return RespNullBulkString
            value = (await reader.readexactly(length + 2))[:-2]
            return RespBulkString(value)

        case b'*':
            length = int((await reader.readuntil(b'\r\n'))[:-2])
            if length < 0:
                return RespNullArray
            values = [await resp_decode(reader) for _ in range(length)]
            return RespArray(values)

    raise RuntimeError(f'Unexpected byte in RESP decoding: {b!r}')
