__all__ = (
    'LlenCommand',
    'LpopCommand',
    'LpushCommand',
    'LrangeCommand',
    'RpushCommand',
)


from dataclasses import dataclass
from typing import List, Optional, Self

from ..connection import RedisConnection
from ..data_structs import RedisList
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class LlenCommand(RedisCommand):
    key: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            lst = database.get(self.key)
            if lst is None:
                return RespInteger(0)

            if not isinstance(lst, RedisList):
                raise RuntimeError('WRONGTYPE')

            return RespInteger(len(lst))

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('LLEN command syntax: LLEN key')
        return cls(key=args[0])


@dataclass(frozen=True)
class LpopCommand(RedisCommand):
    key: bytes
    count: Optional[int] = None

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            lst = database.get(self.key)
            if lst is None:
                return RespNullBulkString

            if not isinstance(lst, RedisList):
                raise RuntimeError('WRONGTYPE')

            popped = lst.lpop(self.count)
            if not lst:
                database.delete(self.key)

            if isinstance(popped, list):
                return RespArray([RespBulkString(e) for e in popped])
            return RespBulkString(popped)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) == 1:
            return cls(key=args[0])
        elif len(args) == 2:
            return cls(key=args[0], count=int(args[1]))
        raise RuntimeError('LPOP command syntax: LPOP key [count]')


@dataclass(frozen=True)
class LpushCommand(RedisCommand):
    key: bytes
    elements: List[bytes]

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            lst = database.setdefault(self.key, RedisList())
            if not isinstance(lst, RedisList):
                raise RuntimeError('WRONGTYPE')

            lst.lpush(self.elements)
            return RespInteger(len(lst))

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) < 2:
            raise RuntimeError('LPUSH command syntax: LPUSH key value [value ...]')
        return cls(key=args[0], elements=args[1:])


@dataclass(frozen=True)
class LrangeCommand(RedisCommand):
    key: bytes
    start: int
    stop: int

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            lst = database.get(self.key)
            if lst is None:
                return RespArray([])

            if not isinstance(lst, RedisList):
                raise RuntimeError('WRONGTYPE')

            elements = lst.get_range(self.start, self.stop)
            return RespArray([RespBulkString(e) for e in elements])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 3:
            raise RuntimeError('LRANGE command syntax: LRANGE key start stop')
        return cls(key=args[0], start=int(args[1]), stop=int(args[2]))


@dataclass(frozen=True)
class RpushCommand(RedisCommand):
    key: bytes
    elements: List[bytes]

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            lst = database.setdefault(self.key, RedisList())
            if not isinstance(lst, RedisList):
                raise RuntimeError('WRONGTYPE')

            lst.rpush(self.elements)
            return RespInteger(len(lst))

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) < 2:
            raise RuntimeError('RPUSH command syntax: RPUSH key value [value ...]')
        return cls(key=args[0], elements=args[1:])
