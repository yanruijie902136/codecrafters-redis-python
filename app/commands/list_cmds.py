__all__ = ('LrangeCommand', 'RpushCommand')


from dataclasses import dataclass
from typing import List, Self

from ..connection import RedisConnection
from ..data_structs import RedisList
from ..protocol import *

from .base import RedisCommand


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
