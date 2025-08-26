__all__ = (
    'ZaddCommand',
    'ZcardCommand',
    'ZrangeCommand',
    'ZrankCommand',
    'ZscoreCommand',
)


from dataclasses import dataclass
from typing import List, Self, Tuple

from ..connection import RedisConnection
from ..data_structs import RedisSortedSet
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class ZaddCommand(RedisCommand):
    key: bytes
    score_member_pairs: List[Tuple[float, bytes]]

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            zset = database.setdefault(self.key, RedisSortedSet())
            if not isinstance(zset, RedisSortedSet):
                raise RuntimeError('WRONGTYPE')

            return RespInteger(zset.add(self.score_member_pairs))

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) < 3 or len(args) % 2 == 0:
            raise RuntimeError('ZADD command syntax: ZADD key score member [score member ...]')

        scores = [float(a) for a in args[1::2]]
        members = args[2::2]
        return cls(key=args[0], score_member_pairs=list(zip(scores, members)))


@dataclass(frozen=True)
class ZcardCommand(RedisCommand):
    key: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            zset = database.get(self.key)
            if zset is None:
                return RespInteger(0)

            if not isinstance(zset, RedisSortedSet):
                raise RuntimeError('WRONGTYPE')

            return RespInteger(len(zset))

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('ZCARD command syntax: ZCARD key')
        return cls(key=args[0])


@dataclass(frozen=True)
class ZrangeCommand(RedisCommand):
    key: bytes
    start: int
    stop: int

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            zset = database.get(self.key)
            if zset is None:
                return RespArray([])

            if not isinstance(zset, RedisSortedSet):
                raise RuntimeError('WRONGTYPE')

            elements = zset.get_range(self.start, self.stop)
            return RespArray([RespBulkString(e) for e in elements])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 3:
            raise RuntimeError('ZRANGE command syntax: ZRANGE key start stop')
        return cls(key=args[0], start=int(args[1]), stop=int(args[2]))


@dataclass(frozen=True)
class ZrankCommand(RedisCommand):
    key: bytes
    member: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            zset = database.get(self.key)
            if zset is None:
                return RespNullBulkString

            if not isinstance(zset, RedisSortedSet):
                raise RuntimeError('WRONGTYPE')

            try:
                return RespInteger(zset.get_rank(self.member))
            except ValueError:
                return RespNullBulkString

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 2:
            raise RuntimeError('ZRANK command syntax: ZRANK key member')
        return cls(key=args[0], member=args[1])


@dataclass(frozen=True)
class ZscoreCommand(RedisCommand):
    key: bytes
    member: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            zset = database.get(self.key)
            if zset is None:
                return RespNullBulkString

            if not isinstance(zset, RedisSortedSet):
                raise RuntimeError('WRONGTYPE')

            try:
                score = zset.get_score(self.member)
                return RespBulkString(str(score).encode())
            except ValueError:
                return RespNullBulkString

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 2:
            raise RuntimeError('ZSCORE command syntax: ZSCORE key member')
        return cls(key=args[0], member=args[1])
