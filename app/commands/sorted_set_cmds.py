__all__ = (
    'ZaddCommand',
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
