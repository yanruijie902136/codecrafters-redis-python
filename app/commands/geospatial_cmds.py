__all__ = 'GeoaddCommand',


from dataclasses import dataclass
from typing import List, Self

from ..connection import RedisConnection
from ..geocoding import compute_score, is_valid_location
from ..protocol import *

from .base import RedisCommand
from .sorted_set_cmds import ZaddCommand


@dataclass(frozen=True)
class GeoaddCommand(RedisCommand):
    key: bytes
    longitude: float
    latitude: float
    member: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        if not is_valid_location(self.longitude, self.latitude):
            return RespSimpleError('ERR invalid longitude or latitude')

        score = compute_score(self.longitude, self.latitude)
        zadd = ZaddCommand(
            key=self.key,
            score_member_pairs=[(score, self.member)],
        )
        return await zadd.execute(conn)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 4:
            raise RuntimeError('GEOADD command syntax: GEOADD key longitude latitude member')
        return cls(
            key=args[0],
            longitude=float(args[1]),
            latitude=float(args[2]),
            member=args[3],
        )
