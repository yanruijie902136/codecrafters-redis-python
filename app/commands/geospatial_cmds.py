__all__ = 'GeoaddCommand',


from dataclasses import dataclass
from typing import List, Self

from ..connection import RedisConnection
from ..protocol import *

from .base import RedisCommand
from .sorted_set_cmds import ZaddCommand


def _compute_score(longitude: float, latitude: float) -> float:
    return 0.0


@dataclass(frozen=True)
class GeoaddCommand(RedisCommand):
    key: bytes
    longitude: float
    latitude: float
    member: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        if not self._is_valid_args():
            return RespSimpleError('ERR invalid longitude or latitude')

        score = _compute_score(self.longitude, self.latitude)
        zadd = ZaddCommand(
            key=self.key,
            score_member_pairs=[(score, self.member)],
        )
        return await zadd.execute(conn)

    def _is_valid_args(self) -> bool:
        return -180 <= self.longitude <= 180 and -85.05112878 <= self.latitude <= 85.05112878

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
