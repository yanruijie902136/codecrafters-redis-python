__all__ = 'GeoaddCommand',


from dataclasses import dataclass
from typing import List, Self

from ..connection import RedisConnection
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class GeoaddCommand(RedisCommand):
    key: bytes
    longitude: float
    latitude: float
    member: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        if not self._is_valid_args():
            return RespSimpleError('ERR invalid longitude or latitude')
        return RespInteger(1)

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
