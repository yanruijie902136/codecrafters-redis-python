__all__ = ('GeoaddCommand', 'GeoposCommand')


from dataclasses import dataclass
from typing import List, Self

from ..connection import RedisConnection
from ..geocoding import compute_location, compute_score, is_valid_location
from ..protocol import *

from .base import RedisCommand
from .sorted_set_cmds import ZaddCommand, ZscoreCommand


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
        return await ZaddCommand(
            key=self.key,
            score_member_pairs=[(score, self.member)],
        ).execute(conn)

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


@dataclass(frozen=True)
class GeoposCommand(RedisCommand):
    key: bytes
    members: List[bytes]

    async def execute(self, conn: RedisConnection) -> RespValue:
        return RespArray([await self._compute(conn, member) for member in self.members])

    async def _compute(self, conn: RedisConnection, member: bytes) -> RespValue:
        response = await ZscoreCommand(key=self.key, member=member).execute(conn)
        if response is RespNullBulkString:
            return RespNullArray

        score = int(response.to_builtin())
        longitude, latitude = compute_location(score)
        return RespArray([RespBulkString(str(longitude)), RespBulkString(str(latitude))])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) < 2:
            raise RuntimeError('GEOPOS command syntax: GEOPOS key member [member ...]')
        return cls(key=args[0], members=args[1:])
