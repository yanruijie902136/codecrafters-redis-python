__all__ = ('SubscribeCommand',)


from dataclasses import dataclass
from typing import List, Self

from ..connection import RedisConnection
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class SubscribeCommand(RedisCommand):
    channel: str

    async def execute(self, conn: RedisConnection) -> RespValue:
        conn.subscribe(self.channel)
        return RespArray([
            RespBulkString('subscribe'),
            RespBulkString(self.channel),
            RespInteger(conn.num_subbed_channels),
        ])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('SUBSCRIBE command syntax: SUBSCRIBE channel')
        return cls(channel=args[0].decode())
