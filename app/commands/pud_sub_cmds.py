__all__ = ('PublishCommand', 'SubscribeCommand', 'UnsubscribeCommand')


from dataclasses import dataclass
from typing import List, Self

from ..channel import count_subbed_channels, count_subscribers, publish, subscribe, unsubscribe
from ..connection import RedisConnection
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class PublishCommand(RedisCommand):
    channel: str
    message: str

    async def execute(self, conn: RedisConnection) -> RespValue:
        await publish(self.channel, self.message)
        return RespInteger(count_subscribers(self.channel))

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 2:
            raise RuntimeError('PUBLISH command syntax: PUBLISH channel message')
        return cls(channel=args[0].decode(), message=args[1].decode())


@dataclass(frozen=True)
class SubscribeCommand(RedisCommand):
    channel: str

    async def execute(self, conn: RedisConnection) -> RespValue:
        subscribe(conn, self.channel)
        return RespArray([
            RespBulkString('subscribe'),
            RespBulkString(self.channel),
            RespInteger(count_subbed_channels(conn)),
        ])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('SUBSCRIBE command syntax: SUBSCRIBE channel')
        return cls(channel=args[0].decode())


@dataclass(frozen=True)
class UnsubscribeCommand(RedisCommand):
    channel: str

    async def execute(self, conn: RedisConnection) -> RespValue:
        unsubscribe(conn, self.channel)
        return RespArray([
            RespBulkString('unsubscribe'),
            RespBulkString(self.channel),
            RespInteger(count_subbed_channels(conn)),
        ])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('UNSUBSCRIBE command syntax: UNSUBSCRIBE channel')
        return cls(channel=args[0].decode())
