__all__ = 'EchoCommand', 'PingCommand'


from dataclasses import dataclass
from typing import List, Self

from ..connection import RedisConnection
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class EchoCommand(RedisCommand):
    message: str

    async def execute(self, conn: RedisConnection) -> RespValue:
        return RespBulkString(self.message)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('ECHO command syntax: ECHO message')
        return cls(message=args[0].decode())


@dataclass(frozen=True)
class PingCommand(RedisCommand):
    async def execute(self, conn: RedisConnection) -> RespValue:
        return RespSimpleString('PONG')

    def to_resp_array(self) -> RespArray:
        return RespArray([RespBulkString('PING')])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if args:
            raise RuntimeError('PING command syntax: PING')
        return cls()
