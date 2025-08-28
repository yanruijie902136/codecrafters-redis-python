__all__ = 'ConfigGetCommand', 'InfoCommand'


from dataclasses import dataclass
from typing import List, Self

from ..connection import RedisConnection
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class ConfigGetCommand(RedisCommand):
    parameters: List[str]

    async def execute(self, conn: RedisConnection) -> RespValue:
        values = []
        for param in self.parameters:
            value = conn.server.config.get(param)
            if value is not None:
                values += [RespBulkString(param), RespBulkString(value)]
        return RespArray(values)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if not args:
            raise RuntimeError('CONFIG GET command syntax: CONFIG GET parameter [parameter ...]')
        return cls(parameters=[arg.decode() for arg in args])


@dataclass(frozen=True)
class InfoCommand(RedisCommand):
    section: str

    async def execute(self, conn: RedisConnection) -> RespValue:
        return RespBulkString(f'role:{conn.server.role}')

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('INFO command syntax: INFO section')
        return cls(section=args[0].decode())
