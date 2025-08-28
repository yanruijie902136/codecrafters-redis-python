__all__ = ('ConfigGetCommand', 'InfoCommand', 'ReplconfCommand')


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
        return RespBulkString(
            f'role:{conn.server.role}\r\n'
            'master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n'
            'master_repl_offset:0'
        )

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('INFO command syntax: INFO section')
        return cls(section=args[0].decode())


@dataclass(frozen=True)
class ReplconfCommand(RedisCommand):
    args: List[str]

    async def execute(self, conn: RedisConnection) -> RespValue:
        return RespSimpleString('OK')

    def to_resp_array(self) -> RespArray:
        return RespArray([RespBulkString('REPLCONF')] + [RespBulkString(arg) for arg in self.args])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        return cls(args=[arg.decode() for arg in args])
