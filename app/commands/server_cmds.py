__all__ = (
    'ConfigGetCommand',
    'InfoCommand',
    'PsyncCommand',
    'ReplconfCommand',
)


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
        server = conn.server
        return RespBulkString(
            f'role:{server.role}\r\n'
            f'master_replid:{server.replication_id}\r\n'
            f'master_repl_offset:{server.replication_offset}'
        )

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('INFO command syntax: INFO section')
        return cls(section=args[0].decode())


@dataclass(frozen=True)
class PsyncCommand(RedisCommand):
    replication_id: str
    offset: int

    async def execute(self, conn: RedisConnection) -> RespValue:
        server = conn.server
        return RespSimpleString(f'FULLRESYNC {server.replication_id} {server.replication_offset}')

    def to_resp_array(self) -> RespArray:
        return RespArray([
            RespBulkString('PSYNC'),
            RespBulkString(self.replication_id),
            RespBulkString(str(self.offset)),
        ])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 2:
            raise RuntimeError('PSYNC command syntax: PSYNC replication_id offset')
        return cls(replication_id=args[0].decode(), offset=int(args[1]))


@dataclass(frozen=True)
class ReplconfCommand(RedisCommand):
    args: List[str]

    async def execute(self, conn: RedisConnection) -> RespValue:
        if self.args[0] == 'GETACK':
            return ReplconfCommand(args=['ACK', str(conn.server.replication_offset)]).to_resp_array()
        return RespSimpleString('OK')

    def to_resp_array(self) -> RespArray:
        return RespArray([RespBulkString('REPLCONF')] + [RespBulkString(arg) for arg in self.args])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        return cls(args=[arg.decode() for arg in args])
