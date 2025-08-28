__all__ = ('KeysCommand', 'TypeCommand', 'WaitCommand')


from dataclasses import dataclass
from fnmatch import fnmatch
from typing import List, Optional, Self

from ..connection import RedisConnection
from ..data_structs import *
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class KeysCommand(RedisCommand):
    pattern: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            matching_keys = [k for k in database.keys() if fnmatch(k, self.pattern)]
            return RespArray([RespBulkString(k) for k in matching_keys])

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('KEYS command syntax: KEYS pattern')
        return cls(pattern=args[0])


@dataclass(frozen=True)
class TypeCommand(RedisCommand):
    key: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            value = database.get(self.key)
            return RespSimpleString(self._stringify_type(value))

    def _stringify_type(self, value: Optional[RedisDataStruct]) -> str:
        if value is None:
            return 'none'

        if isinstance(value, RedisList):
            return 'list'
        elif isinstance(value, RedisSortedSet):
            return 'zset'
        elif isinstance(value, RedisStream):
            return 'stream'
        else:
            return 'string'

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('TYPE command syntax: TYPE key')
        return cls(key=args[0])


@dataclass(frozen=True)
class WaitCommand(RedisCommand):
    num_replicas: int
    timeout: int

    async def execute(self, conn: RedisConnection) -> RespValue:
        return RespInteger(0)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 2:
            raise RuntimeError('WAIT command syntax: WAIT numreplicas timeout')
        return cls(num_replicas=int(args[0]), timeout=int(args[1]))
