__all__ = 'GetCommand', 'IncrCommand', 'SetCommand'


from dataclasses import dataclass
from typing import List, Optional, Self

from ..connection import RedisConnection
from ..data_structs import RedisString
from ..database import Expiry
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class GetCommand(RedisCommand):
    key: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            value = database.get(self.key)
            if value is None:
                return RespNullBulkString

            if not isinstance(value, RedisString):
                raise RuntimeError('WRONGTYPE')

            return RespBulkString(value.to_bytes())

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('GET command syntax: GET key')
        return cls(key=args[0])


@dataclass(frozen=True)
class IncrCommand(RedisCommand):
    key: bytes

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            value = database.setdefault(self.key, RedisString(b'0'))
            if not isinstance(value, RedisString):
                raise RuntimeError('WRONGTYPE')

            try:
                return RespInteger(value.incr())
            except ValueError:
                return RespSimpleError('ERR value is not an integer or out of range')

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) != 1:
            raise RuntimeError('INCR command syntax: INCR key')
        return cls(key=args[0])


@dataclass(frozen=True)
class SetCommand(RedisCommand):
    key: bytes
    value: bytes
    px: Optional[int] = None

    async def execute(self, conn: RedisConnection) -> RespValue:
        database = conn.database
        async with database.lock:
            expiry = Expiry.from_kwargs(px=self.px)
            database.set(self.key, RedisString(self.value), expiry)
            return RespSimpleString('OK')

    def is_write_command(self) -> bool:
        return True

    def to_resp_array(self) -> RespArray:
        bulk_strs = [RespBulkString('SET'), RespBulkString(self.key), RespBulkString(self.value)]
        if self.px is not None:
            bulk_strs += [RespBulkString('PX'), RespBulkString(str(self.px))]
        return RespArray(bulk_strs)

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if len(args) == 2:
            return cls(key=args[0], value=args[1])
        elif len(args) == 4 and args[2].upper() == b'PX':
            return cls(key=args[0], value=args[1], px=int(args[3]))
        raise RuntimeError('SET command syntax: SET key value [PX milliseconds]')
