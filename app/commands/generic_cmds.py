__all__ = 'KeysCommand',


from dataclasses import dataclass
from fnmatch import fnmatch
from typing import List, Self

from ..connection import RedisConnection
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
