__all__ = 'MultiCommand',


from dataclasses import dataclass
from typing import List, Self

from ..connection import RedisConnection
from ..protocol import *

from .base import RedisCommand


@dataclass(frozen=True)
class MultiCommand(RedisCommand):
    async def execute(self, conn: RedisConnection) -> RespValue:
        return RespSimpleString('OK')

    @classmethod
    def from_args(cls, args: List[bytes]) -> Self:
        if args:
            raise RuntimeError('MULTI command syntax: MULTI')
        return cls()
