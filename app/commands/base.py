__all__ = 'RedisCommand',


from abc import ABC, abstractmethod
from typing import List, Self

from ..connection import RedisConnection
from ..protocol import RespArray, RespValue


class RedisCommand(ABC):
    @abstractmethod
    async def execute(self, conn: RedisConnection) -> RespValue:
        raise NotImplementedError

    def to_resp_array(self) -> RespArray:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_args(cls, args: List[bytes]) -> Self:
        raise NotImplementedError
