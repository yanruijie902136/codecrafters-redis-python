from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespInteger, RespSerializable, RespSimpleError

if TYPE_CHECKING:
    from ..client import RedisClient


class IncrCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        key = self.argv[1]
        incremented_value = client.server.database.increment(key)
        if incremented_value is not None:
            return RespInteger(incremented_value)
        return RespSimpleError("ERR value is not an integer or out of range")
