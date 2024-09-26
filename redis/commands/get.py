from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespBulkString, RespSerializable, RespSimpleError

if TYPE_CHECKING:
    from ..client import RedisClient


class GetCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        key = self.argv[1]
        if (value := client.server.database.get(key)) is None:
            return RespBulkString(None)
        if type(value) is str:
            return RespBulkString(value)
        return RespSimpleError(
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )
