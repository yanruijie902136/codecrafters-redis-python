from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespSerializable, RespSimpleString
from ..stream import RedisStream

if TYPE_CHECKING:
    from ..client import RedisClient


class TypeCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        key = self.argv[1]
        if (value := client.server.database.get(key)) is None:
            return RespSimpleString("none")
        if type(value) is RedisStream:
            return RespSimpleString("stream")
        return RespSimpleString("string")
