from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespBulkString, RespSerializable
from ..stream import RedisStream

if TYPE_CHECKING:
    from ..client import RedisClient


class XaddCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        stream_key, entry_id = self.argv[1], self.argv[2]
        client.server.database.set(stream_key, RedisStream())
        return RespBulkString(entry_id)
