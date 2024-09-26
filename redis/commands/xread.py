from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespArray, RespBulkString, RespSerializable
from ..stream import StreamEntryId

if TYPE_CHECKING:
    from ..client import RedisClient


class XreadCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        stream_key = self.argv[2]
        stream = client.server.database.get(stream_key)

        start = StreamEntryId.from_string(self.argv[3])
        start.seq_number += 1
        return RespArray([
            RespArray([
                RespBulkString(stream_key),
                stream.xrange(start),
            ])
        ])
