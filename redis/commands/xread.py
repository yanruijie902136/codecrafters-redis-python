from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespArray, RespBulkString, RespSerializable
from ..stream import StreamEntryId

if TYPE_CHECKING:
    from ..client import RedisClient


class XreadCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        num_streams = (len(self.argv) - 2) // 2
        stream_keys = self.argv[2:num_streams+2]
        starts = [
            StreamEntryId.from_string(s) for s in self.argv[num_streams+2:]
        ]

        xreads = []
        for stream_key, start in zip(stream_keys, starts):
            stream = client.server.database.get(stream_key)
            xreads.append(
                RespArray([
                    RespBulkString(stream_key),
                    stream.xread(start),
                ])
            )
        return RespArray(xreads)
