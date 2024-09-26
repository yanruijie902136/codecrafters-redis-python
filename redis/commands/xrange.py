from __future__ import annotations

import itertools
from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespSerializable
from ..stream import StreamEntryId

if TYPE_CHECKING:
    from ..client import RedisClient


class XrangeCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        stream_key = self.argv[1]
        stream = client.server.database.get(stream_key)
        start = (
            None
            if self.argv[2] == "-"
            else StreamEntryId.from_string(self.argv[2])
        )
        end = (
            None
            if self.argv[3] == "+"
            else StreamEntryId.from_string(self.argv[3])
        )
        return stream.xrange(start, end)
