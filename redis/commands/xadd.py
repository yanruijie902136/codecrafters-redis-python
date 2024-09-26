from __future__ import annotations

import itertools
from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespBulkString, RespSerializable, RespSimpleError
from ..stream import RedisStream, StreamEntryId

if TYPE_CHECKING:
    from ..client import RedisClient


class XaddCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        entry_id = StreamEntryId.from_string(self.argv[2])
        if entry_id.msec == entry_id.seq_number == 0:
            return RespSimpleError(
                "ERR The ID specified in XADD must be greater than 0-0"
            )

        database = client.server.database
        stream_key = self.argv[1]
        if (stream := database.get(stream_key)) is None:
            stream = RedisStream()
            database.set(stream_key, stream)

        data = {k: v for k, v in itertools.batched(self.argv[3:], 2)}
        if not stream.add_entry(entry_id, data):
            return RespSimpleError(
                "ERR The ID specified in XADD is equal or smaller than "
                "the target stream top item"
            )

        return RespBulkString(str(entry_id))
