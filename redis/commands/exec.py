from __future__ import annotations

from typing import TYPE_CHECKING

from .redis_command import RedisCommand
from ..resp2 import RespSerializable, RespSimpleError

if TYPE_CHECKING:
    from ..client import RedisClient


class ExecCommand(RedisCommand):
    def execute(self, client: RedisClient) -> RespSerializable:
        if (response_array := client.transaction.exec()) is None:
            return RespSimpleError("ERR EXEC without MULTI")
        return response_array
