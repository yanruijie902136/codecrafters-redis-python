from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from .resp2 import RespArray

if TYPE_CHECKING:
    from .client import RedisClient
    from .commands import RedisCommand


class RedisTransaction:
    def __init__(self, client: RedisClient) -> None:
        self._client = client
        self._command_queue: Optional[list[RedisCommand]] = None

    def start(self) -> bool:
        if self._command_queue is not None:
            return False
        self._command_queue = []
        return True

    def queue(self, command: RedisCommand) -> bool:
        from .commands import DiscardCommand, ExecCommand, MultiCommand

        if self._command_queue is None or \
                type(command) in [DiscardCommand, ExecCommand, MultiCommand]:
            # DISCARD, EXEC, and MULTI commands will always be executed.
            return False
        self._command_queue.append(command)
        return True

    def discard(self) -> bool:
        if self._command_queue is None:
            return False
        self._command_queue = None
        return True

    def exec(self) -> Optional[RespArray]:
        if self._command_queue is None:
            return None
        responses = [
            command.execute(self._client) for command in self._command_queue
        ]
        self._command_queue = None
        return RespArray(responses)
