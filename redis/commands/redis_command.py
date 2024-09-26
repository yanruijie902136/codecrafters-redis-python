from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from ..resp2 import RespSerializable

if TYPE_CHECKING:
    from ..client import RedisClient


class RedisCommand(abc.ABC):
    def __init__(self, argv: list[str]) -> None:
        self.argv = argv

    @abc.abstractmethod
    def execute(self, client: RedisClient) -> RespSerializable:
        raise NotImplementedError
