from __future__ import annotations
import dataclasses
from typing import Optional, Union

from .stream import RedisStream
from .utils import get_current_timestamp


DataStruct = Union[RedisStream, str]


@dataclasses.dataclass
class RedisDatabaseValue:
    value: DataStruct
    expire_timestamp: float = float('inf')


class RedisDatabase:
    def __init__(self):
        self._database: dict[str, RedisDatabaseValue] = {}

    def get(self, key: str) -> Optional[DataStruct]:
        """Get the value stored at a key, or None if the key has expired."""
        if (item := self._database.get(key)) is None:
            return None
        elif get_current_timestamp() >= item.expire_timestamp:
            self._database.pop(key)
            return None
        return item.value

    def set(
        self, key: str, value: DataStruct, expire_time: Optional[float] = None
    ) -> None:
        """
        Set the value stored at a key, with an optional expire time in
        milliseconds.
        """
        if expire_time is None:
            self._database[key] = RedisDatabaseValue(value)
        else:
            expire_timestamp = get_current_timestamp() + expire_time
            self._database[key] = RedisDatabaseValue(value, expire_timestamp)

    def increment(self, key: str) -> Optional[int]:
        """
        Increment the value stored at a key, and return the incremented value.
        If the key doesn't exist, it's set to 0 before the operation. None is
        returned if the key contains a value of the wrong type or a string that
        can't be represented as integer.
        """
        if (item := self._database.get(key)) is None:
            self.set(key, value="1")
            return 1
        try:
            incremented_value = int(item.value) + 1
            item.value = str(incremented_value)
            return incremented_value
        except:
            return None
