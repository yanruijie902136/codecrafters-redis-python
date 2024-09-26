import dataclasses
import math
from typing import Optional, Union

from .stream import RedisStream
from .utils import get_current_timestamp


DataStruct = Union[RedisStream, str]


@dataclasses.dataclass
class RedisDatabaseValue:
    value: DataStruct
    expire_timestamp: float = math.inf


class RedisDatabase:
    def __init__(self) -> None:
        self._database: dict[str, RedisDatabaseValue] = {}

    def set(
        self,
        key: str,
        value: DataStruct,
        *,
        expire_time: Optional[float] = None,
        expire_timestamp: Optional[float] = None,
    ) -> None:
        if expire_time is None and expire_timestamp is None:
            self._database[key] = RedisDatabaseValue(value)
            return
        if expire_time is not None:
            expire_timestamp = get_current_timestamp() + expire_time
        self._database[key] = RedisDatabaseValue(value, expire_timestamp)

    def get(self, key: str) -> Optional[DataStruct]:
        if (item := self._database.get(key)) is None:
            return None
        elif get_current_timestamp() >= item.expire_timestamp:
            self._database.pop(key)
            return None
        return item.value

    def increment(self, key: str) -> Optional[int]:
        try:
            item = self._database[key]
            incremented_value = int(item.value) + 1
            item.value = str(incremented_value)
            return incremented_value
        except KeyError:
            self.set(key, "1")
            return 1
        except (TypeError, ValueError):
            return None
