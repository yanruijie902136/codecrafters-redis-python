import asyncio
from dataclasses import dataclass
from typing import Dict, Optional

from ..data_structs import RedisDataStruct

from .expiry import Expiry


@dataclass
class _ValueWithExpiry:
    value: RedisDataStruct
    expiry: Optional[Expiry] = None


class RedisDatabase:
    def __init__(self) -> None:
        self._kv: Dict[bytes, _ValueWithExpiry] = {}

        self._lock = asyncio.Lock()

    def get(self, key: bytes) -> Optional[RedisDataStruct]:
        v = self._kv.get(key)
        if v is None:
            return None

        if v.expiry is not None and v.expiry.has_passed():
            del self._kv[key]
            return None

        return v.value

    def set(self, key: bytes, value: RedisDataStruct, expiry: Optional[Expiry] = None) -> None:
        self._kv[key] = _ValueWithExpiry(value, expiry)

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock
