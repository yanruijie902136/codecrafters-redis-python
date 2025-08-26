import asyncio
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

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
        self._conds: Dict[bytes, asyncio.Condition] = {}

    def delete(self, key: bytes) -> None:
        self._kv.pop(key, None)
        self._conds.pop(key, None)

    def get(self, key: bytes) -> Optional[RedisDataStruct]:
        v = self._kv.get(key)
        if v is None:
            return None

        if v.expiry is not None and v.expiry.has_passed():
            del self._kv[key]
            return None

        return v.value

    def keys(self) -> List[bytes]:
        return [k for k, v in self._kv.items() if v.expiry is None or not v.expiry.has_passed()]

    def notify(self, key: bytes) -> None:
        cond = self._get_cond(key)
        cond.notify_all()

    def set(self, key: bytes, value: RedisDataStruct, expiry: Optional[Expiry] = None) -> None:
        self._kv[key] = _ValueWithExpiry(value, expiry)

    def setdefault(self, key: bytes, default: RedisDataStruct) -> RedisDataStruct:
        value = self.get(key)
        if value is not None:
            return value

        self.set(key, default)
        return default

    async def wait_for(self, key: bytes, predicate: Callable[[RedisDataStruct], bool], *, timeout: float = 0.0) -> Optional[RedisDataStruct]:
        cond = self._get_cond(key)
        delay = timeout if timeout else None

        try:
            async with asyncio.timeout(delay):
                while True:
                    value = self.get(key)
                    if value is not None and predicate(value):
                        return value
                    await cond.wait()
        except TimeoutError:
            return None

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    def _get_cond(self, key: bytes) -> asyncio.Condition:
        return self._conds.setdefault(key, asyncio.Condition(self._lock))
