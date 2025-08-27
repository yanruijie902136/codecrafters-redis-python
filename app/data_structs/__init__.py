__all__ = (
    'EntryId',
    'RedisDataStruct',
    'RedisList',
    'RedisSortedSet',
    'RedisStream',
    'RedisString',
    'StreamEntry',
)


from typing import TypeAlias, Union

from .list import RedisList
from .sorted_set import RedisSortedSet
from .stream import EntryId, RedisStream, StreamEntry
from .string import RedisString


RedisDataStruct: TypeAlias = Union[RedisList, RedisSortedSet, RedisStream, RedisString]
