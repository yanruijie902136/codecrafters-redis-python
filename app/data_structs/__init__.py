__all__ = ('RedisDataStruct', 'RedisList', 'RedisSortedSet', 'RedisString')


from typing import TypeAlias, Union

from .list import RedisList
from .sorted_set import RedisSortedSet
from .string import RedisString


RedisDataStruct: TypeAlias = Union[RedisList, RedisSortedSet, RedisString]
