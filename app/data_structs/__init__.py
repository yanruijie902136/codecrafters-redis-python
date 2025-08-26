__all__ = ('RedisDataStruct', 'RedisList', 'RedisString')


from typing import TypeAlias, Union

from .list import RedisList
from .string import RedisString


RedisDataStruct: TypeAlias = Union[RedisList, RedisString]
