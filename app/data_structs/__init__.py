__all__ = 'RedisDataStruct', 'RedisString'


from typing import TypeAlias, Union

from .string import RedisString


RedisDataStruct: TypeAlias = Union[RedisString]
