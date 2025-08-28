from collections import defaultdict
from typing import Set

from .connection import RedisConnection
from .protocol import RespArray, RespBulkString


_channels = defaultdict[str, Set[RedisConnection]](set)


def count_subbed_channels(conn: RedisConnection) -> int:
    return sum(1 for conns in _channels.values() if conn in conns)


def count_subscribers(channel: str) -> int:
    return len(_channels[channel])


def has_subbed(conn: RedisConnection) -> bool:
    return count_subbed_channels(conn) > 0


async def publish(channel: str, message: str) -> None:
    for conn in _channels[channel]:
        await conn.write_resp(RespArray([
            RespBulkString('message'),
            RespBulkString(channel),
            RespBulkString(message),
        ]))


def subscribe(conn: RedisConnection, channel: str) -> None:
    _channels[channel].add(conn)
