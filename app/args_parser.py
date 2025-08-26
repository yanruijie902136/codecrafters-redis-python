from typing import Dict, List, Type

from .commands import *


_COMMAND_CLASSES: Dict[str, Type[RedisCommand]] = {
    'ECHO': EchoCommand,
    'GET': GetCommand,
    'LLEN': LlenCommand,
    'LPUSH': LpushCommand,
    'LRANGE': LrangeCommand,
    'PING': PingCommand,
    'RPUSH': RpushCommand,
    'SET': SetCommand,
}


def parse_args_to_command(args: List[bytes]) -> RedisCommand:
    command_name, args = args[0].decode(), args[1:]
    command_class = _COMMAND_CLASSES.get(command_name.upper())
    if not command_class:
        raise RuntimeError(f'Unknown command: {command_name}')
    return command_class.from_args(args)
