from typing import Dict, List, Type, TypeAlias, Union

from .commands import *


_Lookup: TypeAlias = Dict[str, Type[RedisCommand]]
_NestedLookup: TypeAlias = Dict[str, Union[Type[RedisCommand], _Lookup]]


_COMMAND_CLASSES: _NestedLookup = {
    'BLPOP': BlpopCommand,
    'CONFIG': {
        'GET': ConfigGetCommand,
    },
    'DISCARD': DiscardCommand,
    'ECHO': EchoCommand,
    'EXEC': ExecCommand,
    'GET': GetCommand,
    'INCR': IncrCommand,
    'INFO': InfoCommand,
    'KEYS': KeysCommand,
    'LLEN': LlenCommand,
    'LPOP': LpopCommand,
    'LPUSH': LpushCommand,
    'LRANGE': LrangeCommand,
    'MULTI': MultiCommand,
    'PING': PingCommand,
    'PSYNC': PsyncCommand,
    'REPLCONF': ReplconfCommand,
    'RPUSH': RpushCommand,
    'SET': SetCommand,
    'TYPE': TypeCommand,
    'XADD': XaddCommand,
    'XRANGE': XrangeCommand,
    'XREAD': XreadCommand,
    'ZADD': ZaddCommand,
    'ZCARD': ZcardCommand,
    'ZRANGE': ZrangeCommand,
    'ZRANK': ZrankCommand,
    'ZREM': ZremCommand,
    'ZSCORE': ZscoreCommand,
}


def parse_args_to_command(args: List[bytes]) -> RedisCommand:
    lookup = _COMMAND_CLASSES
    command_name, args = args[0].decode(), args[1:]
    res = lookup.get(command_name.upper())
    if not res:
        raise RuntimeError(f'Unknown command: {command_name}')
    if not isinstance(res, dict):
        return res.from_args(args)

    lookup = res
    subcommand_name, args = args[0].decode(), args[1:]
    res = lookup.get(subcommand_name.upper())
    if not res:
        raise RuntimeError(f'Unknown subcommand: {subcommand_name}')
    return res.from_args(args)
