from .redis_command import RedisCommand

from .discard import DiscardCommand
from .echo import EchoCommand
from .exec import ExecCommand
from .get import GetCommand
from .incr import IncrCommand
from .multi import MultiCommand
from .ping import PingCommand
from .set import SetCommand
from .type import TypeCommand
from .xadd import XaddCommand
from .xrange import XrangeCommand
from .xread import XreadCommand


def argv_to_command(argv: list[str]) -> RedisCommand:
    match command_name := argv[0].upper():
        case "DISCARD":
            return DiscardCommand(argv)
        case "ECHO":
            return EchoCommand(argv)
        case "EXEC":
            return ExecCommand(argv)
        case "GET":
            return GetCommand(argv)
        case "INCR":
            return IncrCommand(argv)
        case "MULTI":
            return MultiCommand(argv)
        case "PING":
            return PingCommand(argv)
        case "SET":
            return SetCommand(argv)
        case "TYPE":
            return TypeCommand(argv)
        case "XADD":
            return XaddCommand(argv)
        case "XRANGE":
            return XrangeCommand(argv)
        case "XREAD":
            return XreadCommand(argv)

    raise ValueError(f"Unknown command: {command_name}")
