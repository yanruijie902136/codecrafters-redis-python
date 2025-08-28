import argparse
import asyncio

from .server import RedisServer, RedisServerConfig


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument('--dbfilename', type=str, default='dump.rdb')
    parser.add_argument('--dir', type=str, default='./')
    parser.add_argument('--port', type=int, default=6379)

    return parser.parse_args()


def main() -> None:
    args = parse_command_line_args()
    config = RedisServerConfig(dbfilename=args.dbfilename, dir=args.dir)
    server = RedisServer(args.port, config)
    asyncio.run(server.run())


if __name__ == '__main__':
    main()
