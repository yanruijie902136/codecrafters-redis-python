import argparse
import asyncio

from .server import RedisServer, RedisServerConfig


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument('--dbfilename', type=str, default='dump.rdb')
    parser.add_argument('--dir', type=str, default='./')
    parser.add_argument('--port', type=int, default=6379)
    parser.add_argument('--replicaof', type=str, default=None)

    return parser.parse_args()


def main() -> None:
    args = parse_command_line_args()

    if args.replicaof is not None:
        master_host_str, master_port_str = args.replicaof.split(' ')
        master_addr = master_host_str, int(master_port_str)
    else:
        master_addr = None

    config = RedisServerConfig(dbfilename=args.dbfilename, dir=args.dir)
    server = RedisServer(args.port, config, master_addr=master_addr)
    asyncio.run(server.run())


if __name__ == '__main__':
    main()
