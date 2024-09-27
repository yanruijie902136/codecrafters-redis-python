import argparse
import asyncio

from redis import RedisServer


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, default=None)
    parser.add_argument("--dbfilename", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    server = RedisServer(dir=args.dir, dbfilename=args.dbfilename)
    asyncio.run(server.start())


if __name__ == "__main__":
    main()
