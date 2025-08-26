import asyncio

from .server import RedisServer


def main() -> None:
    server = RedisServer()
    asyncio.run(server.run())


if __name__ == '__main__':
    main()
