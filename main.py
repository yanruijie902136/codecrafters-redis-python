import asyncio

from redis import RedisServer


def main() -> None:
    server = RedisServer()
    asyncio.run(server.start())


if __name__ == "__main__":
    main()
