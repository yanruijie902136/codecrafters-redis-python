#!/usr/bin/env python3

import asyncio
from typing import Optional

from .database import RedisDatabase
from .resp2 import RespBulkString, RespInteger, RespSimpleError, RespSimpleString


async def recv_argv(reader: asyncio.StreamReader) -> Optional[list[str]]:
    try:
        argc = int((await reader.readuntil(b"\r\n"))[1:-2].decode())
        argv = []
        for _ in range(argc):
            argsize = int((await reader.readuntil(b"\r\n"))[1:-2].decode())
            argv.append((await reader.read(argsize+2))[:-2].decode())
        return argv
    except asyncio.IncompleteReadError:
        return None


database = RedisDatabase()


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        argv = await recv_argv(reader)
        if argv is None:
            break
        print(argv)

        match command_name := argv[0].upper():
            case "ECHO":
                response = RespBulkString(argv[1])
            case "GET":
                response = RespBulkString(database.get(argv[1]))
            case "INCR":
                if (incremented_value := database.increment(argv[1])) is None:
                    response = RespSimpleError("ERR value is not an integer or out of range")
                else:
                    response = RespInteger(incremented_value)
            case "MULTI":
                response = RespSimpleString("OK")
            case "PING":
                response = RespSimpleString("PONG")
            case "SET":
                if len(argv) == 3:
                    database.set(argv[1], argv[2])
                else:
                    database.set(argv[1], argv[2], expire_time=float(argv[-1]))
                response = RespSimpleString("OK")
            case _:
                raise ValueError(f"Unknown command: {command_name}")

        writer.write(response.serialize())
        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main() -> None:
    server = await asyncio.start_server(handle_client, "localhost", 6379, reuse_port=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
