import asyncio
import collections
from typing import Optional

from .database import RedisDatabase
from .resp2 import (
    RespArray,
    RespBulkString,
    RespInteger,
    RespSerializable,
    RespSimpleError,
    RespSimpleString,
)

Argv = list[str]
Transaction = list[Argv]


class RedisClient:
    def __init__(self) -> None:
        self.transaction: Optional[Transaction] = None


class RedisServer:
    def __init__(self) -> None:
        self._database = RedisDatabase()

    async def start(self) -> None:
        server = await asyncio.start_server(self._handle_client, "localhost", 6379, reuse_port=True)
        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client = RedisClient()

        while True:
            if (argv := await self._recv_argv(reader)) is None:
                break

            response = self._exec_argv(argv, client)
            writer.write(response.serialize())
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def _recv_argv(self, reader: asyncio.StreamReader) -> Optional[Argv]:
        try:
            argc = int((await reader.readuntil(b"\r\n"))[1:-2].decode())
            argv = []
            for _ in range(argc):
                argsize = int((await reader.readuntil(b"\r\n"))[1:-2].decode())
                argv.append((await reader.read(argsize+2))[:-2].decode())
            return argv
        except asyncio.IncompleteReadError:
            return None

    def _exec_argv(self, argv: Argv, client: RedisClient) -> RespSerializable:
        command_name = argv[0].upper()
        if command_name != "EXEC" and client.transaction is not None:
            client.transaction.append(argv)
            return RespSimpleString("QUEUED")

        match command_name:
            case "ECHO":
                return RespBulkString(argv[1])
            case "EXEC":
                if client.transaction is None:
                    return RespSimpleError("ERR EXEC without MULTI")
                responses = [self._exec_argv(argv2, client) for argv2 in client.transaction]
                client.transaction = None
                return RespArray(responses)
            case "GET":
                return RespBulkString(self._database.get(argv[1]))
            case "INCR":
                if (incremented_value := self._database.increment(argv[1])) is None:
                    return RespSimpleError("ERR value is not an integer or out of range")
                return RespInteger(incremented_value)
            case "MULTI":
                client.transaction = Transaction()
                return RespSimpleString("OK")
            case "PING":
                return RespSimpleString("PONG")
            case "SET":
                if len(argv) == 3:
                    self._database.set(argv[1], argv[2])
                else:
                    self._database.set(argv[1], argv[2], expire_time=float(argv[-1]))
                return RespSimpleString("OK")
            case _:
                raise ValueError(f"Unknown command: {command_name}")
