import asyncio

from .args_parser import parse_args_to_command
from .connection import RedisConnection
from .database import RedisDatabase


class RedisServer:
    def __init__(self) -> None:
        self._databases = [RedisDatabase() for _ in range(16)]

    def get_database(self, db_index: int) -> RedisDatabase:
        return self._databases[db_index]

    async def run(self) -> None:
        server = await asyncio.start_server(self._client_connected_cb, 'localhost', 6379, reuse_port=True)
        async with server:
            await server.serve_forever()

    async def _client_connected_cb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = RedisConnection(reader, writer, server=self)
        print(f'Accepted connection from {conn.addr}')

        async with conn:
            while True:
                args = await conn.read_args()
                command = parse_args_to_command(args)
                print(f'Received command from {conn.addr}: {command!r}')

                response = await command.execute(conn)
                await conn.write_response(response)
