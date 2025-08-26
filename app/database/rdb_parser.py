import enum
import os
from types import TracebackType
from typing import List, Optional, Self, Type

from ..data_structs import RedisDataStruct, RedisString

from .database import RedisDatabase
from .expiry import Expiry


def rdb_parse(path: str) -> List[RedisDatabase]:
    with _RdbParser(path) as parser:
        return parser.parse()


class _RdbParser:
    def __init__(self, path: str) -> None:
        self._path = path

    def parse(self) -> List[RedisDatabase]:
        self._header()
        while not self._at_eof:
            self._part()
        return self._databases

    def _header(self) -> None:
        assert self._readexactly(9) == b'REDIS0011'

    def _part(self) -> None:
        opcode = ord(self._readexactly(1))
        match opcode:
            case _OpCode.AUX:
                self._aux()
            case _OpCode.RESIZEDB:
                self._resizedb()
            case _OpCode.EXPIRETIMEMS:
                self._expiretimems()
            case _OpCode.EXPIRETIME:
                self._expiretime()
            case _OpCode.SELECTDB:
                self._selectdb()
            case _OpCode.EOF:
                self._eof()
            case _:
                self._file.seek(-1, os.SEEK_CUR)
                self._kvpair()

    def _aux(self) -> None:
        self._read_string()
        self._read_string()

    def _resizedb(self) -> None:
        self._read_length()
        self._read_length()

    def _expiretimems(self) -> None:
        pxat = int.from_bytes(self._readexactly(8), 'little')
        self._kvpair(Expiry(pxat=pxat))

    def _expiretime(self) -> None:
        exat = int.from_bytes(self._readexactly(4), 'little')
        self._kvpair(Expiry(exat=exat))

    def _selectdb(self) -> None:
        self._db_index = self._read_length()

    def _eof(self) -> None:
        self._at_eof = True

    def _kvpair(self, expiry: Optional[Expiry] = None) -> None:
        value_type = ord(self._readexactly(1))
        key = self._read_string().to_bytes()
        value = self._read_value(value_type)
        if expiry is None or not expiry.has_passed():
            self._databases[self._db_index].set(key, value, expiry)

    def _read_length(self) -> int:
        msbs, lsbs = divmod(ord(self._readexactly(1)), 64)
        match msbs:
            case 0b00:
                return lsbs
            case 0b01:
                return (lsbs << 8) | ord(self._readexactly(1))
            case 0b10:
                return int.from_bytes(self._readexactly(4), 'big')

        raise RuntimeError('Expected length but got string instead')

    def _read_value(self, value_type: int) -> RedisDataStruct:
        match value_type:
            case _ValueType.STRING:
                return self._read_string()

        raise RuntimeError(f'Unexpected value type: {value_type}')

    def _read_string(self) -> RedisString:
        msbs, lsbs = divmod(ord(self._readexactly(1)), 64)

        if msbs == 0b11:
            match lsbs:
                case 0:
                    n = int.from_bytes(self._readexactly(1), 'little')
                case 1:
                    n = int.from_bytes(self._readexactly(2), 'little')
                case 2:
                    n = int.from_bytes(self._readexactly(4), 'little')
                case 3:
                    raise RuntimeError('Unexpected LZF-compressed string')
                case _:
                    raise RuntimeError(f'Unknown encoding format: {lsbs:06b}')
            return RedisString(str(n).encode())

        if msbs == 0b00:
            length = lsbs
        elif msbs == 0b01:
            length = (lsbs << 8) | ord(self._readexactly(1))
        else:
            length = int.from_bytes(self._readexactly(4), 'big')

        return RedisString(self._readexactly(length))

    def _readexactly(self, n: int) -> bytes:
        data = self._file.read(n)
        if len(data) != n:
            raise EOFError
        return data

    def __enter__(self) -> Self:
        self._file = open(self._path, 'rb')
        self._at_eof = False
        self._databases = [RedisDatabase() for _ in range(16)]
        self._db_index = 0
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        self._file.close()


@enum.unique
class _OpCode(enum.IntEnum):
    AUX = 0xFA
    RESIZEDB = 0xFB
    EXPIRETIMEMS = 0xFC
    EXPIRETIME = 0xFD
    SELECTDB = 0xFE
    EOF = 0xFF


@enum.unique
class _ValueType(enum.IntEnum):
    STRING = 0
