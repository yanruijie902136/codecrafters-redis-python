import abc
from typing import Optional


class RespSerializable(abc.ABC):
    @abc.abstractmethod
    def serialize(self) -> bytes:
        raise NotImplementedError


class RespArray(RespSerializable):
    def __init__(self, elements: list[RespSerializable]) -> None:
        self.elements = elements

    def serialize(self) -> bytes:
        return f"*{len(self.elements)}\r\n".encode() + b"".join(
            e.serialize() for e in self.elements
        )


class RespBulkString(RespSerializable):
    def __init__(self, string: Optional[str]) -> None:
        self.string = string

    def serialize(self) -> bytes:
        if self.string is None:
            return "$-1\r\n".encode()
        return f"${len(self.string)}\r\n{self.string}\r\n".encode()


class RespInteger(RespSerializable):
    def __init__(self, integer: int) -> None:
        self.integer = integer

    def serialize(self) -> bytes:
        return f":{self.integer}\r\n".encode()


class RespSimpleError(RespSerializable):
    def __init__(self, error_message: str) -> None:
        self.error_message = error_message

    def serialize(self) -> bytes:
        return f"-{self.error_message}\r\n".encode()


class RespSimpleString(RespSerializable):
    def __init__(self, string: str) -> None:
        self.string = string

    def serialize(self) -> bytes:
        return f"+{self.string}\r\n".encode()
