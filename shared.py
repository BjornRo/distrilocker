from enum import IntEnum
from struct import Struct
import msgspec

type ReturnResult = tuple[bool, bytes]

DEFAULT_UNIX_SOCK_ADDRESS = "/dev/shm/dlserver.sock"

response_protocol = Struct(">Q?H")  # Q: int_id, ?: Ok/Err, H: Header_len for data following


class RequestMethods(IntEnum):
    SIZE = 0
    GET = 1
    SET = 2
    DELETE = 3
    UPDATE = 4


class Request(msgspec.Struct, array_like=True):
    index: int
    method: RequestMethods
    key: str
    expiry: int | None
    header_len: int | None

    def __post_init__(self):
        self.key = self.key.lower()
