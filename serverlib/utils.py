from abc import ABC, abstractmethod
import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from struct import Struct
from shared import Request, RequestMethods, ReturnResult
import time

import msgspec

type Address = tuple[str, int | str]


def now_time():
    return int(time.time())


@dataclass(slots=True)
class StoreExpiryItem:
    expiry: int | None
    data: bytes


@dataclass(slots=True)
class StoreItem:
    task: asyncio.Task | None
    data: bytes


class StoreBase(ABC):
    def __init__(self):
        self.store: dict[str, StoreItem] = {}

    def size(self) -> ReturnResult:
        return True, str(len(self.store)).encode()

    async def init(self):
        pass

    @abstractmethod
    async def _task_timer(self, key: str, expiry: int): ...
    @abstractmethod
    async def get(self, request: Request) -> ReturnResult: ...
    @abstractmethod
    async def set(self, request: Request, data: bytes | None) -> ReturnResult: ...
    @abstractmethod
    async def delete(self, request: Request) -> ReturnResult: ...
    @abstractmethod
    async def update(self, request: Request, data: bytes | None) -> ReturnResult: ...


class ProtocolStrategyBase(ABC):
    request_decoder = msgspec.msgpack.Decoder(Request)
    response_protocol = Struct(">?H")  # ?: Ok/Err, H: Header_len for data following

    def __init__(self, num_stores: int, store_type: Callable[[], StoreBase]):
        if num_stores <= 0:
            raise ValueError("More locks than 0")
        self._store: tuple[StoreBase, ...] = tuple(store_type() for _ in range(num_stores))

    @abstractmethod
    async def run(self) -> None: ...
    async def _gen_response(self, request: Request, data: bytes | None) -> ReturnResult:
        store = self._store[request.index]
        match request.method:
            case RequestMethods.SIZE:
                return store.size()
            case RequestMethods.GET:
                return await store.get(request)
            case RequestMethods.SET:
                return await store.set(request, data)
            case RequestMethods.UPDATE:
                return await store.update(request, data)
            case RequestMethods.DELETE:
                return await store.delete(request)
            case _:
                return False, b"10101"


class UnixTCPHandler(ProtocolStrategyBase):
    async def handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            header_len = (await reader.readexactly(1))[0]
            request = self.request_decoder.decode(await reader.readexactly(header_len))
            match request.header_len:
                case None:
                    data = None
                case 0:
                    data = b""
                case value:
                    data = await reader.readexactly(value)

            result, data = await self._gen_response(request=request, data=data)

            writer.write(self.response_protocol.pack(result, len(data)))
            if data:
                writer.write(data)
            await writer.drain()
        except:
            pass
        writer.write_eof()
        writer.close()
        await writer.wait_closed()
