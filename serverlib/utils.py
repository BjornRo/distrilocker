from abc import ABC, abstractmethod
import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from shared import Request, RequestMethods, ReturnResult, response_protocol
from itertools import islice

import msgspec

type Address = tuple[str, int | str]


@dataclass(slots=True)
class StoreLockItem:
    task: asyncio.Task | None
    data: bytes
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(slots=True)
class StoreExpiryItem:
    expiry: int | None
    data: bytes


@dataclass(slots=True)
class StoreItem:
    task: asyncio.Task | None
    data: bytes


class StoreBase[T](ABC):
    __slots__ = "store"
    encoder = msgspec.msgpack.Encoder().encode

    def __init__(self):
        self.store: dict[str, T] = {}

    def size(self) -> ReturnResult:
        return True, str(len(self.store)).encode()

    async def keys(self, request: Request) -> ReturnResult:
        """request.key = 'start..end' start/end: int, exclusive end. start or end can be empty"""
        res = request.key.split("..")
        while True:
            try:  # Iterator might throw if something changed during iteration.
                match res:
                    case "", "":
                        return False, b""
                    case start, "":
                        _iter = islice(self.store, int(start), int(start) + 100)
                    case "", end:
                        _iter = islice(self.store, int(end))
                    case start_end:
                        start, end = map(int, start_end)
                        if start >= end:
                            return False, b""
                        _iter = islice(self.store, int(start), int(end))
                return True, self.encoder(tuple(_iter))
            except:
                pass

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
    __slots__ = "store"
    request_decoder = msgspec.msgpack.Decoder(Request).decode

    def __init__(self, num_stores: int, store_type: Callable[[], StoreBase]):
        if num_stores <= 0:
            raise ValueError("More locks than 0")
        self.store: tuple[StoreBase, ...] = tuple(store_type() for _ in range(num_stores))

    @abstractmethod
    async def run(self) -> None: ...
    async def _gen_response(self, request: Request, data: bytes | None) -> ReturnResult:
        store = self.store[request.index]
        match request.method:
            case RequestMethods.SIZE:
                return store.size()
            case RequestMethods.KEYS:
                return await store.keys(request)
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
    __slots__ = ()

    async def handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        while True:
            try:
                request = await reader.readexactly(9)
                request_id, header_len = request[:8], request[-1]
            except:
                writer.close()
                await writer.wait_closed()
                return
            request = self.request_decoder(await reader.readexactly(header_len))
            match request.data_len:
                case None:
                    data = None
                case 0:
                    data = b""
                case value:
                    data = await reader.readexactly(value)
            result, data = await self._gen_response(request=request, data=data)
            writer.write(response_protocol.pack(request_id, result, len(data)))
            if data:
                writer.write(data)
            await writer.drain()
