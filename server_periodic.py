from __future__ import annotations
from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
import time
from pathlib import Path
import os
import random
from struct import Struct
from enum import IntEnum

import msgspec

type ExpiryAsciiInt = str
type Address = tuple[str, int | str]
type ReturnResult = tuple[bool, bytes]

DEFAULT_UNIX_SOCK_ADDRESS = "/dev/shm/dlserver.sock"


async def main():
    import sys

    try:
        import uvloop  # type:ignore

        asyncio.set_event_loop(uvloop.new_event_loop())
    except:
        pass

    """Examples:
    tcp://0.0.0.0:8888, unix://not/home/but/here"""
    uri = sys.argv[1].lower()
    num_stores = int(sys.argv[2]) if len(sys.argv) == 3 else 4

    protocol, addr_path = uri.split("://")
    match protocol:
        case "tcp":
            server = ProtocolTCP(num_stores, addr_path, 1800)
        case "udp":
            server = ProtocolUDP(num_stores, addr_path, 1800)
        case "unix":
            server = ProtocolUNIX(num_stores, addr_path, 1800)
    await server.run()


class RequestMethods(IntEnum):
    SIZE = 0
    GET = 1
    SET = 2
    UPDATE = 3
    DELETE = 4


class Request(msgspec.Struct, array_like=True):
    index: int
    method: RequestMethods
    key: str
    expiry: int | None
    header_len: int | None

    def __post_init__(self):
        self.key = self.key.lower()


def _now_time():
    return int(time.time())


@dataclass(slots=True)
class _StoreItem:
    expiry: int | None
    data: bytes


class _LockStore:
    def __init__(self, periodic_cleaning_interval_sec: int):
        self.store: dict[str, _StoreItem] = {}
        self.lock = asyncio.Lock()
        self.interval = periodic_cleaning_interval_sec

    async def init(self):
        async def periodic_clean(interval_time: int):
            while True:
                await asyncio.sleep(interval_time)
                async with self.lock:
                    if store_len := len(self.store):
                        for key, item in random.sample(tuple(self.store.items()), k=min(4, store_len)):
                            if item.expiry and _now_time() > item.expiry:
                                del self.store[key]

        self._background_task = asyncio.create_task(periodic_clean(self.interval))

    def size(self) -> ReturnResult:
        return True, str(len(self.store)).encode()

    async def get(self, request: Request) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                if (exp := self.store[request.key].expiry) is None or _now_time() <= exp:
                    return True, self.store[request.key].data
                del self.store[request.key]
        return False, b""

    async def set(self, request: Request, data: bytes | None) -> ReturnResult:
        if request.expiry != 0:
            async with self.lock:
                if request.key not in self.store:
                    exp = None if request.expiry is None else _now_time() + request.expiry
                    self.store[request.key] = _StoreItem(expiry=exp, data=data or b"")
                    return True, b""
        return False, b""

    async def update(self, request: Request, data: bytes | None) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                self.store[request.key].expiry = None if request.expiry is None else _now_time() + request.expiry
                if data is not None:
                    self.store[request.key].data = data
                return True, b""
        return False, b""

    async def delete(self, request: Request) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                del self.store[request.key]
                return True, b""
        return False, b""


class ProtocolStrategyBase(ABC):
    request_decoder = msgspec.msgpack.Decoder(Request)
    response_protocol = Struct(">?H")  # ?: Ok/Err, H: Header_len for data following

    def __init__(self, num_locks: int, periodic_cleaning_interval_sec: int = 14400):
        if num_locks <= 0:
            raise ValueError("More locks than 0")
        self._store = tuple(_LockStore(periodic_cleaning_interval_sec) for _ in range(num_locks))

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

    async def _handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
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


class ProtocolTCP(ProtocolStrategyBase):
    def __init__(self, num_locks: int, address_port: str, periodic_cleaning_interval_sec: int = 14400):
        """Ex: address_port = '0.0.0.0:1337'"""
        super().__init__(num_locks, periodic_cleaning_interval_sec)
        self.address, self.port = address_port.lower().split(":")

    async def run(self):
        server = await asyncio.start_server(self._handler, host=self.address, port=self.port)
        async with server:
            for i in self._store:
                await i.init()
            await server.serve_forever()


class ProtocolUDP(ProtocolStrategyBase):
    def __init__(self, num_locks: int, address_port: str, periodic_cleaning_interval_sec: int = 14400):
        """Ex: address_port = '0.0.0.0:1337'"""
        super().__init__(num_locks, periodic_cleaning_interval_sec)
        self.address, port = address_port.lower().split(":")
        self.port = int(port)
        self._queue: asyncio.Queue[tuple[Address, Request, bytes]] = asyncio.Queue()

    async def run(self):
        async def message_handler_task():
            while True:
                try:
                    addr, request, data = await self._queue.get()
                    result, data = await self._gen_response(request=request, data=data)
                    transport.sendto((b"\x01" if result else b"\x00") + data, addr)
                except asyncio.CancelledError:
                    raise
                except:
                    pass

        class EchoServerProtocol(asyncio.DatagramProtocol):
            def connection_made(_self, transport):
                _self.transport = transport

            def datagram_received(_self, data: bytes, addr: Address):
                request = self.request_decoder.decode(data[1 : data[0] + 1])
                self._queue.put_nowait((addr, request, data[data[0] + 1 :]))

        transport, protocol = await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: EchoServerProtocol(), local_addr=(self.address, self.port)
        )
        self._background_task = asyncio.create_task(message_handler_task())
        try:
            await asyncio.Future()
        except:
            pass
        self._background_task.cancel()
        transport.close()


class ProtocolUNIX(ProtocolStrategyBase):
    def __init__(
        self,
        num_locks: int,
        filepath: str,
        periodic_cleaning_interval_sec: int = 14400,
    ):
        """path/to/me or just_me"""
        super().__init__(num_locks, periodic_cleaning_interval_sec)
        if not filepath:
            filepath = DEFAULT_UNIX_SOCK_ADDRESS
        elif filepath[0] == ".":
            raise ValueError("Invalid path")
        self.filepath = Path(filepath)

    async def run(self):
        if os.path.exists(self.filepath):
            os.remove(self.filepath)
        else:
            os.makedirs(self.filepath.parent, exist_ok=True)
        try:
            server = await asyncio.start_unix_server(self._handler, path=self.filepath)
            async with server:
                for i in self._store:
                    await i.init()
                await server.serve_forever()
        except:
            pass
        try:
            os.remove(self.filepath)
        except:
            pass


if __name__ == "__main__":
    asyncio.run(main())
