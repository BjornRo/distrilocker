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
import sys
import msgspec

type ExpiryAsciiInt = str
type Address = tuple[str, int | str]
type ReturnResult = tuple[bool, bytes]
now_time = lambda: int(time.time())


DEFAULT_UNIX_SOCK_ADDRESS = "/dev/shm/dlserver.sock"

response_protocol = Struct(">?H")  # ?: Ok/Err, H: Header_len for data following


async def main():
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


@dataclass(slots=True)
class _StoreItem:
    expiry: int
    data: bytes


class Request(msgspec.Struct):
    index: int
    method: Methods
    key: str
    expiry: int
    header_len: int

    def __post_init__(self):
        self.key = self.key.lower()


request_decoder = msgspec.msgpack.Decoder(Request)


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
                            if now_time() > item.expiry:
                                del self.store[key]

        self._background_task = asyncio.create_task(periodic_clean(self.interval))

    def size(self) -> ReturnResult:
        return True, str(len(self.store)).encode()

    async def get(self, request: Request) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                if now_time() <= self.store[request.key].expiry:
                    return True, self.store[request.key].data
                del self.store[request.key]
        return False, b""

    async def set(self, request: Request, data: bytes) -> ReturnResult:
        if request.expiry != 0 and data:
            async with self.lock:
                if request.key not in self.store:
                    self.store[request.key] = _StoreItem(expiry=now_time() + request.expiry, data=data)
                    return True, b""
        return False, b""

    async def update(self, request: Request, data: bytes) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                if request.expiry:
                    self.store[request.key].expiry = request.expiry
                if data:
                    self.store[request.key].data = data
                return True, b""
        return False, b""

    async def delete(self, request: Request) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                del self.store[request.key]
                return True, b""
        return False, b""


class Methods(IntEnum):
    SIZE = 0
    GET = 1
    SET = 2
    UPDATE = 3
    DELETE = 4


class ProtocolStrategyBase(ABC):
    def __init__(self, num_locks: int, periodic_cleaning_interval_sec: int = 14400):
        if num_locks <= 0:
            raise ValueError("More locks than 0")
        self._store = tuple(_LockStore(periodic_cleaning_interval_sec) for _ in range(num_locks))

    @abstractmethod
    async def run(self) -> None: ...
    async def _gen_response(self, request: Request, data: bytes) -> ReturnResult:
        store = self._store[request.index]
        match request.method:
            case Methods.SIZE:
                return store.size()
            case Methods.GET:
                return await store.get(request)
            case Methods.SET:
                return await store.set(request, data)
            case Methods.UPDATE:
                return await store.update(request, data)
            case Methods.DELETE:
                return await store.delete(request)
            case _:
                return False, b"10101"

    async def _handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            header_len = (await reader.readexactly(1))[0]
            request = request_decoder.decode(await reader.readexactly(header_len))
            data = await reader.readexactly(request.header_len)
            result, data = await self._gen_response(request=request, data=data)

            writer.write(response_protocol.pack(result, len(data)))
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
                    transport.sendto(response_protocol.pack(result, len(data)) + data, addr)
                except asyncio.CancelledError:
                    raise
                except:
                    pass

        class EchoServerProtocol(asyncio.DatagramProtocol):
            def connection_made(_self, transport):
                _self.transport = transport

            def datagram_received(_self, data: bytes, addr: Address):
                request = request_decoder.decode(data[1 : data[0] + 1])
                data = data[data[0] + 1 :]
                self._queue.put_nowait((addr, request, data))

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
