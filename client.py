from __future__ import annotations

import asyncio
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
import secrets
from typing import cast, override

import msgspec

from shared import (
    DEFAULT_UNIX_SOCK_ADDRESS,
    Request,
    RequestMethods,
    ReturnResult,
    response_protocol,
)


async def main():
    import sys
    import time

    protocol, *addr_path = sys.argv[1].lower().split("://")
    store_id = int(sys.argv[2])

    if protocol == "unix":
        client = ClientUnix(store_id, addr_path[0] if addr_path else "")
    elif protocol == "tcp":
        client = ClientTCP(store_id, addr_port=addr_path[0])
    elif protocol == "udp":
        client = ClientUDP(store_id, addr_port=addr_path[0])
    else:
        raise ValueError("Invalid protocol")

    if protocol != "udp":
        await client.init()
    print(await client.size())
    start = time.time()
    await asyncio.gather(client.set("boDod", 10, b""), client.set("boDod", 10, b""), client.set("boDod", 10, b""))
    # print(await client.set("boDod", 10, b""))
    # print(await client.set("boDod", 10, b""))
    # print(await client.set("boDod", 10, b""))
    print("Elapsed:", time.time() - start)
    print(await client.size())
    if protocol != "udp":
        await client.close()


class ClientBase(ABC):
    encoder = msgspec.msgpack.Encoder().encode

    def __init__(self, store_id: int):
        self.store_id = store_id

    async def init(self):
        pass

    async def close(self):
        pass

    @abstractmethod
    async def _call(self, request: Request, data: bytes | None = None) -> ReturnResult: ...

    async def size(self):
        req = Request(index=self.store_id, method=RequestMethods.SIZE, key="", expiry=None, data_len=None)
        return await self._call(request=req)

    async def keys(self, start_dotdot_end: str = "0..10"):  # 4.. , ..3=0..3
        """start..end"""
        req = Request(
            index=self.store_id,
            method=RequestMethods.KEYS,
            key=start_dotdot_end,
            expiry=None,
            data_len=None,
        )
        return await self._call(request=req)

    async def get(self, key: str):
        req = Request(index=self.store_id, method=RequestMethods.GET, key=key, expiry=None, data_len=None)
        return await self._call(request=req)

    async def set(self, key: str, expiry: int, data: bytes | None):
        req = Request(
            index=self.store_id,
            method=RequestMethods.SET,
            key=key,
            expiry=expiry,
            data_len=len(data) if data else None,
        )
        return await self._call(request=req, data=data)

    async def update(self, key: str, expiry: int, data: bytes | None):
        """data:None does not update value, expiry"""
        hl = None if data is None else len(data)
        req = Request(index=self.store_id, method=RequestMethods.UPDATE, key=key, expiry=expiry, data_len=hl)
        return await self._call(request=req, data=data)

    async def delete(self, key: str):
        req = Request(index=self.store_id, method=RequestMethods.DELETE, key=key, expiry=None, data_len=None)
        return await self._call(request=req)


@dataclass
class CallbackItem:
    request: Request
    data: bytes | None
    channel: asyncio.Queue[ReturnResult] = field(default_factory=lambda: asyncio.Queue(1))


class ClientUnixTCPBase(ClientBase):
    def __init__(self, store_id: int):
        super().__init__(store_id)
        self.callback_queue: asyncio.Queue[CallbackItem] = asyncio.Queue()

    @override
    async def init(self):
        self.reader, self.writer = await self._connect()
        self.background_task = asyncio.create_task(self.connection_multiplexer())

    @override
    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

    async def connection_multiplexer(self):
        waiting_callback: dict[bytes, asyncio.Queue[ReturnResult]] = {}

        async def reader():
            while True:
                resp = await self.reader.readexactly(11)
                request_id = resp[:8]
                ok, header_len = cast(tuple[bool, int], response_protocol.unpack(resp[8:]))
                data = await self.reader.readexactly(header_len) if header_len else b""
                await waiting_callback[request_id].put((ok, data))
                del waiting_callback[request_id]  # Strong-ref @ _call(..) -> ok to delete.

        reader_task = asyncio.create_task(reader())
        while True:
            item = await self.callback_queue.get()
            headers = self.encoder(item.request)
            request_id = secrets.token_bytes(8)
            self.writer.write(request_id + len(headers).to_bytes())
            self.writer.write(headers)
            if item.data:
                self.writer.write(item.data)
            await self.writer.drain()
            waiting_callback[request_id] = item.channel

    @abstractmethod
    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]: ...

    async def _call(self, request: Request, data: bytes | None = None) -> ReturnResult:
        cb = CallbackItem(request=request, data=data)
        await self.callback_queue.put(cb)
        return await cb.channel.get()


class ClientUnix(ClientUnixTCPBase):
    def __init__(self, store_id: int, path: str):
        super().__init__(store_id)
        self.filepath = Path(DEFAULT_UNIX_SOCK_ADDRESS) if path == "" else Path(path)

        if not os.path.exists(self.filepath):
            raise RuntimeError("DL server is not running")

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_unix_connection(self.filepath)


class ClientTCP(ClientUnixTCPBase):
    def __init__(self, store_id: int, addr_port: str):
        super().__init__(store_id)
        self.addr, self.port = addr_port.split(":")

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection(host=self.addr, port=self.port)


class ClientUDP(ClientBase):
    def __init__(self, store_id: int, addr_port: str):
        super().__init__(store_id)
        self.addr, port = addr_port.split(":")
        self.port = int(port)
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()

        class EchoClientProtocol(asyncio.DatagramProtocol):
            def __init__(self, message: bytes, return_val: list[ReturnResult], on_con_lost):
                self.message = message
                self.on_con_lost = on_con_lost
                self.transport = None
                self.return_val = return_val

            def connection_made(self, transport):
                self.transport = transport
                self.transport.sendto(self.message)

            def datagram_received(_self, data: bytes, addr):
                _self.return_val.append((bool(data[0]), data[1:]))
                _self.transport.close()  # type:ignore

            def error_received(self, exc): ...

            def connection_lost(self, exc):
                self.on_con_lost.set_result(True)

        self.client_prot = EchoClientProtocol

    async def _call(self, request: Request, data: bytes | None = None) -> ReturnResult:
        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()
        headers = self.encoder(request)
        message = len(headers).to_bytes(1) + headers
        if data:
            message += data
        return_val: list[ReturnResult] = []
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: self.client_prot(
                message=message,
                return_val=return_val,
                on_con_lost=on_con_lost,
            ),
            remote_addr=(self.addr, self.port),
        )
        try:
            await on_con_lost
        finally:
            transport.close()
        return return_val[0]


if __name__ == "__main__":
    asyncio.run(main())
