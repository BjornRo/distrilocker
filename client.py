from __future__ import annotations
from abc import ABC, abstractmethod
import asyncio
from struct import Struct
from pathlib import Path
import os

from shared import Request, RequestMethods, ReturnResult, DEFAULT_UNIX_SOCK_ADDRESS
import msgspec


async def main():
    import time
    import sys

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

    print(await client.size())
    start = time.time()
    print(await client.set("boDod", 10, b""))
    print("Elapsed:", time.time() - start)
    print(await client.size())


class ClientBase(ABC):
    def __init__(self, store_id: int):
        self.store_id = store_id

    @abstractmethod
    async def _call(self, request: Request, data: bytes | None = None): ...

    async def size(self):
        req = Request(index=self.store_id, method=RequestMethods.SIZE, key="", expiry=0, header_len=None)
        return await self._call(request=req)

    async def get(self, key: str):
        req = Request(index=self.store_id, method=RequestMethods.GET, key=key, expiry=0, header_len=None)
        return await self._call(request=req)

    async def set(self, key: str, expiry: int, data: bytes):
        req = Request(index=self.store_id, method=RequestMethods.SET, key=key, expiry=expiry, header_len=len(data))
        return await self._call(request=req, data=data)

    async def update(self, key: str, expiry: int, data: bytes | None):
        """data:None does not update value, expiry"""
        hl = None if data is None else len(data)
        req = Request(index=self.store_id, method=RequestMethods.UPDATE, key=key, expiry=expiry, header_len=hl)
        return await self._call(request=req, data=data)

    async def delete(self, key: str):
        req = Request(index=self.store_id, method=RequestMethods.DELETE, key=key, expiry=None, header_len=None)
        return await self._call(request=req)


class ClientUnixTCPBase(ClientBase):
    response_protocol = Struct(">?H")  # ?: Ok/Err, H: Header_len for data following

    def __init__(self, store_id: int):
        super().__init__(store_id)

    @abstractmethod
    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]: ...

    async def _call(self, request: Request, data: bytes | None = None) -> ReturnResult:
        reader, writer = await self._connect()
        headers = msgspec.msgpack.encode(request)
        writer.write(len(headers).to_bytes(1))
        writer.write(headers)
        if data:
            writer.write(data)
        await writer.drain()
        ok: bool
        header_len: int
        ok, header_len = self.response_protocol.unpack(await reader.readexactly(3))
        data = await reader.readexactly(header_len) if header_len else b""
        writer.write_eof()
        writer.close()
        await writer.wait_closed()
        return ok, data


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
        headers = msgspec.msgpack.encode(request)
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
