from __future__ import annotations
from abc import ABC, abstractmethod
import asyncio

from typing import Literal
from pathlib import Path
import os

import msgspec

type Methods = Literal[b"size", b"get", b"set", b"update", b"delete"]
DEFAULT_UNIX_SOCK_ADDRESS = "/dev/shm/dlserver.sock"


class Request(msgspec.Struct):
    index: int
    method: Methods
    key: str
    expiry: int
    header_len: int

    def __post_init__(self):
        self.key = self.key.lower()


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
    print(await client.set_expiry("boDod", 17199550145))
    print("Elapsed:", time.time() - start)
    print(await client.size())


class ClientBase(ABC):
    @abstractmethod
    async def _call(self, *args, **kwargs): ...

    async def size(self):
        return await self._call("", method=b"size")

    async def get_expiry(self, key: str):
        return await self._call(key, method=b"get")

    async def set_expiry(self, key: str, expiry: int):
        return await self._call(f"{key}/{expiry}", method=b"set")

    async def update_expiry(self, key: str, expiry: int):
        return await self._call(f"{key}/{expiry}", method=b"update")

    async def delete_expiry(self, key: str):
        return await self._call(key, method=b"delete")


class ClientUnix(ClientBase):
    def __init__(self, store_id: int, path: str):
        self.filepath = Path(DEFAULT_UNIX_SOCK_ADDRESS) if path == "" else Path(path)

        if not os.path.exists(self.filepath):
            raise RuntimeError("DL server is not running")
        self.store_id = str(store_id).encode()

    async def _call(self, key: str, method: Methods) -> int:
        reader, writer = await asyncio.open_unix_connection(self.filepath)
        writer.write(self.store_id + b"/" + method + b"/" + key.encode() + b"\n")
        await writer.drain()
        writer.write_eof()
        header_len = int.from_bytes(await reader.readexactly(2))
        return int(await reader.readexactly(header_len))


class ClientTCP(ClientBase):
    def __init__(self, store_id: int, addr_port: str):
        self.addr, self.port = addr_port.split(":")
        self.store_id = str(store_id).encode()

    async def _call(self, key: str, method: Methods) -> int:
        reader, writer = await asyncio.open_connection(host=self.addr, port=self.port)
        writer.write(self.store_id + b"/" + method + b"/" + key.encode() + b"\n")
        await writer.drain()
        writer.write_eof()
        header_len = int.from_bytes(await reader.readexactly(2))
        return int(await reader.readexactly(header_len))


class ClientUDP(ClientBase):
    def __init__(self, store_id: int, addr_port: str):
        self.addr, port = addr_port.split(":")
        self.port = int(port)
        self.store_id = str(store_id).encode()
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()

        class EchoClientProtocol(asyncio.DatagramProtocol):
            def __init__(self, message: bytes, return_val: list[bytes], on_con_lost):
                self.message = message
                self.on_con_lost = on_con_lost
                self.transport = None
                self.return_val = return_val

            def connection_made(self, transport):
                self.transport = transport
                self.transport.sendto(self.message)

            def datagram_received(self, data, addr):
                self.return_val.append(data)
                self.transport.close()  # type:ignore

            def error_received(self, exc):
                pass

            def connection_lost(self, exc):
                self.on_con_lost.set_result(True)

        self.client_prot = EchoClientProtocol

    async def _call(self, key: str, method: Methods) -> int:
        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()
        return_val: list[bytes] = []
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: self.client_prot(
                self.store_id + b"/" + method + b"/" + key.encode(),
                return_val=return_val,
                on_con_lost=on_con_lost,
            ),
            remote_addr=(self.addr, self.port),
        )
        try:
            await on_con_lost
        finally:
            transport.close()
        return int(return_val[0]) if return_val else 0


if __name__ == "__main__":
    asyncio.run(main())
