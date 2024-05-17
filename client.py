from __future__ import annotations
from abc import ABC, abstractmethod
import asyncio

from typing import Literal
from pathlib import Path
import os


DEFAULT_UNIX_SOCK_ADDRESS = "/dev/shm/dlserver.sock"


async def main():
    import time
    import sys

    protocol, *addr_path = sys.argv[1].lower().split("://")
    store_id = int(sys.argv[2])

    if protocol == "unix":
        client = ClientUnix(store_id, addr_path[0] if addr_path else "")
    elif protocol == "tcp":
        client = ClientTCP(store_id, addr_port=addr_path[0])
    else:
        raise ValueError("Invalid protocol")

    print(await client.size())
    start = time.time()
    print(await client.set_expiry("boDo", 1719955015))
    print(await client.set_expiry("boDod", 1719955015))
    print("Elapsed:", time.time() - start)
    print(await client.size())


class BaseClient(ABC):
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


class ClientUnix(BaseClient):
    def __init__(self, store_id: int, path: str):
        self.filepath = Path(DEFAULT_UNIX_SOCK_ADDRESS) if path == "" else Path(path)

        if not os.path.exists(self.filepath):
            raise RuntimeError("DL server is not running")
        self.store_id = str(store_id).encode()

    async def _call(self, key: str, method: Literal[b"size", b"get", b"set", b"update", b"delete"]) -> int:
        reader, writer = await asyncio.open_unix_connection(self.filepath)
        writer.write(self.store_id + b"/" + method + b"/" + key.encode() + b"\n")
        await writer.drain()
        writer.write_eof()
        header_len = int.from_bytes(await reader.readexactly(2))
        return int(await reader.readexactly(header_len))


class ClientTCP(BaseClient):
    def __init__(self, store_id: int, addr_port: str):
        self.addr, self.port = addr_port.split(":")
        self.store_id = str(store_id).encode()

    async def _call(self, key: str, method: Literal[b"get", b"set", b"update", b"delete"]) -> int:
        reader, writer = await asyncio.open_connection(host=self.addr, port=self.port)
        writer.write(self.store_id + b"/" + method + b"/" + key.encode() + b"\n")
        await writer.drain()
        writer.write_eof()
        header_len = int.from_bytes(await reader.readexactly(2))
        return int(await reader.readexactly(header_len))


if __name__ == "__main__":
    asyncio.run(main())
