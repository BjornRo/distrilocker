from __future__ import annotations
from abc import ABC, abstractmethod
import asyncio
import time
from typing import Literal
from pathlib import Path
import os
import sys
import uvloop  # type:ignore

type SocketType = Literal["unix", "tcp"]
type Expiry = int

now_time = lambda: int(time.time())

UNIX_SOCK_ADDRESS = "/dev/shm/dlserver/sock"


async def main():
    setting = sys.argv[1].lower()
    store_id = int(sys.argv[2])

    if setting == "unix":
        client = ClientUnix(store_id)
    else:
        sock_type, addr_path = setting.split("://", 1)
        if sock_type != "tcp":
            raise ValueError("unknown socket type")
        client = ClientUnix(store_id)

    asyncio.set_event_loop(uvloop.new_event_loop())
    start = time.time()
    print(await client.set_expiry("boDo", 1719955015))
    print("Elapsed:", time.time() - start)


class BaseClient(ABC):
    @abstractmethod
    async def _call(self, *args, **kwargs): ...

    async def get_expiry(self, key: str):
        return await self._call(key, method=b"get")

    async def set_expiry(self, key: str, expiry: int):
        return await self._call(f"{key}/{expiry}", method=b"set")

    async def update_expiry(self, key: str, expiry: int):
        return await self._call(f"{key}/{expiry}", method=b"update")

    async def delete_expiry(self, key: str):
        return await self._call(key, method=b"delete")


class ClientUnix(BaseClient):
    def __init__(self, store_id: int):
        filepath = Path(UNIX_SOCK_ADDRESS)
        if not os.path.exists(filepath):
            raise RuntimeError("DL server is not running")
        self.store_id = str(store_id).encode()

    async def _call(self, key: str, method: Literal[b"get", b"set", b"update", b"delete"]) -> int:
        reader, writer = await asyncio.open_unix_connection(UNIX_SOCK_ADDRESS)
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
