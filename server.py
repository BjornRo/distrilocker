from __future__ import annotations
import asyncio
import time
from typing import Literal
from pathlib import Path
import os
import random
import sys
import uvloop  # type:ignore

type SocketType = Literal["unix", "tcp"]
type Expiry = int

now_time = lambda: int(time.time())

UNIX_SOCK_ADDRESS = "/dev/shm/dlserver/sock"


async def main():
    setting = sys.argv[1].lower()
    num_stores = int(sys.argv[2]) if len(sys.argv) == 3 else 4

    if setting == "unix":
        sock_type = "unix"
        addr_path = None
    else:
        sock_type, addr_path = setting.split("://", 1)
        if sock_type != "tcp":
            raise ValueError("unknown socket type")

    asyncio.set_event_loop(uvloop.new_event_loop())
    await Server(num_stores, sock_type, addr_path).run()


class _LockStore:
    def __init__(self):
        self.store: dict[str, Expiry] = {}
        self.lock = asyncio.Lock()

    async def init(self):
        async def periodic_clean(interval_time: int):
            while True:
                await asyncio.sleep(interval_time)
                async with self.lock:
                    if store_len := len(self.store):
                        for key, expiry in random.sample(tuple(self.store.items()), k=min(4, store_len)):
                            if now_time() > expiry:
                                del self.store[key]

        self._background_task = asyncio.create_task(periodic_clean(14400))

    async def get_expiry(self, key: str) -> Literal[0] | int:
        """Return 0 means no key found"""
        key = key.lower()
        async with self.lock:
            if key in self.store:
                expiry = self.store[key]
                if now_time() <= expiry:
                    return expiry
                del self.store[key]
        return 0

    async def set_expiry(self, key: str, expiry: int) -> Literal[0, 1]:
        """Return 0 means something went wrong, 1 means OK"""
        if now_time() <= expiry:
            key = key.lower()
            async with self.lock:
                if key not in self.store:
                    self.store[key] = expiry
                    return 1
        return 0

    async def update_expiry(self, key: str, expiry: int) -> Literal[0, 1]:
        """Return 0 means something went wrong, 1 means OK"""
        if now_time() <= expiry:
            key = key.lower()
            async with self.lock:
                if key in self.store:
                    self.store[key] = expiry
                    return 1
        return 0

    async def delete_expiry(self, key: str) -> Literal[1]:
        """Returns nothing useful"""
        key = key.lower()

        async with self.lock:
            if key in self.store:
                del self.store[key]
        return 1


class Server:
    def __init__(self, num_locks: int, socket_type: SocketType, address: str | None):
        """address: 0.0.0.0:2155 or None for unix"""
        if socket_type == "tcp":
            if not address or address and ":" not in address:
                raise ValueError("invalid address, should be ip_to_bind:port; ex: 0.0.0.0:1337")
        if num_locks <= 0:
            raise ValueError("More locks than 0")

        self.num_locks = num_locks
        self._address = address
        self._socket_type: SocketType = socket_type
        self._store = tuple(_LockStore() for _ in range(num_locks))

    async def run(self):
        if self._socket_type == "unix":
            filepath = Path(UNIX_SOCK_ADDRESS)
            if os.path.exists(filepath):
                os.remove(filepath)
                os.rmdir(filepath.parent)
            os.makedirs(filepath.parent, exist_ok=True)

            try:
                server = await asyncio.start_unix_server(self.__handler, path=filepath)
                async with server:
                    for i in self._store:
                        await i.init()
                    await server.serve_forever()
            except:
                try:
                    os.rmdir(filepath)
                except:
                    pass
        else:
            assert self._address is not None
            ip, port = self._address.split(":")
            server = await asyncio.start_server(self.__handler, host=ip, port=port)
            async with server:
                for i in self._store:
                    await i.init()
                await server.serve_forever()

    async def __handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            path = Path((await reader.readuntil()).decode())
            index, method, *args = path.parts[1:] if path.is_absolute() else path.parts
            store = self._store[int(index)]
            key = args[0]
            match method:
                case "get":
                    expiry = str(await store.get_expiry(key)).encode()
                    response = len(expiry).to_bytes(2) + expiry
                case "set":
                    response = b"\x00\x01" + str(await store.set_expiry(key, int(args[1]))).encode()
                case "update":
                    response = b"\x00\x01" + str(await store.update_expiry(key, int(args[1]))).encode()
                case "delete":
                    response = b"\x00\x01" + str(await store.delete_expiry(key)).encode()
                case _:
                    response = b"\x00\x0510101"
            writer.write(response)
            await writer.drain()
            writer.write_eof()
        except:
            pass
        writer.close()
        await writer.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
