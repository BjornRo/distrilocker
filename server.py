from __future__ import annotations
import asyncio
import time
from typing import Literal
from pathlib import Path
import os
import random
import sys

type Expiry = int

now_time = lambda: int(time.time())

DEFAULT_UNIX_SOCK_ADDRESS = "/dev/shm/dlserver.sock"


async def main():
    try:
        import uvloop  # type:ignore

        asyncio.set_event_loop(uvloop.new_event_loop())
    except:
        pass

    """Examples:
    tcp://0.0.0.0:8888, unix://not/home/but/here"""
    setting = sys.argv[1].lower()
    num_stores = int(sys.argv[2]) if len(sys.argv) == 3 else 4

    await Server(num_stores, setting).run()


class _LockStore:
    def __init__(self, periodic_cleaning_interval_sec: int):
        self.store: dict[str, Expiry] = {}
        self.lock = asyncio.Lock()
        self.interval = periodic_cleaning_interval_sec

    async def init(self):
        async def periodic_clean(interval_time: int):
            while True:
                await asyncio.sleep(interval_time)
                async with self.lock:
                    if store_len := len(self.store):
                        for key, expiry in random.sample(tuple(self.store.items()), k=min(4, store_len)):
                            if now_time() > expiry:
                                del self.store[key]

        self._background_task = asyncio.create_task(periodic_clean(self.interval))

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
    def __init__(self, num_locks: int, uri: str, periodic_cleaning_interval_sec: int = 14400):
        """address: 0.0.0.0:2155 or unix(default)/unix:///path/to/me for unix"""
        if num_locks <= 0:
            raise ValueError("More locks than 0")

        path = uri.split("://")
        if path[0] == "unix":
            self._address = DEFAULT_UNIX_SOCK_ADDRESS if (len(path) == 1 or path[1] == "") else path[1]
        elif path[0] == "tcp":
            self._address = path[1]
            if ":" not in self._address:
                raise ValueError("invalid address, should be ip_to_bind:port; ex: 0.0.0.0:1337")
        else:
            raise ValueError("unknown protocol")

        self.num_locks = num_locks
        self._socket_type: SocketType = path[0]  # type:ignore
        self._store = tuple(_LockStore(periodic_cleaning_interval_sec) for _ in range(num_locks))

    async def run(self):
        if self._socket_type == "unix":
            filepath = Path(self._address)
            if os.path.exists(filepath):
                os.remove(filepath)
            else:
                if str(filepath.parent) != ".":
                    os.makedirs(filepath.parent, exist_ok=True)
            try:
                server = await asyncio.start_unix_server(self.__handler, path=filepath)
                async with server:
                    for i in self._store:
                        await i.init()
                    await server.serve_forever()
            except:
                try:
                    os.remove(filepath)
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
