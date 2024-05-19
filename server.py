from __future__ import annotations
import asyncio
from collections.abc import Callable
from pathlib import Path
import os
from server_utils import ProtocolStrategyBase, Request, Address, StoreBase
import stores

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

    store_strat = lambda: stores.Counter()
    protocol, addr_path = uri.split("://")
    match protocol:
        case "tcp":
            server = ProtocolTCP(num_stores, addr_path, store_strat)
        case "udp":
            server = ProtocolUDP(num_stores, addr_path, store_strat)
        case "unix":
            server = ProtocolUNIX(num_stores, addr_path, store_strat)
    await server.run()


class ProtocolTCP(ProtocolStrategyBase):
    def __init__(self, num_stores: int, address_port: str, store_type: Callable[[], StoreBase]):
        """Ex: address_port = '0.0.0.0:1337'"""
        super().__init__(num_stores=num_stores, store_type=store_type)
        self.address, self.port = address_port.lower().split(":")

    async def run(self):
        server = await asyncio.start_server(self._handler, host=self.address, port=self.port)
        async with server:
            for i in self._store:
                await i.init()
            await server.serve_forever()


class ProtocolUDP(ProtocolStrategyBase):
    def __init__(self, num_stores: int, address_port: str, store_type: Callable[[], StoreBase]):
        """Ex: address_port = '0.0.0.0:1337'"""
        super().__init__(num_stores=num_stores, store_type=store_type)
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
        for i in self._store:
            await i.init()
        self._background_task = asyncio.create_task(message_handler_task())
        try:
            await asyncio.Future()
        except:
            pass
        self._background_task.cancel()
        transport.close()


class ProtocolUNIX(ProtocolStrategyBase):
    def __init__(self, num_stores: int, filepath: str, store_type: Callable[[], StoreBase]):
        """path/to/me or just_me"""
        super().__init__(num_stores=num_stores, store_type=store_type)
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
