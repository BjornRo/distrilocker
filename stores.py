import asyncio
import random
from typing import override
from server_utils import Request, ReturnResult, StoreBase, StoreExpiryItem, StoreItem, now_time


class Counter(StoreBase):
    async def _task_timer(self, key: str, expiry: int):
        await asyncio.sleep(expiry)
        del self.store[key]

    async def get(self, request: Request) -> ReturnResult:
        if request.key in self.store:
            return True, self.store[request.key].data
        return False, b""

    async def set(self, request: Request, data: bytes | None) -> ReturnResult:
        if (exp := request.expiry) != 0:
            if (key := request.key) not in self.store:
                task = None if exp is None else asyncio.create_task(self._task_timer(key, exp))
                self.store[key] = StoreItem(task=task, data=data or b"")
            else:
                if task := self.store[key].task:
                    task.cancel()
                self.store[key].task = None if exp is None else asyncio.create_task(self._task_timer(key, exp))
                self.store[key].data = data or b""
            return True, b""
        return False, b""

    async def delete(self, request: Request) -> ReturnResult:
        if request.key in self.store:
            if task := self.store[request.key].task:
                task.cancel()
            del self.store[request.key]
            return True, b""
        return False, b""

    async def update(self, *args, **kwargs):
        return False, b"not implemented"


class LockStore(StoreBase):
    def __init__(self):
        super().__init__()
        self.store: dict[str, StoreItem] = {}
        self.lock = asyncio.Lock()

    async def _task_timer(self, key: str, expiry: int):
        await asyncio.sleep(expiry)
        async with self.lock:
            del self.store[key]

    async def get(self, request: Request) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                return True, self.store[request.key].data
        return False, b""

    async def set(self, request: Request, data: bytes | None) -> ReturnResult:
        if (exp := request.expiry) != 0:
            async with self.lock:
                if (key := request.key) not in self.store:
                    task = None if exp is None else asyncio.create_task(self._task_timer(key, exp))
                    self.store[key] = StoreItem(task=task, data=data or b"")
                    return True, b""
        return False, b""

    async def update(self, request: Request, data: bytes | None) -> ReturnResult:
        async with self.lock:
            if (key := request.key) in self.store:
                if (exp := request.expiry) != 0:
                    if task := self.store[key].task:
                        task.cancel()
                    self.store[key].task = None if exp is None else asyncio.create_task(self._task_timer(key, exp))

                if data is not None:
                    self.store[request.key].data = data
                return True, b""
        return False, b""

    async def delete(self, request: Request) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                if task := self.store[request.key].task:
                    task.cancel()
                del self.store[request.key]
                return True, b""
        return False, b""


class LockStorePeriodicClean(StoreBase):
    def __init__(self, periodic_cleaning_interval_sec: int):
        self.store: dict[str, StoreExpiryItem] = {}
        self.lock = asyncio.Lock()
        self.interval = periodic_cleaning_interval_sec

    @override
    async def init(self):
        async def periodic_clean(interval_time: int):
            while True:
                await asyncio.sleep(interval_time)
                async with self.lock:
                    if store_len := len(self.store):
                        for key, item in random.sample(tuple(self.store.items()), k=min(4, store_len)):
                            if item.expiry and now_time() > item.expiry:
                                del self.store[key]

        self._background_task = asyncio.create_task(periodic_clean(self.interval))

    async def get(self, request: Request) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                if (exp := self.store[request.key].expiry) is None or now_time() <= exp:
                    return True, self.store[request.key].data
                del self.store[request.key]
        return False, b""

    async def set(self, request: Request, data: bytes | None) -> ReturnResult:
        if request.expiry != 0:
            async with self.lock:
                if request.key not in self.store:
                    exp = None if request.expiry is None else now_time() + request.expiry
                    self.store[request.key] = StoreExpiryItem(expiry=exp, data=data or b"")
                    return True, b""
        return False, b""

    async def delete(self, request: Request) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                del self.store[request.key]
                return True, b""
        return False, b""

    async def update(self, request: Request, data: bytes | None) -> ReturnResult:
        async with self.lock:
            if request.key in self.store:
                self.store[request.key].expiry = None if request.expiry is None else now_time() + request.expiry
                if data is not None:
                    self.store[request.key].data = data
                return True, b""
        return False, b""
