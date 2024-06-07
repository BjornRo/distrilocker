import asyncio
from .utils import Request, ReturnResult, StoreBase, StoreItem, StoreLockItem


class _StoreTaskTimer(StoreBase[StoreLockItem]):
    async def _task_timer(self, key: str, expiry: int):
        await asyncio.sleep(expiry)
        try:
            async with self.store[key].lock:
                del self.store[key]
        except:
            pass


class Counter(_StoreTaskTimer):
    """Set increases, Delete deletes"""

    async def get(self, _: Request) -> ReturnResult:
        return False, b"not implemented"

    async def set(self, request: Request, _: bytes | None) -> ReturnResult:
        if (exp := request.expiry) != 0:
            key = request.key
            data = b"\x01"
            try:
                async with self.store[key].lock:
                    if task := self.store[key].task:
                        task.cancel()
                    self.store[key].task = None if exp is None else asyncio.create_task(self._task_timer(key, exp))
                    if (value := int.from_bytes(self.store[key].data)) < 4294967294:
                        data = (value + 1).to_bytes(4)
                        self.store[key].data = data
            except:
                task = None if exp is None else asyncio.create_task(self._task_timer(key, exp))
                self.store[key] = StoreLockItem(task=task, data=data)
            return True, data
        return False, b""

    async def delete(self, request: Request) -> ReturnResult:
        try:
            key = request.key
            async with self.store[key].lock:
                if task := self.store[key].task:
                    task.cancel()
                del self.store[key]
            return True, b""
        except:
            return False, b""

    async def update(self, *args, **kwargs):
        return False, b"not implemented"


class LockCache(_StoreTaskTimer):
    async def get(self, request: Request) -> ReturnResult:
        try:  # No need to lock here
            return True, self.store[request.key].data
        except:
            return False, b""

    async def set(self, request: Request, data: bytes | None) -> ReturnResult:
        if (exp := request.expiry) != 0:
            key = request.key
            try:
                async with self.store[key].lock:
                    if task := self.store[key].task:
                        task.cancel()
                    self.store[key].task = None if exp is None else asyncio.create_task(self._task_timer(key, exp))
                    self.store[key].data = data or b""
            except:
                self.store[key] = StoreLockItem(
                    task=None if exp is None else asyncio.create_task(self._task_timer(key, exp)),
                    data=data or b"",
                )
            return True, b""
        return False, b""

    async def delete(self, request: Request) -> ReturnResult:
        key = request.key
        try:
            async with self.store[key].lock:
                if task := self.store[key].task:
                    task.cancel()
                del self.store[key]
                return True, b""
        except:
            return False, b""

    async def update(self, *args, **kwargs):
        return False, b"not implemented"


class LockStore(StoreBase[StoreItem]):  # slow due to global lock
    def __init__(self):
        super().__init__()
        self.lock = asyncio.Lock()

    async def _task_timer(self, key: str, expiry: int):
        await asyncio.sleep(expiry)
        async with self.lock:
            try:
                del self.store[key]
            except:
                pass

    async def get(self, request: Request) -> ReturnResult:
        async with self.lock:
            try:
                return True, self.store[request.key].data
            except:
                return False, b""

    async def set(self, request: Request, data: bytes | None) -> ReturnResult:
        if (exp := request.expiry) != 0:
            async with self.lock:
                key = request.key
                if key not in self.store:
                    self.store[key] = StoreItem(
                        task=None if exp is None else asyncio.create_task(self._task_timer(key, exp)),
                        data=data or b"",
                    )
                    return True, b""
        return False, b""

    async def update(self, request: Request, data: bytes | None) -> ReturnResult:
        try:
            async with self.lock:
                key = request.key
                if (exp := request.expiry) != 0:
                    if task := self.store[key].task:
                        task.cancel()
                    self.store[key].task = None if exp is None else asyncio.create_task(self._task_timer(key, exp))

                if data is not None:
                    self.store[key].data = data
            return True, b""
        except:
            return False, b""

    async def delete(self, request: Request) -> ReturnResult:
        try:
            async with self.lock:
                if task := self.store[request.key].task:
                    task.cancel()
                del self.store[request.key]
            return True, b""
        except:
            return False, b""
