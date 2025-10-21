# ============================= core/plugins_base.py =============================
from typing import Protocol, Awaitable

class Plugin(Protocol):
    name: str
    use_dedicated_threadpool: bool = False  # 默认不隔离

    async def setup(self, kernel) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
