# ============================= core/message_queue.py =============================
import asyncio, time, logging, itertools
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict

logger = logging.getLogger(__name__)

@dataclass
class Message:
    task: str
    params: Dict[str, Any]
    delay_sec: float = 0.0
    timeout_sec: float = 30.0
    retries: int = 0
    max_retries: int = 3
    id: str = field(default_factory=lambda: f"msg_{int(time.time()*1000)}")

class MessageQueue:
    def __init__(self):
        self._queue = asyncio.PriorityQueue()
        self._handlers: Dict[str, Callable[[Message], Awaitable[None]]] = {}
        self._running = False
        self._counter = itertools.count()  # 👈 唯一自增计数器，解决比较问题

    async def register(self, name: str, cb: Callable[[Message], Awaitable[None]]):
        self._handlers[name] = cb

    async def put(self, msg: Message):
        # 添加唯一序号，确保元组之间可比较
        await self._queue.put((time.time() + msg.delay_sec, next(self._counter), msg))
        logger.debug(f"Queued message {msg.task} (delay={msg.delay_sec})")

    async def start(self, workers=2):
        """Start background worker tasks"""
        self._running = True
        for i in range(workers):
            asyncio.create_task(self._worker(i))
        logger.info(f"MessageQueue started with {workers} workers")

    async def _worker(self, wid: int):
        """Continuously process messages"""
        while self._running:
            t, _, msg = await self._queue.get()
            await asyncio.sleep(max(0, t - time.time()))
            h = self._handlers.get(msg.task)
            if not h:
                logger.warning(f"No handler registered for task '{msg.task}'")
                continue

            try:
                await asyncio.wait_for(h(msg), timeout=msg.timeout_sec)
            except asyncio.TimeoutError:
                logger.error(f"Task {msg.task} timed out after {msg.timeout_sec}s")
            except Exception as e:
                logger.exception(f"Worker-{wid} error handling {msg.task}: {e}")
