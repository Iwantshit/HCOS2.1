# core/bus.py
import asyncio, time, logging, uuid
from typing import Any, Dict

logger = logging.getLogger(__name__)

class Event:
    def __init__(self, topic: str, payload: Dict[str, Any], priority: int = 5):
        self.topic = topic
        self.payload = payload
        self.ts = time.time()
        self.priority = priority

    def __lt__(self, other):
        return self.priority < other.priority


class InternalBus:
    def __init__(self):
        self._subs = {}
        self._queue = asyncio.PriorityQueue()
        self._running = False
        self._task = None

    # ======================================================
    # 订阅与取消订阅
    # ======================================================
    async def subscribe(self, topic, callback):
        logger.info(f"[Bus] subscribe id={id(self)} topic={topic}")
        self._subs.setdefault(topic, []).append(callback)

    async def unsubscribe(self, topic, callback):
        subs = self._subs.get(topic, [])
        if callback in subs:
            subs.remove(callback)

    # ======================================================
    # 普通发布事件（入队或立即分发）
    # ======================================================
    async def publish(self, event, immediate: bool = False):
        logger.info(f"[Bus] publish id={id(self)} topic={event.topic} priority={event.priority}")
        if immediate:
            await self._dispatch_event(event)
        else:
            await self._queue.put((event.priority, event))

    # ======================================================
    # 请求-响应机制
    # ======================================================
    async def request(
        self,
        topic: str,
        payload: dict,
        success_event: str,
        fail_event: str,
        timeout: float = 3.0,
        priority: int = 5,
    ):
        """
        发布命令事件并等待响应事件。
        返回 (bool, payload)
        """
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        req_id = str(uuid.uuid4())
        payload["req_id"] = req_id

        async def on_success(e):
            if e.payload.get("req_id") == req_id and not fut.done():
                fut.set_result((True, e.payload))

        async def on_fail(e):
            if e.payload.get("req_id") == req_id and not fut.done():
                fut.set_result((False, e.payload))

        await self.subscribe(success_event, on_success)
        await self.subscribe(fail_event, on_fail)

        await self.publish(Event(topic, payload, priority))

        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return (False, {"reason": "timeout"})
        finally:
            await self.unsubscribe(success_event, on_success)
            await self.unsubscribe(fail_event, on_fail)

    # ======================================================
    # 启动与停止分发循环
    # ======================================================
    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._dispatch_loop())
        logger.info("[Bus] dispatch loop started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[Bus] dispatch loop stopped")

    # ======================================================
    # 内部分发逻辑
    # ======================================================
    async def _dispatch_loop(self):
        while self._running:
            try:
                _, event = await self._queue.get()
                await self._dispatch_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Bus] dispatch error: {e}")

    async def _dispatch_event(self, event):
        handlers = []
        for topic, subs in self._subs.items():
            if topic.endswith("*") and event.topic.startswith(topic[:-1]):
                handlers += subs
            elif topic == event.topic:
                handlers += subs

        if not handlers:
            logger.debug(f"[Bus] No subscribers for {event.topic}")
            return

        start = time.time()
        for cb in handlers:
            try:
                await cb(event)
            except Exception as e:
                logger.exception(f"[Bus] handler {cb} failed: {e}")
        elapsed = (time.time() - start) * 1000
        logger.info(f"[Bus] event {event.topic} handled by {len(handlers)} subscribers in {elapsed:.2f} ms")
