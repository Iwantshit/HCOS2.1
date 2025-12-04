# ============================= core/bus.py =============================
import asyncio, time, logging, uuid, itertools
from typing import Any, Dict

logger = logging.getLogger(__name__)


# ======================================================
# Event with priority (0 = lowest, 9 = highest)
# ======================================================
class Event:
    def __init__(self, topic: str, payload: Dict[str, Any], priority: int = 3):
        # clamp 0–9
        self.priority = max(0, min(9, priority))
        self.topic = topic
        self.payload = payload
        self.ts = time.time()

    def __repr__(self):
        return f"Event(topic={self.topic}, prio={self.priority})"


# ======================================================
# InternalBus: real-time dispatch with priority fallback
# ======================================================
class InternalBus:
    def __init__(self):
        self._subs = {}
        self._queue = asyncio.PriorityQueue()
        self._running = False
        self._task = None

        # 用于稳定排序相同优先级和相同时间的事件
        self._counter = itertools.count()

    # ======================================================
    # subscribe / unsubscribe
    # ======================================================
    async def subscribe(self, topic, callback):
        logger.info(f"[Bus] subscribe topic={topic}")
        self._subs.setdefault(topic, []).append(callback)

    async def unsubscribe(self, topic, callback):
        subs = self._subs.get(topic, [])
        if callback in subs:
            subs.remove(callback)

    # ======================================================
    # publish — 实时分发 + 优先级用于堆积情况
    # ======================================================
    async def publish(self, event: Event):
        """
        优先级队列结构：
            (-priority, counter, event, future)

        说明：
            -priority 让“数值大的事件”排在最前面
            counter 用来避免 PriorityQueue 比较两个 event 时出错
        """

        logger.info(f"[Bus] publish topic={event.topic} prio={event.priority}")

        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        # 🔥 使用 -priority，让 9 > 0
        await self._queue.put(
            (-event.priority, next(self._counter), event, fut)
        )

        # publish 等待 dispatch 完成（非阻塞事件循环）
        tasks = await fut
        return tasks

    # ======================================================
    # request 保持不变
    # ======================================================
    async def request(
        self,
        topic: str,
        payload: dict,
        success_event: str,
        fail_event: str,
        timeout: float = 3.0,
        priority: int = 3,
    ):
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
    # 启动
    # ======================================================
    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._dispatch_loop())
        logger.info("[Bus] started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[Bus] stopped")

    # ======================================================
    # Dispatcher Loop
    # ======================================================
    async def _dispatch_loop(self):
        while self._running:
            try:
                _, _, event, fut = await self._queue.get()
                tasks = await self._dispatch_event(event)

                if not fut.done():
                    fut.set_result(tasks)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Bus] dispatch error: {e}")

    # ======================================================
    # 分发事件
    # ======================================================
    async def _dispatch_event(self, event):
        handlers = []

        for topic, subs in self._subs.items():
            if topic.endswith("*") and event.topic.startswith(topic[:-1]):
                handlers += subs
            elif topic == event.topic:
                handlers += subs

        if not handlers:
            logger.warning(f"[Bus] event '{event.topic}' has no subscribers")
            return []

        tasks = []
        start = time.time()

        for cb in handlers:
            tasks.append(asyncio.create_task(cb(event)))

        elapsed = (time.time() - start) * 1000
        logger.info(
            f"[Bus] dispatch {event.topic} → {len(handlers)} handlers in {elapsed:.2f}ms"
        )

        return tasks
