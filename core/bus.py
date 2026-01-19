import asyncio
import itertools
import time
import uuid
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


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
# InternalBus
# ======================================================
class InternalBus:
    def __init__(self):
        self._subs = {}
        self._queue = asyncio.PriorityQueue()
        self._running = False
        self._task = None

        # 稳定排序
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
    # publish (RPC-style: wait for dispatch)
    # ======================================================
    async def publish(self, event: Event):
        """
        publish = 等待 dispatch 完成
        返回已 create 的 tasks
        """
        if not self._running:
            raise RuntimeError("Bus not started")

        logger.info(f"[Bus] publish topic={event.topic} prio={event.priority}")

        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        await self._queue.put(
            (-event.priority, next(self._counter), event, fut)
        )

        tasks = await fut
        return tasks

    # ======================================================
    # publish_async (fire-and-forget)
    # ======================================================
    def publish_async(self, event: Event):
        """
        fire-and-forget
        不等待 dispatch
        不返回 tasks
        """
        if not self._running:
            raise RuntimeError("Bus not started")

        logger.info(f"[Bus] publish_async topic={event.topic} prio={event.priority}")

        asyncio.create_task(
            self._queue.put(
                (-event.priority, next(self._counter), event, None)
            )
        )

    # ======================================================
    # request 
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
    # lifecycle
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
    # dispatcher loop
    # ======================================================
    async def _dispatch_loop(self):
        while self._running:
            try:
                _, _, event, fut = await self._queue.get()

                tasks = await self._dispatch_event(event)

                if fut is not None and not fut.done():
                    fut.set_result(tasks)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"[Bus] dispatch error: {e}")

    # ======================================================
    # dispatch event
    # ======================================================
    async def _dispatch_event(self, event: Event):
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
            task = asyncio.create_task(cb(event))
            task.add_done_callback(
                lambda t, ev=event: logger.exception(
                    f"[Bus] handler error for {ev.topic}",
                    exc_info=t.exception()
                ) if t.exception() else None
            )
            tasks.append(task)

        elapsed = (time.time() - start) * 1000
        logger.info(
            f"[Bus] dispatch {event.topic} → {len(handlers)} handlers in {elapsed:.2f}ms"
        )

        return tasks
