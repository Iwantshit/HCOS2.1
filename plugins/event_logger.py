# plugins/event_logger.py
import logging
from core.bus import Event

logger = logging.getLogger(__name__)

class EventLoggerPlugin:
    name = "event_logger"
    use_dedicated_threadpool = False

    async def setup(self, kernel):
        self.k = kernel
        # 通配符订阅所有事件
        await self.k.bus.subscribe("*", self._log_event)
        logger.info("[EventLogger] setup complete")

    async def start(self):
        logger.info("[EventLogger] started")

    async def stop(self):
        logger.info("[EventLogger] stopped")

    async def _log_event(self, e: Event):
        src = e.payload.get("source", "unknown")
        logger.info(
            f"[EventLogger] ({e.topic}) [prio={e.priority}] "
            f"from={src} payload={e.payload}"
        )