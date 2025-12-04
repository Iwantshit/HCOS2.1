# plugins/event_logger.py
import logging
from core.bus import Event

logger = logging.getLogger(__name__)

class EventLoggerPlugin:
    name = "event_logger"
    use_dedicated_threadpool = False
    device = {}
    is_collection = False

    def __init__(self, lang="zh"):
        """
        lang: "zh" 中文（默认）
              "en" 英文
        """
        self.lang = lang

    async def setup(self, kernel):
        self.k = kernel
        
        # await self.k.bus.subscribe("*", self._log_event)
        logger.info(self._t("[EventLogger] 初始化完成", "[EventLogger] setup complete"))

    async def start(self):
        # 通配符订阅所有事件
        await self.k.bus.subscribe("*", self._log_event)
        logger.info(self._t("[EventLogger] 已启动", "[EventLogger] started"))

    async def stop(self):
        await self.k.bus.unsubscribe("*", self._log_event)
        logger.info(self._t("[EventLogger] 已停止", "[EventLogger] stopped"))

    async def _log_event(self, e: Event):
        src = e.payload.get("source", "unknown")

        if self.lang == "zh":
            msg = (
                f"[事件日志] (主题={e.topic}) [优先级={e.priority}] "
                f"来自={src} 载荷={e.payload}"
            )
        else:  # English
            msg = (
                f"[EventLogger] (topic={e.topic}) [prio={e.priority}] "
                f"from={src} payload={e.payload}"
            )

        logger.info(msg)

    # ---- internal helper ----
    def _t(self, zh: str, en: str):
        """根据语言返回中英文文本"""
        return zh if self.lang == "zh" else en

    async def _update_state(self, rid, state, req_id=None):
        pass