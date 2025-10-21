# ============================= plugins/light.py =============================
from core.bus import Event
from core.resources import Resource
from core.message_queue import Message
import asyncio, logging
logger = logging.getLogger(__name__)

class LightPlugin:
    name = "light"
    use_dedicated_threadpool = False  # 共享主线程池

    async def setup(self, kernel):
        self.k = kernel
        await self.k.bus.subscribe("cmd.light.*", self._cmd)
        await self.k.mq.register("light.self_check", self._self_check)
    
    async def start(self):
        for rid in ("light.living", "light.kitchen"):
            await self.k.resources.upsert(Resource(rid, "device.light", state={"on": False}))
        logger.info(f"[light] is Running.")
        self.k.spawn(self._loop())

    async def stop(self):
        logger.info(f"[light] is Stopped.")
    
    async def _cmd(self, e: Event):
        rid, act = e.payload.get("id"), e.payload.get("action")
        # 使用 run_in_plugin_executor 运行阻塞设备指令
        await self.k.run_in_plugin_executor(self, self._sync_toggle_device, rid, act)
        await self.k.bus.publish(Event("evt.light.state", {"id": rid, "action": act}))

    def _sync_toggle_device(self, rid, act):
        """这里模拟同步I/O指令，放入线程池"""
        import time
        time.sleep(0.5)  # 模拟I/O延迟
        logger.info(f"[{self.name}] toggled {rid} -> {act}")

    async def _loop(self):
        while True:
            await self.k.mq.put(Message(task="light.self_check", params={}))
            await asyncio.sleep(30)

    async def _self_check(self, msg): ...
