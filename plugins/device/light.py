# ============================= plugins/light.py =============================
from core.bus import Event
from core.resources import Resource
from core.message_queue import Message
import asyncio, logging, time

logger = logging.getLogger(__name__)

class LightPlugin():
    name = "light"
    use_dedicated_threadpool = False

    def __init__(self):
        super().__init__()
        self._lights = {}  # {id: {"state": "on"/"off"}}

    async def setup(self, kernel):
        self.k = kernel
        await self.k.bus.subscribe("cmd.light.on", self._cmd_on)
        await self.k.bus.subscribe("cmd.light.off", self._cmd_off)
        await self.k.bus.subscribe("cmd.light.toggle", self._cmd_toggle)
        await self.k.bus.subscribe("cmd.light.status", self._cmd_status)
        logger.info("[LightPlugin] setup complete")

    # ------------------ 开灯 ------------------
    async def _cmd_on(self, e: Event):
        rid = e.payload.get("id")
        if not rid:
            return

        await self.k.run_in_plugin_executor(self, self._sync_turn_on, rid)
        await self._update_state(rid, "on")

    # ------------------ 关灯 ------------------
    async def _cmd_off(self, e: Event):
        rid = e.payload.get("id")
        if not rid:
            return

        await self.k.run_in_plugin_executor(self, self._sync_turn_off, rid)
        await self._update_state(rid, "off")

    # ------------------ 切换 ------------------
    async def _cmd_toggle(self, e: Event):
        rid = e.payload.get("id")
        if not rid:
            return

        prev = self._lights.get(rid, {}).get("state", "off")
        new_state = "off" if prev == "on" else "on"
        await self.k.run_in_plugin_executor(self, self._sync_toggle_device, rid, new_state)
        await self._update_state(rid, new_state)

    # ------------------ 状态查询 ------------------
    async def _cmd_status(self, e: Event):
        rid = e.payload.get("id")
        if rid:
            state = self._lights.get(rid, {"state": "unknown"})
            await self.k.bus.publish(Event("evt.light.state", {"id": rid, "state": state["state"]}))
        else:
            # 返回所有灯状态
            await self.k.bus.publish(Event("evt.light.all_states", {"lights": self._lights}))

    # ------------------ 内部方法：同步设备操作 ------------------
    def _sync_turn_on(self, rid):
        logger.info(f"[Hardware] Turning ON {rid}")
        time.sleep(0.2)  # 模拟硬件延迟

    def _sync_turn_off(self, rid):
        logger.info(f"[Hardware] Turning OFF {rid}")
        time.sleep(0.2)

    def _sync_toggle_device(self, rid, new_state):
        logger.info(f"[Hardware] Toggling {rid} -> {new_state}")
        time.sleep(0.2)

    # ------------------ 内部方法：更新状态 + 资源同步 ------------------
    async def _update_state(self, rid, state):
        self._lights[rid] = {"state": state}
        await self.k.resources.upsert(Resource(
            resource_id=f"light:{rid}",
            kind="device",
            state={"state": state}
        ))
        await self.k.bus.publish(Event("evt.light.state", {"id": rid, "state": state}))
        logger.info(f"[LightPlugin] {rid} -> {state}")
