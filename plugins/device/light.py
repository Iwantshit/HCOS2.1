# ============================= plugins/light.py =============================
from core.bus import Event
from core.resources import Resource
from core.message_queue import Message
import asyncio, logging, time

logger = logging.getLogger(__name__)

class LightPlugin():
    name = "light"
    use_dedicated_threadpool = False

    def __init__(self, config):
        super().__init__()
        self.is_collection = True
        self._config = config

        # 记录订阅过的事件 → stop() 时可以注销
        self._subscriptions = []
        
        self.device = {}
        for device in config["device"]:
            self.device[device["id"]] = {}
            for k, v in device.items():
                logger.info(f"[LightPlugin] 加载设备 {k}: {v}")
                if k != "id":
                    self.device[device["id"]][k] = v
        

    async def setup(self, kernel):
        self.k = kernel
        logger.info("[LightPlugin] 硬件自检中...")
        # （你可以未来在这里做真正的硬件检查）
        await asyncio.sleep(0.2)
        logger.info("[LightPlugin] 硬件自检完成，插件已启动")
        

    # ------------------ 添加订阅并记录 ------------------
    async def _subscribe(self, topic, callback):
        """封装订阅方法，记录订阅信息用于 stop() 注销事件。"""
        await self.k.bus.subscribe(topic, callback)
        self._subscriptions.append((topic, callback))

    # ------------------ 插件启动 ------------------
    async def start(self):

        # ⭐ 订阅并记录所有事件

        await self._subscribe("cmd.light.on", self._cmd_on)
        await self._subscribe("cmd.light.off", self._cmd_off)
        await self._subscribe("cmd.light.toggle", self._cmd_toggle)
        await self._subscribe("cmd.light.status", self._cmd_status)

        # 更新 Plugin 状态到 Resource 系统
        await self.k.resources.upsert(Resource(
            resource_id="plugin:light",
            kind="plugin",
            state={"available": True}
        ))

        logger.info("[LightPlugin] 插件启动，已标记 available=True")
    
    # ------------------ 插件停止 ------------------
    async def stop(self):
        logger.info("[LightPlugin] 正在注销所有事件绑定...")

        # 取消所有注册事件
        for topic, callback in self._subscriptions:
            await self.k.bus.unsubscribe(topic, callback)

        self._subscriptions.clear()

        # 更新 Plugin 状态到 Resource 系统
        await self.k.resources.upsert(Resource(
            resource_id="plugin:light",
            kind="plugin",
            state={"available": False}
        ))

        logger.info("[LightPlugin] 插件停止，已标记 available=False")

    # ------------------ 设备存在性检查 ------------------
    def _check_exists(self, rid, req_id=None):
        if rid not in self.device:
            logger.warning(f"[LightPlugin] device {rid} not found")
            return Event("evt.light.error", {
                "reason": "device_not_found",
                "id": rid,
                "req_id": req_id
            })
        return None

    # ------------------ 开灯 ------------------
    async def _cmd_on(self, e: Event):
        rid = e.payload.get("id")

        if not rid and self.is_collection:
            await self.k.bus.publish(Event(
                "evt.light.error",
                {
                    "reason": "missing_device_id",
                    "message": "请指定设备ID",
                    "req_id": e.payload.get("req_id")
                }
            ))
            return
        
        err = self._check_exists(rid, e.payload.get("req_id"))
        if err:
            await self.k.bus.publish(err)
            return

        await self.k.run_in_plugin_executor(self, self._sync_turn_on, rid)
        await self._update_state(rid, "on", e.payload.get("req_id"))

    # ------------------ 关灯 ------------------
    async def _cmd_off(self, e: Event):
        rid = e.payload.get("id")

        if not rid and self.is_collection:
            await self.k.bus.publish(Event(
                "evt.light.error",
                {
                    "reason": "missing_device_id",
                    "message": "请指定设备ID",
                    "req_id": e.payload.get("req_id")
                }
            ))
            return

        err = self._check_exists(rid, e.payload.get("req_id"))
        if err:
            await self.k.bus.publish(err)
            return

        await self.k.run_in_plugin_executor(self, self._sync_turn_off, rid)
        await self._update_state(rid, "off", e.payload.get("req_id"))

    # ------------------ 切换 ------------------
    async def _cmd_toggle(self, e: Event):
        rid = e.payload.get("id")

        if not rid and self.is_collection:
            await self.k.bus.publish(Event(
                "evt.light.error",
                {
                    "reason": "missing_device_id",
                    "message": "请指定设备ID",
                    "req_id": e.payload.get("req_id")
                }
            ))
            return

        err = self._check_exists(rid, e.payload.get("req_id"))
        if err:
            await self.k.bus.publish(err)
            return

        prev = self.device[rid]["state"]
        new_state = "off" if prev == "on" else "on"

        await self.k.run_in_plugin_executor(self, self._sync_toggle_device, rid, new_state)
        await self._update_state(rid, new_state, e.payload.get("req_id"))

    # ------------------ 状态查询 ------------------
    async def _cmd_status(self, e: Event):
        rid = e.payload.get("id")

        if rid:
            if rid not in self.device:
                await self.k.bus.publish(Event("evt.light.error", {
                    "reason": "device_not_found",
                    "id": rid,
                    "req_id": e.payload.get("req_id")
                }))
                return
            
            state = self.device[rid]["state"]
            await self.k.bus.publish(Event("evt.light.state", {
                "id": rid,
                "state": state,
                "req_id": e.payload.get("req_id")
            }))
            return
        
        await self.k.bus.publish(Event("evt.light.all_states", {
            "lights": self.device,
            "req_id": e.payload.get("req_id")
        }))

    # ------------------ 同步模拟硬件操作 ------------------
    def _sync_turn_on(self, rid):
        logger.info(f"[Hardware] Turning ON {rid}")
        time.sleep(0.2)

    def _sync_turn_off(self, rid):
        logger.info(f"[Hardware] Turning OFF {rid}")
        time.sleep(0.2)

    def _sync_toggle_device(self, rid, new_state):
        logger.info(f"[Hardware] Toggling {rid} -> {new_state}")
        time.sleep(0.2)

    # ------------------ 更新状态并上报事件 ------------------
    async def _update_state(self, rid, state, req_id=None):
        self.device[rid]["state"] = state

        await self.k.resources.upsert(Resource(
            resource_id=f"light:{rid}",
            kind="device",
            state={"state": state}
        ))

        await self.k.bus.publish(Event("evt.light.state", {
            "id": rid,
            "state": state,
            "req_id": req_id
        }))

        logger.info(f"[LightPlugin] {rid} -> {state}")
