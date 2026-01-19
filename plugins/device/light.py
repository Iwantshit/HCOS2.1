# ============================= plugins/light.py =============================
from core.bus import Event
from core.resources import Resource
from core.message_queue import Message
import asyncio, logging, time
from core.plugins_base import Entity

logger = logging.getLogger(__name__)

class LightEntity(Entity):

    def __init__(self, id: str, name: str, location: str, unique_id: str, state: str, extra_parameter, device_info: dict):
        super().__init__(id, name, location, unique_id, state, extra_parameter, device_info)

    def sync_turn_on(self, **kwargs) -> bool:
        self.update_state("on")# 只能在实体class中调用update_state
        time.sleep(2)
        return True
    def sync_turn_off(self, **kwargs) -> bool:
        self.update_state("off")
        time.sleep(2)
        return True


class LightPlugin():
    name = "light"
    use_dedicated_threadpool = False

    def __init__(self, config):
        super().__init__()
        # self.is_collection = True
        self._config = config

        # 记录订阅过的事件 → stop() 时可以注销
        self._subscriptions = []
        
        self.device = {}

    async def setup(self, kernel) -> list[Entity]:
        self.k = kernel
        logger.info("[LightPlugin] 硬件自检中...")
        # （你可以未来在这里做真正的硬件检查）
        # await asyncio.sleep(0.2)
        return_entity_list = []
        for device in self._config["device"]:
            extra_parameter = {}
            for k, v in device.items():
                    if k not in ['id', 'name', 'location', 'unique_id', 'state']:
                        extra_parameter[k] = v
            new_light_entity = LightEntity(
                id=device["id"],
                name=device["name"],
                location=device["location"],
                unique_id=device["unique_id"],
                state="off",
                extra_parameter=extra_parameter,
                device_info={
                    'info': None # 设备信息
                }
            )
            self.device[device["id"]] = new_light_entity
            return_entity_list.append(new_light_entity)
        logger.info("[LightPlugin] 硬件自检完成，插件已启动")
        return return_entity_list
    
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
        await self._subscribe("cmd.light.state", self._cmd_state)

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
    def _check_exists(self, device_id, req_id=None):
        if device_id not in self.device:
            logger.warning(f"[LightPlugin] device {device_id} not found")
            return Event("evt.light.error", {
                "reason": "device_not_found",
                "id": device_id,
                "req_id": req_id
            })
        return None

    # ------------------ 开灯 ------------------
    async def _cmd_on(self, e: Event):
        device_id = e.payload.get("id")

        if not device_id and self.is_collection:
            await self.k.bus.publish(Event(
                "evt.light.error",
                {
                    "reason": "missing_device_id",
                    "message": "请指定设备ID",
                    "req_id": e.payload.get("req_id")
                }
            ))
            return
        
        err = self._check_exists(device_id, e.payload.get("req_id"))
        if err:
            await self.k.bus.publish(err)
            return

        await self.k.run_in_plugin_executor(self, self._sync_turn_on, device_id)
        await self._update_state(device_id, "on", e.payload.get("req_id"))

    # ------------------ 关灯 ------------------
    async def _cmd_off(self, e: Event):
        device_id = e.payload.get("id")

        if not device_id and self.is_collection:
            await self.k.bus.publish(Event(
                "evt.light.error",
                {
                    "reason": "missing_device_id",
                    "message": "请指定设备ID",
                    "req_id": e.payload.get("req_id")
                }
            ))
            return

        err = self._check_exists(device_id, e.payload.get("req_id"))
        if err:
            await self.k.bus.publish(err)
            return

        await self.k.run_in_plugin_executor(self, self._sync_turn_off, device_id)
        await self._update_state(device_id, "off", e.payload.get("req_id"))

    # ------------------ 切换 ------------------
    async def _cmd_toggle(self, e: Event):
        device_id = e.payload.get("id")

        if not device_id and self.is_collection:
            await self.k.bus.publish(Event(
                "evt.light.error",
                {
                    "reason": "missing_device_id",
                    "message": "请指定设备ID",
                    "req_id": e.payload.get("req_id")
                }
            ))
            return

        err = self._check_exists(device_id, e.payload.get("req_id"))
        if err:
            await self.k.bus.publish(err)
            return

        prev = self.device[device_id].state
        new_state = "off" if prev == "on" else "on"

        await self.k.run_in_plugin_executor(self, self._sync_toggle_device, device_id, new_state)
        await self._update_state(device_id, new_state, e.payload.get("req_id"))
    async def _cmd_state(self, e: Event):
        device_id = e.payload.get("id")

        if device_id:
            if device_id not in self.device:
                await self.k.bus.publish(Event("evt.light.error", {
                    "reason": "device_not_found",
                    "id": device_id,
                    "req_id": e.payload.get("req_id")
                }))
                return
            
            state = self.device[device_id].state
            await self.k.bus.publish(Event("evt.light.state", {
                "id": device_id,
                "state": state,
                "req_id": e.payload.get("req_id")
            }))
            return
        
        # Filter to return only id and state for each device
        lights_state = {
            device_id: {"state": device_entity.state}
            for device_id, device_entity in self.device.items()
        }
        await self.k.bus.publish(Event("evt.light.all_states", {
            "lights": lights_state,
            "req_id": e.payload.get("req_id")
        }))

    # ------------------ 插件调用实体硬件同步接口 ------------------
    def _sync_turn_on(self, device_id):
        logger.info(f"[Light Hardware] Turning ON {device_id}")
        if self.device[device_id].state == "off":
            res = self.device[device_id].sync_turn_on()
            if res:
                logger.info(f"[Light Hardware] Turning ON {device_id} -> Success")
            else:
                logger.warning(f"[Light Hardware] Turning ON {device_id} -> Failed")
        else:
            logger.warning(f"[Light Hardware] Turning ON {device_id} -> Already ON")

    def _sync_turn_off(self, device_id):
        logger.info(f"[Light Hardware] Turning OFF {device_id}")
        if self.device[device_id].state == "off":
            res = self.device[device_id].sync_turn_off()
            if res:
                logger.info(f"[Light Hardware] Turning OFF {device_id} -> Success")
            else:
                logger.warning(f"[Light Hardware] Turning OFF {device_id} -> Failed")
        else:
            logger.warning(f"[Light Hardware] Turning OFF {device_id} -> Already OFF")

    def _sync_toggle_device(self, device_id, new_state):
        logger.info(f"[Light Hardware] Toggling {device_id} -> {new_state}")
        if new_state == "off":
            res = self.device[device_id].sync_turn_off()
            if res:
                logger.info(f"[Light Hardware] Turning OFF {device_id} -> Success")
            else:
                logger.warning(f"[Light Hardware] Turning OFF {device_id} -> Failed")
        elif new_state == "on":
            res = self.device[device_id].sync_turn_on()
            if res:
                logger.info(f"[Light Hardware] Turning ON {device_id} -> Success")
            else:
                logger.warning(f"[Light Hardware] Turning ON {device_id} -> Failed")

    # ------------------ 更新状态并上报事件 ------------------
    async def _update_state(self, device_id, state, req_id=None):
        # self.device[rid].state = state

        await self.k.resources.upsert(Resource(
            resource_id=f"light:{device_id}",
            kind="device",
            state={"state": state}
        ))

        await self.k.bus.publish(Event("evt.light.state", {
            "id": device_id,
            "state": state,
            "req_id": req_id
        }))

        logger.info(f"[LightPlugin] {device_id} -> {state}")
