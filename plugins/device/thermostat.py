# ============================= plugins/thermostat.py =============================
import asyncio, logging, time, random
from core.bus import Event
from core.resources import Resource
from core.message_queue import Message
from core.plugins_base import Entity

logger = logging.getLogger(__name__)

class ThermostatEntity(Entity):
    def __init__(self, id: str, name: str, location: str, unique_id: str, state: str, extra_parameter, device_info: dict):
        super().__init__(id, name, location, unique_id, state, extra_parameter, device_info)
    
    def sync_turn_on(self, **kwargs) -> bool:
        self.update_state("on") # 只能在实体class中调用update_state
        time.sleep(0.2)
        return True
    def sync_turn_off(self, **kwargs) -> bool:
        self.update_state("off")
        time.sleep(0.2)
        return True
    
    def update_target(self, target):
        self.extra_parameter['target'] = target

    def update_current(self, current): # 更新温度 这个是从硬件获取的，不能允许系统主动设置
        self.extra_parameter['current'] = current

    def sync_set_target(self, temp:float, **kwargs) -> bool:
        self.update_target(temp)
        time.sleep(0.2)
        return True

    def sync_get_current(self, **kwargs) -> float:
        time.sleep(0.2)
        temp =  20 + random.random() * 5 #随便给了个温度cos一下从硬件读取的温度
        self.update_current(temp)
        return temp
    

class ThermostatPlugin:
    name = "thermostat"
    use_dedicated_threadpool = True

    def __init__(self, config):
        super().__init__()

        # self.is_collection = False
        self._config = config
        self._subscriptions = []

        # 加载设备配置
        self.device = {}

        # 最关键：由 MQ 控制轮询逻辑
        self.isrunning = False

    # ---------------------------------------------
    # setup
    # ---------------------------------------------
    async def setup(self, kernel) -> list[Entity]:
        self.k = kernel

        logger.info("[ThermostatPlugin] 硬件自检中...")
        return_entity_list = []
        for device in self._config["device"]:
            extra_parameter = {}
            for k, v in device.items():
                    if k not in ['id', 'name', 'location', 'unique_id', 'state']:
                        extra_parameter[k] = v
            new_light_entity = ThermostatEntity(
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
        logger.info("[ThermostatPlugin] 已初始化")

        return return_entity_list
    

    # ---------------------------------------------
    # 帮助函数：记录订阅
    # ---------------------------------------------
    async def _subscribe(self, topic, callback):
        await self.k.bus.subscribe(topic, callback)
        self._subscriptions.append((topic, callback))

    # ---------------------------------------------
    # 插件启动
    # ---------------------------------------------
    async def start(self):

        # 注册 MQ 任务
        await self.k.mq.register("thermo.poll", self._mq_poll)
        await self.k.mq.register("thermo.update", self._update_state)
        await self._subscribe("cmd.thermostat.on", self._cmd_on)
        await self._subscribe("cmd.thermostat.off", self._cmd_off)
        await self._subscribe("cmd.thermostat.set", self._cmd_set)
        await self._subscribe("cmd.thermostat.state", self._cmd_state)

        await self.k.resources.upsert(Resource(
            resource_id="plugin:thermostat",
            kind="plugin",
            state={"available": True}
        ))

        logger.info("[ThermostatPlugin] 启动完成，等待用户开启设备")

    # ---------------------------------------------
    # 插件停止
    # ---------------------------------------------
    async def stop(self):
        logger.info("[ThermostatPlugin] 正在停止插件...")

        self.isrunning = False  # 停止轮询（不会再投递 poll）

        for topic, cb in self._subscriptions:
            await self.k.bus.unsubscribe(topic, cb)
        self._subscriptions.clear()

        await self.k.resources.upsert(Resource(
            resource_id="plugin:thermostat",
            kind="plugin",
            state={"available": False}
        ))

        logger.info("[ThermostatPlugin] 已停止")

    # ============================================================================
    # 开 / 关 功能
    # ============================================================================
    async def _cmd_on(self, e: Event):

        device_id = e.payload.get("id")

        if self.device[device_id].state == "on":
            return

        self._sync_turn_on(device_id)
        self.isrunning = True

        # 直接投递第一次轮询任务
        await self.k.mq.put(Message(
            task="thermo.poll",
            params={"id": self.device[device_id].id},
            delay_sec=0.0
        ))

        await self._mq_update_from_event(e)
        logger.info("[ThermostatPlugin] 已开启（MQ轮询启动）")

    async def _cmd_off(self, e: Event):
        device_id = e.payload.get("id")

        if self.device["state"] == "off":
            return

        self._sync_turn_off(device_id)
        self.isrunning = False  # ⚠ MQ 将不会再继续投递 poll

        await self._mq_update_from_event(e)
        logger.info("[ThermostatPlugin] 已关闭（停止轮询）")

    # ============================================================================
    # 设置目标温度
    # ============================================================================
    async def _cmd_set(self, e: Event):
        target = e.payload.get("target")
        if target is None:
            await self._send_error("missing_target", e.payload)
            return

        # 阻塞动作放线程池
        await self.k.run_in_plugin_executor(self, self._sync_set_target, float(target))

        self.device["target"] = float(target)

        # 更新状态
        await self._mq_update_from_event(e)

    async def _cmd_state(self, e: Event):
        await self.k.bus.publish(Event("evt.thermostat.state", {
            "id": self.device["id"],
            "current": self.device["current"],
            "target": self.device["target"],
            "state": self.device["state"],
            "req_id": e.payload.get("req_id")
        }))

    # ============================================================================
    # MQ：轮询逻辑
    # ============================================================================
    async def _mq_poll(self, msg: Message):
        """在 MQ worker 中执行轮询任务"""

        if not self.isrunning:
            return  # 已关闭，不再投递后续任务

        # 阻塞操作放线程池
        device_id = msg.params.get("id")
        temp = await self.k.run_in_plugin_executor(self, self._poll_sensor_sync, device_id)
        self.device["current"] = temp

        # 投递状态更新任务
        await self.k.mq.put(Message(
            task="thermo.update",
            params={"id": device_id}
        ))

        # 再次投递下一次轮询
        await self.k.mq.put(Message(
            task="thermo.poll",
            params={"id": device_id},
            delay_sec=0.5  # 轮询间隔
        ))

    # ============================================================================
    # MQ：状态更新
    # ============================================================================
    async def _update_state(self, msg: Message):

        req_id = msg.params.get("req_id")
        device_id = msg.params.get('id')

        current = self.device[device_id].extra_parameter["current"]
        target = self.device[device_id].extra_parameter["target"]
        state = self.device[device_id].state

        await self.k.resources.upsert(Resource(
            resource_id=f"thermostat:{device_id}",
            kind="device",
            state={
                "current": current,
                "target": target,
                "state": state
            }
        ))

        await self.k.bus.publish(Event("evt.thermostat.state", {
            "id": device_id,
            "current": current,
            "target": target,
            "state": state,
            "req_id": req_id
        }))

        
        logger.info(
            f"[ThermostatPlugin/MQ] 状态 -> state={state}, "
            f"current={current}, target={target}"
        )

    # 用于从命令事件触发更新
    async def _mq_update_from_event(self, e: Event):
        await self.k.mq.put(Message(
            task="thermo.update",
            params={"id": e.payload.get("id")}
        ))

    # ============================================================================
    # 硬件模拟
    # ============================================================================
    def _sync_set_target(self, device_id, temp: float):
        res = self.device[device_id].sync_set_target(temp)
        if res:
            logger.info(f"[ThermostatPlugin] 设置目标温度为 {temp:.1f}°C")
        else:
            logger.warning(f"[ThermostatPlugin] 设置目标温度失败")

    def _poll_sensor_sync(self, device_id):
        temp = self.device[device_id].sync_get_current()
        return temp
    
    def _sync_turn_on(self, device_id):
        logger.info(f"[Thermostat Hardware] Turning ON {device_id}")
        if self.device[device_id].state == "off":
            res = self.device[device_id].sync_turn_on()
            if res:
                logger.info(f"[Thermostat Hardware] Turning ON {device_id} -> Success")
            else:
                logger.warning(f"[Thermostat Hardware] Turning ON {device_id} -> Failed")
        else:
            logger.warning(f"[Thermostat Hardware] Turning ON {device_id} -> Already ON")

    def _sync_turn_off(self, device_id):
        logger.info(f"[Thermostat Hardware] Turning OFF {device_id}")
        if self.device[device_id].state == "off":
            res = self.device[device_id].sync_turn_off()
            if res:
                logger.info(f"[Thermostat Hardware] Turning OFF {device_id} -> Success")
            else:
                logger.warning(f"[Thermostat Hardware] Turning OFF {device_id} -> Failed")
        else:
            logger.warning(f"[Thermostat Hardware] Turning OFF {device_id} -> Already OFF")

    async def _send_error(self, reason, payload):
        await self.k.bus.publish(Event("evt.thermostat.error", {
            "reason": reason,
            "req_id": payload.get("req_id")
        }))
