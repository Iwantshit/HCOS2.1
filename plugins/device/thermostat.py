# ============================= plugins/thermostat.py =============================
import asyncio, logging, time, random
from core.bus import Event
from core.resources import Resource
from core.message_queue import Message

logger = logging.getLogger(__name__)

class ThermostatPlugin:
    name = "thermostat"
    use_dedicated_threadpool = True

    def __init__(self, config):
        super().__init__()

        self.is_collection = False
        self._config = config
        self._subscriptions = []

        # 加载设备配置
        self.device = {}
        for device in config["device"]:
            for k, v in device.items():
                logger.info(f"[ThermostatPlugin] 加载设备 {k}: {v}")
                self.device[k] = v

        # 最关键：由 MQ 控制轮询逻辑
        self.isrunning = False

    # ---------------------------------------------
    # setup
    # ---------------------------------------------
    async def setup(self, kernel):
        self.k = kernel

        # 注册 MQ 任务
        await self.k.mq.register("thermo.poll", self._mq_poll)
        await self.k.mq.register("thermo.update", self._update_state)

        logger.info("[ThermostatPlugin] 硬件自检中...")
        await asyncio.sleep(0.2)
        logger.info("[ThermostatPlugin] 已初始化")

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

        await self._subscribe("cmd.thermostat.on", self._cmd_on)
        await self._subscribe("cmd.thermostat.off", self._cmd_off)
        await self._subscribe("cmd.thermostat.set", self._cmd_set)
        await self._subscribe("cmd.thermostat.status", self._cmd_status)

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
        if self.device["state"] == "on":
            return

        self.device["state"] = "on"
        self.isrunning = True

        # 直接投递第一次轮询任务
        await self.k.mq.put(Message(
            task="thermo.poll",
            params={"id": self.device["id"]},
            delay_sec=0.0
        ))

        await self._mq_update_from_event(e)
        logger.info("[ThermostatPlugin] 已开启（MQ轮询启动）")

    async def _cmd_off(self, e: Event):
        if self.device["state"] == "off":
            return

        self.device["state"] = "off"
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

    async def _cmd_status(self, e: Event):
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
        temp = await self.k.run_in_plugin_executor(self, self._poll_sensor_sync)
        self.device["current"] = temp

        # 投递状态更新任务
        await self.k.mq.put(Message(
            task="thermo.update",
            params={"req_id": None}
        ))

        # 再次投递下一次轮询
        await self.k.mq.put(Message(
            task="thermo.poll",
            params={"id": self.device["id"]},
            delay_sec=0.5  # 轮询间隔
        ))

    # ============================================================================
    # MQ：状态更新
    # ============================================================================
    async def _update_state(self, msg: Message):

        req_id = msg.params.get("req_id")

        await self.k.resources.upsert(Resource(
            resource_id=f"thermostat:{self.device['id']}",
            kind="device",
            state={
                "current": self.device["current"],
                "target": self.device["target"],
                "state": self.device["state"]
            }
        ))

        await self.k.bus.publish(Event("evt.thermostat.state", {
            "id": self.device["id"],
            "current": self.device["current"],
            "target": self.device["target"],
            "state": self.device["state"],
            "req_id": req_id
        }))

        logger.info(
            f"[ThermostatPlugin/MQ] 状态 -> state={self.device['state']}, "
            f"current={self.device['current']}, target={self.device['target']}"
        )

    # 用于从命令事件触发更新
    async def _mq_update_from_event(self, e: Event):
        await self.k.mq.put(Message(
            task="thermo.update",
            params={"req_id": e.payload.get("req_id")}
        ))

    # ============================================================================
    # 硬件模拟
    # ============================================================================
    def _sync_set_target(self, temp: float):
        time.sleep(0.3)
        logger.info(f"[ThermostatPlugin] 设置目标温度为 {temp:.1f}°C")

    def _poll_sensor_sync(self):
        time.sleep(1)
        return 20 + random.random() * 5

    async def _send_error(self, reason, payload):
        await self.k.bus.publish(Event("evt.thermostat.error", {
            "reason": reason,
            "req_id": payload.get("req_id")
        }))
