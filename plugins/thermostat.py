# ============================= plugins/thermostat.py =============================
import asyncio, logging, time, random
from core.bus import Event
from core.resources import Resource
from core.message_queue import Message
logger=logging.getLogger(__name__)

class ThermostatPlugin:
    name = "thermostat"
    use_dedicated_threadpool = True  # 独立线程池（高事件量）

    async def setup(self, kernel):
        self.k = kernel
        await self.k.bus.subscribe("cmd.thermo.*", self._cmd)  # 注册命令回调

    async def start(self):
        self.k.spawn(self._poll_loop())
        logger.info("[thermostat] is Running")
        self.isrunning = True

    async def stop(self):
        logger.info("[thermostat] is Stopped")
        self.isrunning = False

    # ✅ 新增：命令处理函数
    async def _cmd(self, e: Event):
        target = e.payload.get("target")
        if target is not None:
            await self.k.run_in_plugin_executor(self, self._set_target_temp, float(target))

    # ✅ 实际执行逻辑（同步执行，放入独立线程池）
    def _set_target_temp(self, temp: float):
        time.sleep(0.3)  # 模拟阻塞调用
        logger.info(f"[thermostat] Target temperature set to {temp:.1f}°C")

    # 轮询任务
    async def _poll_loop(self):
        while self.isrunning:
            await self.k.run_in_plugin_executor(self, self._poll_sensor)
            await asyncio.sleep(0.5)  # 高频采样

    def _poll_sensor(self):
        time.sleep(1)
        temp = 20 + random.random() * 5
        logger.info(f"[thermostat] Current temperature {temp:.2f}°C")
        return temp
