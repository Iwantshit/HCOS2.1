# core/kernel.py
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from core.bus import InternalBus
from core.cache import TTLCache
from core.resources import ResourceManager
from core.message_queue import MessageQueue
from core.plugins_base import Plugin

logger = logging.getLogger(__name__)

class SmartHomeKernel:
    def __init__(self):
        # ==== 系统核心组件 ====
        self.bus = InternalBus()             # 内部事件总线（带优先队列）
        self.cache = TTLCache()              # 缓存系统
        self.resources = ResourceManager(self.bus, self.cache)
        self.mq = MessageQueue()             # 异步消息队列

        # ==== 异步运行时 ====
        self._bg_tasks = []                  # 后台协程列表
        self.global_executor = ThreadPoolExecutor(max_workers=10)
        self._plugin_executors = {}          # plugin.name -> ThreadPoolExecutor

    # ======================================================
    # 启动与停止
    # ======================================================
    async def start(self):
        # 启动总线分发循环
        await self.bus.start()          # 🌟 启动事件优先队列分发协程
        await self.mq.start()           # 启动消息队列
        logger.info("Kernel started")

    async def stop(self):
        # 停止总线分发
        await self.bus.stop()
        # 关闭线程池
        for pool in self._plugin_executors.values():
            pool.shutdown(wait=False)
        self.global_executor.shutdown(wait=False)
        logger.info("Kernel stopped")

    # ======================================================
    # 协程与任务管理
    # ======================================================
    def spawn(self, coro):
        """启动后台协程（自动管理生命周期）"""
        task = asyncio.create_task(coro)
        self._bg_tasks.append(task)
        return task

    # ======================================================
    # 插件执行器管理
    # ======================================================
    def register_plugin_executor(self, plugin: Plugin):
        """为插件分配独立或共享线程池"""
        if plugin.use_dedicated_threadpool:
            executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"{plugin.name}-pool")
            self._plugin_executors[plugin.name] = executor
            logger.info(f"Plugin {plugin.name} runs in dedicated threadpool")
        else:
            logger.info(f"Plugin {plugin.name} uses global threadpool")

    async def run_in_plugin_executor(self, plugin: Plugin, fn, *args):
        """执行阻塞任务（自动选择线程池）"""
        loop = asyncio.get_running_loop()
        executor = (
            self._plugin_executors.get(plugin.name)
            if plugin.use_dedicated_threadpool
            else self.global_executor
        )
        return await loop.run_in_executor(executor, fn, *args)
