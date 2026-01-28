# plugins/light.py
import asyncio
import logging
import aiohttp
import socket
from typing import Dict, Optional, Any
from core.bus import Event
from core.resources import Resource
from core.plugins_base import Plugin, Entity

logger = logging.getLogger(__name__)

class LightEntity(Entity):
    """ESP8266 网络灯光设备"""
    
    def __init__(
            self, 
            id: str, 
            name: str, 
            location: str, 
            unique_id: str, 
            state: str, 
            extra_parameter: dict,
            device_info: dict,
            brightness: int = 0
    ):
        super().__init__(id, name, location, unique_id, state, extra_parameter, device_info)
        self.brightness = brightness
        self.ip = extra_parameter.get('ip', '')
        self.online = False
        self.last_seen = 0


class LightPlugin(Plugin):
    """ESP8266 网络灯光插件"""
    
    name = "light"
    use_dedicated_threadpool = False
    is_collection = True
    device = {}

    def __init__(self, config=None):  # 修改这里：config 参数为可选
        # 如果系统传递了配置就使用，否则用空字典
        self.config = config if config is not None else {}
        self._entities: Dict[str, LightEntity] = {}  # 设备ID -> LightEntity
        self._session: Optional[aiohttp.ClientSession] = None
        self._subscriptions = []
        self._poll_task = None
        self._discovery_task = None
        
        # 配置参数（使用默认值或配置中的值）
        self.poll_interval = self.config.get("poll_interval", 10)
        self.discovery_interval = self.config.get("discovery_interval", 60)
        self.http_timeout = self.config.get("http_timeout", 3)
        
        # 初始化设备字典（为了兼容性）
        self.device = {}

    async def setup(self, kernel):
        """初始化插件"""
        self.k = kernel
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.http_timeout)
        )
        
        logger.info("[LightPlugin] 插件初始化完成")
        return []  # 返回空列表，通过发现或API添加设备

    async def start(self):
        """启动插件"""
        logger.info("[LightPlugin] 正在启动插件...")
        
        # 订阅命令
        await self._subscribe("cmd.light.on", self._cmd_on)
        await self._subscribe("cmd.light.off", self._cmd_off)
        await self._subscribe("cmd.light.brightness", self._cmd_brightness)
        await self._subscribe("cmd.light.toggle", self._cmd_toggle)
        await self._subscribe("cmd.light.state", self._cmd_state)
        await self._subscribe("cmd.light.add", self._cmd_add_device)  # 添加设备的命令
        await self._subscribe("cmd.light.remove", self._cmd_remove_device)  # 移除设备的命令
        
        # 启动设备状态轮询
        self._poll_task = asyncio.create_task(self._poll_devices())

        # 更新插件状态
        await self.k.resources.upsert(Resource(
            resource_id="plugin:light",
            kind="plugin",
            state={"available": True}
        ))

        logger.info("[LightPlugin] 插件启动完成")

    async def stop(self):
        """停止插件"""
        logger.info("[LightPlugin] 正在停止插件...")
        
        # 取消订阅
        for topic, callback in self._subscriptions:
            await self.k.bus.unsubscribe(topic, callback)
        
        # 取消任务
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        
        # 关闭会话
        if self._session:
            await self._session.close()
        
        logger.info("[LightPlugin] 插件已停止")

    async def _subscribe(self, topic: str, callback):
        """订阅事件"""
        await self.k.bus.subscribe(topic, callback)
        self._subscriptions.append((topic, callback))

    # ==================== 设备管理 ====================
    async def _cmd_add_device(self, e: Event):
        """添加新设备"""
        device_config = e.payload
        
        # 必要字段检查
        required_fields = ["id", "name", "ip"]
        for field in required_fields:
            if field not in device_config:
                await self.k.bus.publish(Event(
                    "evt.light.error",
                    {
                        "reason": f"missing_field_{field}",
                        "message": f"缺少必要字段: {field}",
                        "req_id": e.payload.get("req_id")
                    }
                ))
                return
        
        device_id = device_config["id"]
        
        # 检查设备是否已存在
        if device_id in self._entities:
            await self.k.bus.publish(Event(
                "evt.light.error",
                {
                    "reason": "device_exists",
                    "message": f"设备已存在: {device_id}",
                    "device_id": device_id,
                    "req_id": e.payload.get("req_id")
                }
            ))
            return
        
        # 创建设备实体
        entity = LightEntity(
            id=device_id,
            name=device_config["name"],
            location=device_config.get("location", ""),
            unique_id=device_config.get("unique_id", device_id),
            state=device_config.get("state", "off"),
            brightness=device_config.get("brightness", 0),
            extra_parameter={
                "ip": device_config["ip"],
                **device_config.get("extra_parameter", {})
            },
            device_info=device_config.get("device_info", {})
        )
        
        # 添加到管理
        self._entities[device_id] = entity
        self.device[device_id] = entity.__dict__.copy()
        
        logger.info(f"[LightPlugin] 添加设备: {entity.name} (ID: {device_id}, IP: {entity.ip})")
        
        # 发布事件
        await self.k.bus.publish(Event(
            "evt.light.device_added",
            {
                "device_id": device_id,
                "name": entity.name,
                "ip": entity.ip,
                "timestamp": asyncio.get_event_loop().time(),
                "req_id": e.payload.get("req_id")
            }
        ))

    async def _cmd_remove_device(self, e: Event):
        """移除设备"""
        device_id = e.payload.get("id")
        
        if device_id not in self._entities:
            await self.k.bus.publish(Event(
                "evt.light.error",
                {
                    "reason": "device_not_found",
                    "message": f"设备未找到: {device_id}",
                    "device_id": device_id,
                    "req_id": e.payload.get("req_id")
                }
            ))
            return
        
        # 移除设备
        entity = self._entities.pop(device_id)
        self.device.pop(device_id, None)
        
        logger.info(f"[LightPlugin] 移除设备: {entity.name} (ID: {device_id})")
        
        # 发布事件
        await self.k.bus.publish(Event(
            "evt.light.device_removed",
            {
                "device_id": device_id,
                "name": entity.name,
                "timestamp": asyncio.get_event_loop().time(),
                "req_id": e.payload.get("req_id")
            }
        ))

    # ==================== 设备控制 ====================
    async def _cmd_on(self, e: Event):
        """开灯"""
        await self._control_device(e, "on")

    async def _cmd_off(self, e: Event):
        """关灯"""
        await self._control_device(e, "off")

    async def _cmd_brightness(self, e: Event):
        """调整亮度"""
        device_id = e.payload.get("id")
        brightness = e.payload.get("brightness")
        
        if brightness is None:
            await self._send_error("缺少亮度参数", device_id, e.payload.get("req_id"))
            return
        
        await self._control_device(e, "brightness", brightness)

    async def _cmd_toggle(self, e: Event):
        """切换状态"""
        device_id = e.payload.get("id")
        
        if device_id not in self._entities:
            await self._send_error("设备未找到", device_id, e.payload.get("req_id"))
            return
        
        entity = self._entities[device_id]
        action = "off" if entity.state == "on" else "on"
        
        await self._control_device(e, action)

    async def _cmd_state(self, e: Event):
        """获取设备状态"""
        device_id = e.payload.get("id")
        
        if device_id:
            # 单个设备
            if device_id not in self._entities:
                await self._send_error("设备未找到", device_id, e.payload.get("req_id"))
                return
            
            entity = self._entities[device_id]
            await self._publish_state(entity, e.payload.get("req_id"))
        else:
            # 所有设备
            all_states = []
            for entity in self._entities.values():
                all_states.append({
                    "id": entity.id,
                    "name": entity.name,
                    "state": entity.state,
                    "brightness": entity.brightness,
                    "online": entity.online,
                    "ip": entity.ip
                })
            
            await self.k.bus.publish(Event(
                "evt.light.all_states",
                {
                    "devices": all_states,
                    "count": len(all_states),
                    "req_id": e.payload.get("req_id")
                }
            ))

    # ==================== 核心控制逻辑 ====================
    async def _control_device(self, e: Event, action: str, value=None):
        """控制设备的核心方法"""
        device_id = e.payload.get("id")
        
        # 验证设备
        if not device_id:
            await self._send_error("缺少设备ID", None, e.payload.get("req_id"))
            return
        
        if device_id not in self._entities:
            await self._send_error("设备未找到", device_id, e.payload.get("req_id"))
            return
        
        entity = self._entities[device_id]
        
        # 检查设备IP
        if not entity.ip:
            await self._send_error("设备没有IP地址", device_id, e.payload.get("req_id"))
            return
        
        try:
            # 构建HTTP请求URL
            if action == "on":
                url = f"http://{entity.ip}/light?state=on"
                if value:
                    url += f"&brightness={value}"
            elif action == "off":
                url = f"http://{entity.ip}/light?state=off"
            elif action == "brightness":
                url = f"http://{entity.ip}/light?brightness={value}"
                action = "on"  # 设置亮度意味着开灯
            else:
                await self._send_error(f"无效的操作: {action}", device_id, e.payload.get("req_id"))
                return
            
            # 发送请求
            logger.info(f"[LightPlugin] 控制设备: {entity.name} -> {action}")
            
            async with self._session.get(url) as response:
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                
                # 更新设备状态
                if action == "on":
                    entity.state = "on"
                    entity.brightness = value if value else 100
                elif action == "off":
                    entity.state = "off"
                    entity.brightness = 0
                
                entity.online = True
                
                # 发布状态更新
                await self._publish_state(entity, e.payload.get("req_id"))
                
        except Exception as err:
            logger.error(f"[LightPlugin] 控制失败: {entity.name} -> {err}")
            await self._send_error(f"控制失败: {str(err)}", device_id, e.payload.get("req_id"))
            entity.online = False

    async def _poll_devices(self):
        """设备状态轮询"""
        while True:
            try:
                for entity in self._entities.values():
                    if entity.ip:
                        await self._check_device_status(entity)
                
                await asyncio.sleep(self.poll_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[LightPlugin] 轮询出错: {e}")
                await asyncio.sleep(self.poll_interval)

    async def _check_device_status(self, entity: LightEntity):
        """检查设备状态"""
        try:
            # 使用 /api 接口而不是 /status 接口
            url = f"http://{entity.ip}/api"
            async with self._session.get(url) as response:
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                
                data = await response.json()
                light_data = data.get("light", {})
                
                # 更新状态
                entity.state = "on" if light_data.get("state") else "off"
                entity.brightness = light_data.get("brightness", 0)
                entity.online = True
                
                logger.debug(f"设备状态更新成功: {entity.name} -> {entity.state} (亮度: {entity.brightness})")
                    
        except Exception as e:
            logger.warning(f"设备状态更新失败: {entity.name} -> {e}")
            entity.online = False

    # ==================== 辅助方法 ====================
    async def _publish_state(self, entity: LightEntity, req_id=None):
        """发布设备状态"""
        await self.k.bus.publish(Event(
            "evt.light.state",
            {
                "id": entity.id,
                "name": entity.name,
                "state": entity.state,
                "brightness": entity.brightness,
                "online": entity.online,
                "ip": entity.ip,
                "req_id": req_id
            }
        ))

    async def _send_error(self, message: str, device_id: str = None, req_id=None):
        """发送错误消息"""
        error_data = {
            "message": message,
            "req_id": req_id
        }
        
        if device_id:
            error_data["id"] = device_id
        
        await self.k.bus.publish(Event(
            "evt.light.error",
            error_data
        ))