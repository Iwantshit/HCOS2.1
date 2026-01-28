# ============================= main_2.py =============================
import asyncio, logging
from core.kernel import SmartHomeKernel
from plugins.device.light import LightPlugin
from plugins.device.thermostat import ThermostatPlugin
from plugins.sys.user_auth import UserAuthPlugin
from plugins.sys.event_logger import EventLoggerPlugin
from core.bus import Event
from core.plugins_base import Plugin, validate_plugin, read_yaml_file

logging.basicConfig(level=logging.INFO)

async def light_control_demo(k):
    """灯光控制演示：开灯3秒 → 关灯3秒 → 中等亮度3秒"""
    print("\n" + "="*50)
    print("开始灯光控制演示")
    print("="*50)
    
    ESP8266_IP = "192.168.100.176" 
    
    # 添加测试灯设备（通过API动态添加）
    print("1. 添加测试灯设备...")
    await k.bus.publish(Event(
        "cmd.light.add",
        {
            "id": "test_light",
            "name": "演示灯",
            "ip": ESP8266_IP,  # 使用变量
            "location": "演示房间",
            "state": "off",
            "brightness": 0
        }
    ))
    await asyncio.sleep(2)  # 等待设备添加完成
    
    # 第一阶段：打开灯（100%亮度）3秒
    print("\n第一阶段：开灯（100%亮度）3秒...")
    await k.bus.publish(Event(
        "cmd.light.on",
        {
            "id": "test_light",
            "brightness": 100  # 100%亮度
        }
    ))
    await asyncio.sleep(3)
    
    # 第二阶段：关灯3秒
    print("\n第二阶段：关灯3秒...")
    await k.bus.publish(Event(
        "cmd.light.off",
        {"id": "test_light"}
    ))
    await asyncio.sleep(3)
    
    # 第三阶段：打开灯（50%亮度）3秒
    print("\n第三阶段：开灯（50%亮度）3秒...")
    await k.bus.publish(Event(
        "cmd.light.brightness",
        {
            "id": "test_light",
            "brightness": 50  # 50%亮度
        }
    ))
    await asyncio.sleep(3)
    
    # 最后：关灯
    print("\n演示完成，关闭灯...")
    await k.bus.publish(Event(
        "cmd.light.off",
        {"id": "test_light"}
    ))
    
    print("\n" + "="*50)
    print("灯光控制演示完成")
    print("="*50)

async def main():
    key = b"MySecretAESKey!"
    k = SmartHomeKernel()
    await k.start()
    created_plugins = []

    # 系统插件
    v, msg = validate_plugin(UserAuthPlugin, is_sys_plugin=True)
    print(msg)
    if v:
        user_auth = UserAuthPlugin(db_path="data/userdb.enc", key=key)
        k.register_plugin_executor(user_auth)
        created_plugins.append(user_auth)

    v, msg = validate_plugin(EventLoggerPlugin, is_sys_plugin=True)
    print(msg)
    if v:
        event_logger = EventLoggerPlugin()
        k.register_plugin_executor(event_logger)
        created_plugins.append(event_logger)

    # 创建灯光插件
    print("正在初始化灯光插件...")
    
    # 根据 light.py 的 __init__ 方法选择正确的方式
    try:
        # 先尝试不传参数
        light = LightPlugin()
        v, msg = validate_plugin(LightPlugin)
    except TypeError as e:
        # 如果失败，尝试传空字典
        print(f"尝试不传参数失败，改为传递空字典: {e}")
        light = LightPlugin({})
        v, msg = validate_plugin(LightPlugin, config={})
    
    print(msg)
    if v:
        k.register_plugin_executor(light)
        created_plugins.append(light)

    # 温控器插件（可选）
    try:
        config = read_yaml_file(r"plugins\device\device_list.yaml")
        v, msg = validate_plugin(ThermostatPlugin, config=config.get(ThermostatPlugin.name, {}))
        print(msg)
        if v:
            thermo = ThermostatPlugin(config.get(ThermostatPlugin.name, {}))
            k.register_plugin_executor(thermo)
            created_plugins.append(thermo)
    except Exception as e:
        print(f"温控器插件加载失败: {e}")

    # 设置插件
    print("\n设置插件...")
    await light.setup(k)
    if 'thermo' in locals():
        await thermo.setup(k)
    await user_auth.setup(k)
    await event_logger.setup(k)
    
    print("启动插件...")
    await light.start()
    if 'thermo' in locals():
        await thermo.start()
    await user_auth.start()
    await event_logger.start() 
    
    # 等待插件完全启动
    await asyncio.sleep(2)
    
    # 执行灯光控制演示
    await light_control_demo(k)
    
    # 等待一会儿
    print("\n等待5秒...")
    await asyncio.sleep(5)
    
    # 停止插件
    print("\n正在停止插件...")
    for plugin in created_plugins:
        if plugin is None:
            continue
        try:
            await plugin.stop()
        except Exception as e:
            logging.error(f"停止插件 {getattr(plugin, 'name', 'unknown')} 时出错: {e}")
    
    await k.stop()
    print("系统已停止")

if __name__ == '__main__':
    asyncio.run(main())