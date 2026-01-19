# ============================= main.py =============================
import asyncio, logging
from core.kernel import SmartHomeKernel
from plugins.device.light import LightPlugin
from plugins.device.thermostat import ThermostatPlugin
from plugins.sys.user_auth import UserAuthPlugin
from plugins.sys.event_logger import EventLoggerPlugin
from core.bus import Event
from core.plugins_base import Plugin, validate_plugin, read_yaml_file

logging.basicConfig(level=logging.INFO)
        
async def main():
    key = b"MySecretAESKey!"
    k = SmartHomeKernel()
    await k.start()
    created_plugins = []

    # 系统插件 （其实不需要自检
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

    # 设备列表配置（未来可扩展为动态加载插件 & 设备）
    config = read_yaml_file(r"plugins\device\device_list.yaml") # 读取设备配置

    v, msg = validate_plugin(LightPlugin, config=config[LightPlugin.name])
    print(msg)
    if v:
        light = LightPlugin(config[LightPlugin.name])
        k.register_plugin_executor(light)
        created_plugins.append(light)

    v, msg = validate_plugin(ThermostatPlugin, config=config[ThermostatPlugin.name])
    print(msg)
    if v:
        thermo = ThermostatPlugin(config[ThermostatPlugin.name])
        k.register_plugin_executor(thermo)
        created_plugins.append(thermo)



    await light.setup(k)
    await thermo.setup(k)
    await user_auth.setup(k)
    await event_logger.setup(k)
    
    await light.start()
    await thermo.start()
    await user_auth.start()
    await event_logger.start()

    # await k.bus.publish(Event("cmd.thermostat.on", {}))
    # await asyncio.sleep(100)
    # await light.stop()
    # await thermo.stop()


    # 用户注册
    await k.bus.publish(Event("cmd.user.register", {"username": "Alice", "phone": "13800138000"}, priority=6))
    # 用户登录
    # await asyncio.sleep(10)
    await k.bus.publish(Event("cmd.user.login", {"phone": "13800138000", "password": "13800138000"}, priority=6))
    # await asyncio.sleep(1)
    # 超级管理员修改权限
    await k.bus.publish(Event("cmd.user.set_role", {
        "operator": "00000000000",
        "target": "13800138000",
        "role": "admin"
    }))
    
    ok, res = await k.bus.request(
        "cmd.user.list",
        {"operator":"00000000000", "page": 1, "limit": 20},
        "evt.user.list_success",
        "evt.user.list_failed"
    )
    if ok:
        print(res)
    else:
        print(res)
    
    # tasks = k.bus.publish_async(Event("cmd.thermostat.on", {'id': 'thermo_001'})) 
    tasks = k.bus.publish_async(Event("cmd.light.on", {"id": "light_001"})) 
    # await asyncio.gather(*tasks) 
    tasks = k.bus.publish_async(Event("cmd.light.off", {"id": "light_002"}))
    # await asyncio.gather(*tasks) 
    await asyncio.sleep(4)
    tasks = await k.bus.publish(Event("cmd.light.toggle", {"id": "light_001"}))
    await asyncio.gather(*tasks) 
    tasks = await k.bus.publish(Event("cmd.light.toggle", {"id": "light_003"}))
    await asyncio.gather(*tasks) 
    tasks = await k.bus.publish(Event("cmd.light.state", {}))
    await asyncio.gather(*tasks) 

    # tasks = await k.bus.publish(Event("cmd.thermostat.on", {'id': 'thermo_001'})) 
    
    # await asyncio.gather(*tasks) 
    tasks = k.bus.publish_async(Event("cmd.thermostat.on", {'id': 'thermo_001'})) 
    await asyncio.sleep(20)
    logger = logging.getLogger(__name__)

    # 依次停止所有已经创建并注册的插件，按创建顺序停止
    for plugin in created_plugins:
        if plugin is None:
            continue
        stop_fn = getattr(plugin, "stop", None)
        if stop_fn is None:
            continue
        try:
            # 支持 async stop 方法
            await stop_fn()
            logger.info("Stopped plugin: %s", getattr(plugin, "name", repr(plugin)))
        except Exception as e:
            logger.exception("Error while stopping plugin %s: %s", getattr(plugin, "name", repr(plugin)), e)

    await asyncio.sleep(10)

    await k.stop()


if __name__=='__main__':
    asyncio.run(main())