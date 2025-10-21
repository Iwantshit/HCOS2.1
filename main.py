# ============================= main.py =============================
import asyncio, logging
from core.kernel import SmartHomeKernel
from plugins.light import LightPlugin
from plugins.thermostat import ThermostatPlugin
from plugins.user_auth import UserAuthPlugin
from plugins.event_logger import EventLoggerPlugin
from core.bus import Event

logging.basicConfig(level=logging.INFO)

async def main():
    key = b"MySecretAESKey!"
    k = SmartHomeKernel()
    light = LightPlugin()
    thermo = ThermostatPlugin()
    user_auth = UserAuthPlugin(db_path="data/userdb.enc", key=key)

    event_logger = EventLoggerPlugin()

    k.register_plugin_executor(light)
    k.register_plugin_executor(event_logger)
    k.register_plugin_executor(thermo)
    k.register_plugin_executor(user_auth)

    await light .setup(k)
    await thermo.setup(k)
    await user_auth.setup(k)
    await event_logger.setup(k)
    await k.start()

    # await light.start()
    # await thermo.start()
    await user_auth.start()
    await event_logger.start()

    # await asyncio.sleep(5)
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
    await user_auth.stop()
    await asyncio.sleep(1)

    await k.stop()


if __name__=='__main__':
    asyncio.run(main())