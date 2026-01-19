# ============================= core/plugins_base.py =============================
from typing import Protocol, Awaitable

class Entity:
    # id: str
    # name: str
    # location: str
    # unique_id: str
    # state: str

    def __init__(self, id: str, name: str, location: str, unique_id: str, state: str, extra_parameter: dict, device_info: dict, ):
        """
        id: 设备在插件dict中的id
        name: 设备名称
        location: 设备位置
        unique_id: 设备唯一标识符
        state: 设备状态on/off
        extra_parameter: 设备额外的参数
        """
        self.id = id
        self.name = name
        self.location = location
        self.unique_id = unique_id
        self.state = state
        self.available = True
        self.extra_parameter = extra_parameter
        self.device_info = device_info
    
    def update_state(self, state: str):
        self.state = state

    def update_available(self, available: bool):
        self.available = available

        

class Plugin(Protocol):
    name: str 
    use_dedicated_threadpool: bool = False  # 默认不隔离
    is_collection: bool
    device: dict | dict[str, dict]  # 无论单设备还是集合，都使用device属性表示设备信息，单设备时为单个dict，集合时为id到dict的映射
    def __init__(self): ... # 构造函数
    async def setup(self, kernel) -> list[Entity]: ... # kernel 注入和部分内容的初始化
    async def start(self) -> None: ... # 启动插件（注册事件等）并告知系统设置为在线状态
    async def stop(self) -> None: ... # 停止插件（注销事件等）是否取消正在运行的任务视具体插件而定 并告知系统设置为离线状态
    async def _update_state(self, rid, state, req_id=None): ... # 更新信息系统中的设备状态并进行evt广播，仅对设备插件有效

# core/plugin_validator.py
import inspect
from typing import Mapping

def validate_plugin(plugin_cls, is_sys_plugin: bool = False, config: Mapping = {}):
    """验证插件类是否符合 Plugin 模板要求"""

    errors = []
    name = getattr(plugin_cls, 'name', '<unnamed>')
    # ---------- 1. 必须能实例化 ----------
    try:
        if is_sys_plugin:  # 系统插件
            instance = plugin_cls()
        else:  # 普通插件
            instance = plugin_cls(config)

    except Exception as e:
        errors.append(f"{name} 无法实例化: {e}")
        return False, errors

    # ---------- 2. 检查必要属性 ----------
    required_attrs = [
        "name",
        "use_dedicated_threadpool",
        # "is_collection",
        "device"
    ]

    for attr in required_attrs:
        if not hasattr(instance, attr):
            errors.append(f"{name} 缺少必要属性: {attr}")

    # ---------- 3. 检查 device 类型 ----------
    if hasattr(instance, "device"):
        dev = instance.device

        # device 必须是 dict 映射：id -> 状态dict
        if not isinstance(dev, Mapping):
            errors.append(f"{name}.device 必须是 dict（集合插件）")


    # ---------- 4. 检查必要方法 ----------
    required_methods = [
        "setup",
        "start",
        "stop",
        "_update_state",
    ]

    for method in required_methods:
        fn = getattr(instance, method, None)
        if fn is None:
            errors.append(f"{name} 缺少必要方法: {method}")
        elif not inspect.iscoroutinefunction(fn):
            errors.append(f"{name}.{method} 必须是 async 函数")

    # ---------- 5. 是否通过 ----------
    if errors:
        return False, errors
    print(f"{name} 验证通过！")
    return True, [f"{name} 验证通过！"]

def read_yaml_file(yaml_path:str):
    """
    读取yaml文件并返回数据
    yaml_path: yaml文件的路径
    """
    import yaml
    import logging

    logger = logging.getLogger(__name__)

    try:
        with open(yaml_path, "r", encoding="utf-8") as file:
            try:
                data = yaml.safe_load(file)
            except yaml.YAMLError as ye:
                msg = f"Failed to parse YAML file '{yaml_path}': {ye}"
                logger.error(msg)
                # 将解析错误以更具描述性的消息重新抛出，保留原始异常作为原因
                raise yaml.YAMLError(msg) from ye
        return data
    except FileNotFoundError as fnf:
        msg = f"YAML file not found: {yaml_path}"
        logger.error(msg)
        raise FileNotFoundError(msg) from fnf
    except PermissionError as pe:
        msg = f"Permission denied when reading YAML file: {yaml_path}"
        logger.error(msg)
        raise PermissionError(msg) from pe
    except Exception as e:
        # 广泛捕获以便为调用方提供有用的上下文信息
        msg = f"Unexpected error reading YAML file '{yaml_path}': {e}"
        logger.exception(msg)
        raise RuntimeError(msg) from e