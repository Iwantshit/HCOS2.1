# plugins/user_auth.py
import asyncio
import hashlib
import logging
import json
import base64
import os
from typing import Dict
from core.bus import Event
from core.resources import Resource
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

logger = logging.getLogger(__name__)

# ==========================================================
# 轻量加密数据库：使用 AES + JSON 存储
# ==========================================================
class EncryptedUserDB:
    def __init__(self, path: str, key: bytes):
        self.path = path
        self.key = hashlib.sha256(key).digest()  # derive 32-byte key
        self.block_size = 16

    def _pad(self, data: bytes) -> bytes:
        pad_len = self.block_size - len(data) % self.block_size
        return data + bytes([pad_len]) * pad_len

    def _unpad(self, data: bytes) -> bytes:
        return data[:-data[-1]]

    def _encrypt(self, plaintext: str) -> bytes:
        iv = get_random_bytes(self.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        ciphertext = cipher.encrypt(self._pad(plaintext.encode()))
        return iv + ciphertext

    def _decrypt(self, ciphertext: bytes) -> str:
        iv, data = ciphertext[:self.block_size], ciphertext[self.block_size:]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        plaintext = self._unpad(cipher.decrypt(data))
        return plaintext.decode()

    def save(self, users: Dict[str, Dict]):
        try:
            text = json.dumps(users)
            data = self._encrypt(text)
            with open(self.path, "wb") as f:
                f.write(data)
            logger.info("[UserDB] 数据库保存成功")
        except Exception as e:
            logger.exception("[UserDB] 保存失败: %s", e)

    def load(self) -> Dict[str, Dict]:
        if not os.path.exists(self.path):
            return {}
        try:
            with open(self.path, "rb") as f:
                data = f.read()
            text = self._decrypt(data)
            return json.loads(text)
        except Exception as e:
            logger.exception("[UserDB] 读取失败: %s", e)
            return {}
# ==========================================================


class UserAuthPlugin:
    name = "user_auth"
    use_dedicated_threadpool = False  # 不隔离线程池
    device = {}
    is_collection = False

    def __init__(self, db_path="userdb.enc", key: bytes = b"DefaultSecretKey"):
        self.k = None
        self._users: Dict[str, Dict] = {}
        self.db = EncryptedUserDB(db_path, key)
        self.super_admin = {
            "username": "root",
            "phone": "00000000000",
            "role": "super_admin"
        }

    async def setup(self, kernel):
        self.k = kernel
        self._users = self.db.load()
        await self.k.resources.upsert(Resource(
            resource_id=f"user:{self.super_admin['phone']}",
            kind="account",
            state=self.super_admin
        ))
        # 同步用户到资源管理器
        for phone, info in self._users.items():
            await self.k.resources.upsert(Resource(
                resource_id=f"user:{phone}",
                kind="account",
                state=info
            ))
        logger.info(f"[UserAuth] 已加载 {len(self._users)} 个用户")
        logger.info("[UserAuth] setup complete")

    async def start(self):
        # 从数据库读取用户信息
        await self.k.bus.subscribe("cmd.user.register", self._handle_register)
        await self.k.bus.subscribe("cmd.user.login", self._handle_login)
        await self.k.bus.subscribe("cmd.user.set_role", self._handle_set_role)
        await self.k.bus.subscribe("cmd.user.list", self._handle_list_users)
        logger.info("[UserAuth] start complete")

    async def stop(self):
        # 停止前保存用户数据库
        self.db.save(self._users)
        logger.info("[UserAuth] 用户数据库已保存")

    # ------------------ 用户注册 ------------------
    async def _handle_register(self, e: Event):
        req_id = e.payload.get("req_id")
        try:
            username = e.payload.get("username")
            phone = e.payload.get("phone")

            # 参数校验
            if not username or not phone:
                raise ValueError("缺少参数")

            if phone == self.super_admin["phone"]:
                raise PermissionError("该手机号为超级管理员")

            if phone in self._users:
                raise ValueError("手机号已注册")

            # 注册新用户
            user_info = {
                "username": username,
                "phone": phone,
                "role": "user",
                "password_hash": self._hash_password(phone)
            }
            self._users[phone] = user_info
            self.db.save(self._users)

            # 更新资源
            await self.k.resources.upsert(Resource(
                resource_id=f"user:{phone}",
                kind="account",
                state=user_info
            ))

            # 成功事件
            await self.k.bus.publish(Event("evt.user.register_success", {
                "status": "ok",
                "data": {"phone": phone, "role": "user"},
                "req_id": req_id
            }))
            logger.info(f"[UserAuth] 注册成功：{phone}")

        except Exception as ex:
            logger.exception(f"[UserAuth] 注册失败: {ex}")
            await self.k.bus.publish(Event("evt.user.register_failed", {
                "status": "error",
                "reason": str(ex),
                "req_id": req_id
            }))

    # ------------------ 用户登录 ------------------
    async def _handle_login(self, e: Event):
        req_id = e.payload.get("req_id")
        try:
            phone = e.payload.get("phone")
            password = e.payload.get("password", phone)

            if not phone:
                raise ValueError("缺少手机号")

            # 超级管理员登录
            if phone == self.super_admin["phone"]:
                if password == phone:
                    await self.k.bus.publish(Event("evt.user.login_success", {
                        "status": "ok",
                        "data": {"phone": phone, "role": "super_admin"},
                        "req_id": req_id
                    }))
                    logger.info("[UserAuth] 超级管理员登录成功")
                    return
                else:
                    raise PermissionError("超级管理员密码错误")

            user = self._users.get(phone)
            if not user:
                raise ValueError("用户不存在")

            if user["password_hash"] != self._hash_password(password):
                raise ValueError("密码错误")

            # 登录成功
            await self.k.bus.publish(Event("evt.user.login_success", {
                "status": "ok",
                "data": {"phone": phone, "role": user["role"]},
                "req_id": req_id
            }))
            logger.info(f"[UserAuth] 登录成功：{phone}")

        except Exception as ex:
            logger.exception(f"[UserAuth] 登录失败: {ex}")
            await self.k.bus.publish(Event("evt.user.login_failed", {
                "status": "error",
                "reason": str(ex),
                "req_id": req_id
            }))

    # ------------------ 权限调整 ------------------
    async def _handle_set_role(self, e: Event):
        req_id = e.payload.get("req_id")
        try:
            operator = e.payload.get("operator")
            target = e.payload.get("target")
            new_role = e.payload.get("role")

            # 权限与参数校验
            if not operator or not target or not new_role:
                raise ValueError("缺少参数")

            if operator != self.super_admin["phone"]:
                raise PermissionError("无权限")

            if new_role not in ("admin", "user"):
                raise ValueError("角色非法")

            if target not in self._users:
                raise ValueError("目标用户不存在")

            # 修改角色
            self._users[target]["role"] = new_role
            self.db.save(self._users)
            await self.k.resources.upsert(Resource(
                resource_id=f"user:{target}",
                kind="account",
                state=self._users[target]
            ))

            # 成功事件
            await self.k.bus.publish(Event("evt.user.role_change_success", {
                "status": "ok",
                "data": {"target": target, "new_role": new_role},
                "req_id": req_id
            }))
            logger.info(f"[UserAuth] {target} 权限变更为 {new_role}")

            # 系统广播（可选）
            await self.k.bus.publish(Event("evt.user.role_changed", {
                "target": target,
                "new_role": new_role,
                "req_id": req_id
            }))

        except Exception as ex:
            logger.exception(f"[UserAuth] 权限修改失败: {ex}")
            await self.k.bus.publish(Event("evt.user.role_change_failed", {
                "status": "error",
                "reason": str(ex),
                "req_id": req_id
            }))

    # ------------------ 用户列表查询（仅管理员及超级管理员） ------------------
    async def _handle_list_users(self, e: Event):
        req_id = e.payload.get("req_id")
        try:
            operator = e.payload.get("operator")
            page = int(e.payload.get("page", 1))
            limit = int(e.payload.get("limit", 20))
            start = (page - 1) * limit
            end = start + limit

            # ========== 权限校验 ==========
            # 超级管理员直接通过
            if operator == self.super_admin["phone"]:
                role = "super_admin"
            else:
                # 从用户数据库中查找操作者
                user = self._users.get(operator)
                if not user:
                    raise PermissionError("无效的操作用户")
                role = user["role"]

            # 检查权限
            if role not in ("admin", "super_admin"):
                raise PermissionError("权限不足，仅管理员可查看用户列表")

            # ========== 执行查询 ==========
            users = list(self._users.values())[start:end]

            await self.k.bus.publish(Event("evt.user.list_success", {
                "status": "ok",
                "data": users,
                "page": page,
                "total": len(self._users),
                "req_id": req_id
            }))
            logger.info(f"[UserAuth] 用户列表请求成功：操作者 {operator} ({role}) 共 {len(self._users)} 用户")

        except Exception as ex:
            logger.exception(f"[UserAuth] 用户列表请求失败: {ex}")
            await self.k.bus.publish(Event("evt.user.list_failed", {
                "status": "error",
                "reason": str(ex),
                "req_id": req_id
            }))
    # ------------------ 工具函数 ------------------
    def _hash_password(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    async def _update_state(self, rid, state, req_id=None):
        pass