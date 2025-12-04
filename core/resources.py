# ============================= core/resources.py =============================
import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from .cache import ttl_cache
from .bus import Event

@dataclass
class Resource:
    resource_id: str
    kind: str
    meta: Dict[str, Any] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)

class ResourceManager:
    def __init__(self, bus, cache):
        self._bus, self._cache = bus, cache
        self._res: Dict[str, Resource] = {}
        self._lock = asyncio.Lock()

    async def upsert(self, r: Resource):
        async with self._lock:
            self._res[r.resource_id] = r

        # 不阻塞调用方
        asyncio.create_task(self._cache.set(f"res:{r.resource_id}", r))
        asyncio.create_task(self._bus.publish(Event("res.upsert", {
            "id": r.resource_id,
            "kind": r.kind,
            "state": r.state,
            "meta": r.meta
        })))

    async def remove(self, rid: str):
        async with self._lock:
            self._res.pop(rid, None)
        await self._cache.delete(f"res:{rid}")
        await self._bus.publish(Event("res.remove", {"id": rid}))

    @ttl_cache(ttl=3)
    async def get(self, rid: str) -> Optional[Resource]:
        async with self._lock:
            return self._res.get(rid)

    async def list(self) -> List[Resource]:
        async with self._lock:
            return list(self._res.values())