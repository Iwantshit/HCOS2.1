# ============================= core/cache.py =============================
import asyncio, time, functools
from collections import deque

class TTLCache:
    def __init__(self, maxsize=512, ttl=30.0):
        self.maxsize = maxsize
        self.ttl = ttl
        self._store, self._order = {}, deque()
        self._lock = asyncio.Lock()

    async def get(self, key):
        async with self._lock:
            val = self._store.get(key)
            if not val: return None
            ts, data = val
            if time.time()-ts > self.ttl:
                self._store.pop(key, None)
                return None
            self._order.remove(key)
            self._order.append(key)
            return data

    async def set(self, key, value):
        async with self._lock:
            if len(self._store)>=self.maxsize:
                oldest=self._order.popleft();self._store.pop(oldest,None)
            self._store[key]=(time.time(),value);self._order.append(key)

def ttl_cache(ttl=10.0):
    def deco(fn):
        cache=TTLCache(ttl=ttl)
        @functools.wraps(fn)
        async def wrapper(*a,**kw):
            k=f"{fn.__name__}:{a}:{tuple(sorted(kw.items()))}"
            v=await cache.get(k)
            if v is not None: return v
            r=await fn(*a,**kw)
            await cache.set(k,r)
            return r
        return wrapper
    return deco