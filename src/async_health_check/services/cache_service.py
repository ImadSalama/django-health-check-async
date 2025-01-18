from django.core.cache import CacheKeyWarning, caches
import secrets
import string
from async_health_check.exceptions import ServiceReturnedUnexpectedResult, ServiceUnavailable
from async_health_check.services.base_service import BaseService
from redis.exceptions import RedisError


class CacheHealthCheck(BaseService):
    SERVICE_CODE = "CACHE"
    SERVICE_NAME = "Cache"

    def __init__(self, caches: dict[str], critical_service: bool = True):
        super(CacheHealthCheck, self).__init__(critical_service)
        self.cache_key = self.generate_cache_key()
        self.caches = caches

    @staticmethod
    def generate_cache_key(length=16):
        characters = string.ascii_letters + string.digits
        return ''.join(secrets.choice(characters) for _ in range(length))

    async def _run_check(self):
        for cache in self.caches:
            cache = caches[cache['BACKEND']]
            try:
                cache.set(self.cache_key, "checked")
                if not cache.get(self.cache_key) == "checked":
                    raise ServiceUnavailable(f"Cache key {self.cache_key} does not match")
            except CacheKeyWarning as e:
                self.add_error(ServiceReturnedUnexpectedResult("Cache key warning"), e)
            except ValueError as e:
                self.add_error(ServiceReturnedUnexpectedResult("ValueError"), e)
            except (ConnectionError, RedisError) as e:
                self.add_error(ServiceReturnedUnexpectedResult("Connection Error"), e)
