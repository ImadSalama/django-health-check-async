from async_health_check.constants import ServiceCodes
from async_health_check.services.base_service import BaseService
from async_health_check.services.cache_service import CacheHealthCheck
from async_health_check.services.database_usage_service import DatabaseUsageHealthCheck
from async_health_check.services.disk_usage_service import DiskUsageHealthCheck
from async_health_check.services.memory_usage_service import MemoryUsageHealthCheck


class ServiceFactory:
    SERVICE_MAP: dict[str, type(BaseService)] = {
        ServiceCodes.DISK_USAGE: DiskUsageHealthCheck,
        ServiceCodes.MEMORY_USAGE: MemoryUsageHealthCheck,
        ServiceCodes.DATABASE_USAGE: DatabaseUsageHealthCheck,
        ServiceCodes.CACHE: CacheHealthCheck,
    }

    @staticmethod
    def get_service(service_name, options):
        service = ServiceFactory.SERVICE_MAP.get(service_name)
        if not service:
            raise ValueError(f"Service {service_name} not found")
        return service(**options)
