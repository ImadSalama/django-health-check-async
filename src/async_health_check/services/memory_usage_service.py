import locale
from async_health_check.exceptions import ServiceWarning, ServiceReturnedUnexpectedResult
from async_health_check.services.base_service import BaseService
import psutil
import socket


class MemoryUsageHealthCheck(BaseService):
    SERVICE_CODE = "MEMORY_USAGE"
    SERVICE_NAME = "Memory Usage"

    def __init__(self, memory_min: int, critical_service: bool = True):
        super(MemoryUsageHealthCheck, self).__init__(critical_service)
        self.memory_min = memory_min

    async def _run_check(self):
        try:
            memory = psutil.virtual_memory()
            host = socket.gethostname()
            if self.memory_min and memory.available < (self.memory_min * 1024 * 1024):
                locale.setlocale(locale.LC_ALL, "")
                avail = "{:n}".format(int(memory.available / 1024 / 1024))
                threshold = "{:n}".format(self.memory_min)
                raise ServiceWarning(
                    "{host} {avail} MB available RAM below {threshold} MB".format(
                        host=host, avail=avail, threshold=threshold
                    )
                )
        except ValueError as e:
            self.add_error(ServiceReturnedUnexpectedResult("ValueError"), e)

