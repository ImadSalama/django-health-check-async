import socket

from async_health_check.exceptions import ServiceWarning, ServiceReturnedUnexpectedResult
from async_health_check.services.base_service import BaseService
import psutil


class DiskUsageHealthCheck(BaseService):
    SERVICE_CODE = "DISK_USAGE"
    SERVICE_NAME = "Disk Usage"

    def __init__(self, disk_usage_max: int, critical_service: bool = True):
        super(DiskUsageHealthCheck, self).__init__(critical_service)
        self.disk_usage_max = disk_usage_max

    async def _run_check(self):
        try:
            host = socket.gethostname()
            du = psutil.disk_usage("/")
            if self.disk_usage_max and du.percent >= self.disk_usage_max:
                raise ServiceWarning(
                    "{host} {percent}% disk usage exceeds {disk_usage}%".format(
                        host=host, percent=du.percent, disk_usage=self.disk_usage_max
                    )
                )
        except ValueError as e:
            self.add_error(ServiceReturnedUnexpectedResult("ValueError"), e)
