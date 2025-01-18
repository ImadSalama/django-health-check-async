from django.db import IntegrityError, DatabaseError, connections

from async_health_check.exceptions import ServiceReturnedUnexpectedResult, ServiceUnavailable
from async_health_check.services.base_service import BaseService


class DatabaseUsageHealthCheck(BaseService):
    SERVICE_CODE = "DATABASE_USAGE"
    SERVICE_NAME = "Database Usage"

    def __init__(self, databases: list[str], critical_service: bool = True):
        super(DatabaseUsageHealthCheck, self).__init__(critical_service)
        self.databases = databases

    async def _run_check(self):
        try:
            for db_name in self.databases:
                with connections[db_name].cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
        except IntegrityError as e:
            self.add_error(ServiceReturnedUnexpectedResult("Integrity Error"), e)
        except DatabaseError as e:
            self.add_error(ServiceUnavailable("Database error"), e)
