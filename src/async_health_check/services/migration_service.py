from django.db import DatabaseError, connections, DEFAULT_DB_ALIAS
from django.db.migrations.executor import MigrationExecutor

import settings
from async_health_check.exceptions import ServiceReturnedUnexpectedResult, ServiceUnavailable
from async_health_check.services.base_service import BaseService


class MigrationHealthCheck(BaseService):
    SERVICE_CODE = "MIGRATION"
    SERVICE_NAME = "Migration"

    def __init__(self, databases: list[str], critical_service: bool = True):
        super(MigrationHealthCheck, self).__init__(critical_service)
        self.databases = databases

    @staticmethod
    def get_migration_plan(executor):
        return executor.migration_plan(executor.loader.graph.leaf_nodes())

    async def _run_check(self):
        db_alias = getattr(settings, "health_check_migration_db", DEFAULT_DB_ALIAS)
        try:
            executor = MigrationExecutor(connections[db_alias])
            plan = self.get_migration_plan(executor)
            if plan:
                self.add_error(ServiceUnavailable("There are migrations to apply"))
        except DatabaseError as e:
            self.add_error(ServiceUnavailable("Database is not ready"), e)
        except Exception as e:
            self.add_error(ServiceUnavailable("Unexpected error"), e)
