from timeit import default_timer as timer
from async_health_check.exceptions import HealthCheckException

from django.utils.translation import gettext_lazy as _  # noqa: N812
import logging

logger = logging.getLogger('async-django-health-check')


class BaseService:
    SERVICE_CODE = ""
    SERVICE_NAME = ""

    def __init__(self, critical_service: bool = True):
        self.critical_service = critical_service
        self.errors = []
        self.time_taken = 0

    async def run_check(self):
        start_time = timer()
        try:
            await self._run_check()
        except HealthCheckException as e:
            self.add_error(e, e)
        except BaseException:
            logger.exception("Unexpected Error")
        finally:
            self.time_taken = timer() - start_time
        return self

    async def _run_check(self):
        raise NotImplementedError("Run Check Is not Implemented in the Base Service")

    def add_error(self, error, cause=None):
        if isinstance(error, HealthCheckException):
            pass
        elif isinstance(error, str):
            msg = error
            error = HealthCheckException(msg)
        else:
            msg = _("unknown error")
            error = HealthCheckException(msg)
        if isinstance(cause, BaseException):
            logger.exception(str(error))
        else:
            logger.error(str(error))
        self.errors.append(error)
