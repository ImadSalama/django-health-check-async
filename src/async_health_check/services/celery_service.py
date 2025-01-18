from asgiref.sync import sync_to_async
from celery.exceptions import TaskRevokedError, TimeoutError

from async_health_check.exceptions import ServiceUnavailable, ServiceReturnedUnexpectedResult
from async_health_check.services.base_service import BaseService
from .celery_tasks import add


class AsyncCeleryHealthCheck(BaseService):
    SERVICE_CODE = "CELERY"
    SERVICE_NAME = "Async Celery Health Check"

    def __init__(self, critical_service: bool = True, celery_timeout: int = 3, celery_result_timeout: int = 3,
                 celery_queue_timeout: int = 3):
        super().__init__(critical_service)
        self.celery_timeout = celery_timeout
        self.celery_result_timeout = celery_result_timeout
        self.celery_queue_timeout = celery_queue_timeout

    async def _run_check(self):

        try:
            result = await self.invoke_task()

            if result != 8:
                self.add_error(
                    ServiceReturnedUnexpectedResult("Celery returned wrong result")
                )
        except IOError as e:
            self.add_error(ServiceUnavailable("IOError"), e)
        except NotImplementedError as e:
            self.add_error(
                ServiceUnavailable(
                    "NotImplementedError: Make sure CELERY_RESULT_BACKEND is set"
                ),
                e,
            )
        except TaskRevokedError as e:
            self.add_error(
                ServiceUnavailable(
                    "TaskRevokedError: The task was revoked, likely because it spent "
                    "too long in the queue"
                ),
                e,
            )
        except TimeoutError as e:
            self.add_error(
                ServiceUnavailable(
                    "TimeoutError: The task took too long to return a result"
                ),
                e,
            )
        except BaseException as e:
            self.add_error(ServiceUnavailable("Unknown error"), e)

    async def invoke_task(self):
        """Invoke the Celery task asynchronously and retrieve its result."""
        async_result = await sync_to_async(
            add.apply_async, thread_sensitive=False
        )(args=[4, 4], expires=self.celery_queue_timeout, queue=None)

        # Wait for the result asynchronously
        result = await sync_to_async(async_result.get, thread_sensitive=False)(
            timeout=self.celery_result_timeout
        )

        return result
