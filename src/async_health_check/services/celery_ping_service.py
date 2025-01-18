from asgiref.sync import sync_to_async
from celery.app import default_app as app

from async_health_check.exceptions import ServiceUnavailable
from async_health_check.services.base_service import BaseService


class AsyncCeleryPingHealthCheck(BaseService):
    SERVICE_CODE = "CELERY"
    SERVICE_NAME = "Async Celery Health Check"

    CORRECT_PING_RESPONSE = {"ok": "pong"}

    def __init__(self, critical_service: bool = True, celery_ping_timeout: int = 1):
        super().__init__(critical_service)
        self.celery_ping_timeout = celery_ping_timeout

    async def _run_check(self):
        try:
            ping_result = await sync_to_async(app.control.ping,
                                              thread_sensitive=False)(timeout=self.celery_ping_timeout)
        except IOError as e:
            self.add_error(ServiceUnavailable("IOError"), e)
        except NotImplementedError as exc:
            self.add_error(
                ServiceUnavailable(
                    "NotImplementedError: Make sure CELERY_RESULT_BACKEND is set"
                ),
                exc,
            )
        except BaseException as exc:
            self.add_error(ServiceUnavailable("Unknown error"), exc)
        else:
            if not ping_result:
                self.add_error(ServiceUnavailable("Celery workers unavailable"))
            else:
                await self.check_ping_result(ping_result)

    async def check_ping_result(self, ping_result):
        """Check the ping response from workers."""
        active_workers = []

        for result in ping_result:
            worker, response = list(result.items())[0]
            if response != self.CORRECT_PING_RESPONSE:
                self.add_error(
                    ServiceUnavailable(
                        f"Celery worker {worker} response was incorrect"
                    )
                )
                continue
            active_workers.append(worker)

        if not self.errors:
            await self.check_active_queues(active_workers)

    async def check_active_queues(self, active_workers):
        """Verify that active workers are monitoring the defined queues."""
        defined_queues = app.conf.CELERY_QUEUES

        if not defined_queues:
            return

        defined_queues = {queue.name for queue in defined_queues}
        active_queues = set()

        active_queues_info = await sync_to_async(app.control.inspect(active_workers).active_queues,
                                                 thread_sensitive=False)()

        for queues in active_queues_info.values():
            active_queues.update(queue.get("name") for queue in queues)

        for queue in defined_queues.difference(active_queues):
            self.add_error(
                ServiceUnavailable(f"No worker for Celery task queue {queue}"),
            )
