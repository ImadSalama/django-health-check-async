from aiohttp import ClientSession
from django.conf import settings
import asyncio
from async_health_check.constants import ServiceStatus
from async_health_check.services.service_factory import ServiceFactory


class HealthCheckEngine:
    _instance = None

    def __init__(self, ):
        self.settings = {}
        self.services = []

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(HealthCheckEngine, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def load_settings(self):
        self.settings = settings.HEALTH_CHECK
        return self

    def load_services(self):
        for setting in self.settings['SERVICES']:
            service = ServiceFactory.get_service(setting['NAME'], setting['OPTIONS'])
            self.services.append(service)
        return self

    def run_checks(self):
        async def fetch_all():
            async with ClientSession() as session:
                tasks = [service.run_check() for service in self.services]
                await asyncio.gather(*tasks)

        asyncio.run(fetch_all())
        return self

    def retrieve_results(self):
        results = []
        for service in self.services:
            results.append(
                {
                    'SERVICE_NAME': service.SERVICE_NAME,
                    'STATUS': ServiceStatus.FAILED if service.errors else ServiceStatus.SUCCESS,
                    'CRITICAL_SERVICE': service.critical_service,
                    'ERRORS': [str(err) for err in service.errors],
                    'TIME_TAKEN': service.time_taken,
                }
            )
        return results
