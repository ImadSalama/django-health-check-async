from async_health_check.engine import HealthCheckEngine


def run_health_checks():
    return HealthCheckEngine().load_settings().load_services().run_checks().retrieve_results()
