import os
import pytest

from elasticecsmetrics import ElasticECSMetricsLogger


@pytest.fixture
def es_host():
    return os.getenv('TEST_ES_SERVER', 'localhost')


@pytest.fixture
def es_port():
    default_port = 9200
    try:
        return int(os.getenv('TEST_ES_PORT', default_port))
    except ValueError:
        return default_port


def test_ping(es_host, es_port):
    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     es_index_name="pythontest",
                                     use_ssl=False)
    es_test_server_is_up = logger.test_es_source()
    assert es_test_server_is_up
