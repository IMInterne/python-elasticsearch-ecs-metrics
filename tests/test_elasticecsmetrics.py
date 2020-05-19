import datetime
import json
import os
import pytest
import time

from dateutil.tz import tzlocal

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


def test_buffered_metric_insertion_flushed_when_buffer_full(es_host, es_port, tmpdir):
    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     use_ssl=False,
                                     buffer_size=2,
                                     flush_frequency_in_sec=1000,
                                     es_index_name="pythontest",
                                     flush_failure_folder=str(tmpdir))

    es_test_server_is_up = logger.test_es_source()
    assert es_test_server_is_up

    logger.log_time_metric('test', datetime.datetime.now(tzlocal()), 0)
    logger.log_time_metric('test', datetime.datetime.now(tzlocal()), 0)
    assert 0 == len(logger._buffer)
    assert 0 == len(tmpdir.listdir(fil=(lambda path: path.ext == '.json')))


def test_es_metric_with_additional_env_fields(es_host, es_port, tmpdir):
    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     use_ssl=False,
                                     es_index_name="pythontest",
                                     flush_failure_folder=str(tmpdir),
                                     es_additional_fields={'App': 'Test', 'Nested': {'One': '1', 'Two': '2'}},
                                     es_additional_fields_in_env={'App': 'ENV_APP', 'Environment': 'ENV_ENV',
                                                                  'Nested': {'One': 'ENV_ONE'}})

    es_test_server_is_up = logger.test_es_source()
    assert es_test_server_is_up

    logger.log_time_metric('test', datetime.datetime.now(tzlocal()), 0)
    assert 1 == len(logger._buffer)
    assert 'Test' == logger._buffer[0]['App']
    assert '1' == logger._buffer[0]['Nested']['One']
    assert '2' == logger._buffer[0]['Nested']['Two']
    assert 'Environment' not in logger._buffer[0]

    logger.flush()
    assert 0 == len(logger._buffer)

    os.environ['ENV_APP'] = 'Test2'
    os.environ['ENV_ENV'] = 'Dev'
    os.environ['ENV_ONE'] = 'One'
    logger.log_time_metric('test', datetime.datetime.now(tzlocal()), 0)
    assert 1 == len(logger._buffer)
    assert 'Test2' == logger._buffer[0]['App']
    assert 'Dev' == logger._buffer[0]['Environment']
    assert 'One' == logger._buffer[0]['Nested']['One']
    assert '2' == logger._buffer[0]['Nested']['Two']

    del os.environ['ENV_APP']
    del os.environ['ENV_ENV']
    del os.environ['ENV_ONE']

    logger.flush()
    assert 0 == len(logger._buffer)
    assert 0 == len(tmpdir.listdir(fil=(lambda path: path.ext == '.json')))


def test_log_time_metric_timer(es_host, es_port, tmpdir):
    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     use_ssl=False,
                                     es_index_name="pythontest",
                                     flush_failure_folder=str(tmpdir))

    es_test_server_is_up = logger.test_es_source()
    assert es_test_server_is_up

    with logger.log_time_metric_timer('testTimer'):
        time.sleep(1)

    assert 1 == len(logger._buffer)
    assert 1000000 <= logger._buffer[0]['metrics']['time']['us']
    logger.flush()
    assert 0 == len(logger._buffer)
    assert 0 == len(tmpdir.listdir(fil=(lambda path: path.ext == '.json')))


def test_buffered_log_insertion_after_interval_expired(es_host, es_port, tmpdir):
    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     use_ssl=False,
                                     flush_frequency_in_sec=0.1,
                                     es_index_name="pythontest",
                                     flush_failure_folder=str(tmpdir))

    es_test_server_is_up = logger.test_es_source()
    assert es_test_server_is_up

    logger.log_time_metric('test', datetime.datetime.now(tzlocal()), 0)
    assert 1 == len(logger._buffer)
    time.sleep(1)
    assert 0 == len(logger._buffer)
    assert 0 == len(tmpdir.listdir(fil=(lambda path: path.ext == '.json')))


def test_fast_insertion_of_hundred_metrics(es_host, es_port, tmpdir):
    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     use_ssl=False,
                                     buffer_size=500,
                                     flush_frequency_in_sec=0.5,
                                     es_index_name="pythontest",
                                     flush_failure_folder=str(tmpdir))
    for i in range(100):
        logger.log_time_metric('test', datetime.datetime.now(tzlocal()), 0)
    logger.flush()
    assert 0 == len(logger._buffer)
    assert 0 == len(tmpdir.listdir(fil=(lambda path: path.ext == '.json')))


def test_flush_failed_files(tmpdir):
    logger = ElasticECSMetricsLogger(hosts=[{'host': '', 'port': 0}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     use_ssl=False,
                                     es_index_name="pythontest",
                                     flush_failure_folder=str(tmpdir))

    logger.log_time_metric('test', datetime.datetime.now(tzlocal()), 0)
    logger.flush()
    assert 0 == len(logger._buffer)
    json_files = tmpdir.listdir(fil=(lambda path: path.ext == '.json'))
    assert 1 == len(json_files)

    with open(str(json_files[0]), mode='r') as json_file:
        failed_flush_buffer = json.load(json_file)
        assert 1 == len(failed_flush_buffer)
        assert 'test' == failed_flush_buffer[0]['metrics']['name']
        assert 0 == failed_flush_buffer[0]['metrics']['time']['us']


def test_index_name_frequency_functions(es_host, es_port):
    index_name = "pythontest"
    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     es_index_name=index_name,
                                     use_ssl=False,
                                     index_name_frequency=ElasticECSMetricsLogger.IndexNameFrequency.DAILY)
    assert ElasticECSMetricsLogger._get_daily_index_name(index_name) == logger._index_name_func.__func__(index_name)

    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     es_index_name=index_name,
                                     use_ssl=False,
                                     index_name_frequency=ElasticECSMetricsLogger.IndexNameFrequency.WEEKLY)
    assert ElasticECSMetricsLogger._get_weekly_index_name(index_name) == logger._index_name_func.__func__(index_name)

    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     es_index_name=index_name,
                                     use_ssl=False,
                                     index_name_frequency=ElasticECSMetricsLogger.IndexNameFrequency.MONTHLY)
    assert ElasticECSMetricsLogger._get_monthly_index_name(index_name) == logger._index_name_func.__func__(index_name)

    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     es_index_name=index_name,
                                     use_ssl=False,
                                     index_name_frequency=ElasticECSMetricsLogger.IndexNameFrequency.YEARLY)
    assert ElasticECSMetricsLogger._get_yearly_index_name(index_name) == logger._index_name_func.__func__(index_name)

    logger = ElasticECSMetricsLogger(hosts=[{'host': es_host, 'port': es_port}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     es_index_name=index_name,
                                     use_ssl=False,
                                     index_name_frequency=ElasticECSMetricsLogger.IndexNameFrequency.NEVER)
    assert ElasticECSMetricsLogger._get_never_index_name(index_name) == logger._index_name_func.__func__(index_name)
