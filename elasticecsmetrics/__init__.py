"""
Elasticsearch metric logging
"""

import collections
import contextlib
import copy
import datetime
import json
import logging
import os
import socket
import time
import uuid
from dateutil.tz import tzlocal
from threading import Timer, Lock

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers as eshelpers
from enum import Enum

logger = logging.getLogger(__name__)

try:
    from requests_kerberos import HTTPKerberosAuth, DISABLED
    CMR_KERBEROS_SUPPORTED = True
except ImportError:
    CMR_KERBEROS_SUPPORTED = False

try:
    from requests_aws4auth import AWS4Auth
    AWS4AUTH_SUPPORTED = True
except ImportError:
    AWS4AUTH_SUPPORTED = False


class ElasticECSMetricsLogger(object):
    """
    Elasticsearch metrics logger

    Allows to log metrics to elasticsearch into json format.
    """

    class AuthType(Enum):
        """ Authentication types supported

        The handler supports
         - No authentication
         - Basic authentication
         - Kerberos or SSO authentication (on windows and linux)
        """
        NO_AUTH = 0
        BASIC_AUTH = 1
        KERBEROS_AUTH = 2
        AWS_SIGNED_AUTH = 3

    class IndexNameFrequency(Enum):
        """ Index type supported
        the handler supports
        - Daily indices
        - Weekly indices
        - Monthly indices
        - Year indices
        - Never expiring indices
        """
        DAILY = 0
        WEEKLY = 1
        MONTHLY = 2
        YEARLY = 3
        NEVER = 4

    # Defaults for the class
    __DEFAULT_ELASTICSEARCH_HOST = [{'host': 'localhost', 'port': 9200}]
    __DEFAULT_AUTH_USER = ''
    __DEFAULT_AUTH_PASSWD = ''
    __DEFAULT_AWS_ACCESS_KEY = ''
    __DEFAULT_AWS_SECRET_KEY = ''
    __DEFAULT_AWS_REGION = ''
    __DEFAULT_USE_SSL = False
    __DEFAULT_VERIFY_SSL = True
    __DEFAULT_AUTH_TYPE = AuthType.NO_AUTH
    __DEFAULT_INDEX_FREQUENCY = IndexNameFrequency.DAILY
    __DEFAULT_BUFFER_SIZE = 1000
    __DEFAULT_FLUSH_FREQ_INSEC = 1
    __DEFAULT_ADDITIONAL_FIELDS = {}
    __DEFAULT_ADDITIONAL_FIELDS_IN_ENV = {}
    __DEFAULT_ES_INDEX_NAME = 'python_metrics'

    __AGENT_TYPE = 'python-elasticsearch-ecs-metrics'
    __AGENT_VERSION = '1.0.0'
    __ECS_VERSION = "1.4.0"

    @staticmethod
    def _get_daily_index_name(es_index_name):
        """ Returns elasticearch index name
        :param: index_name the prefix to be used in the index
        :return: A srting containing the elasticsearch indexname used which should include the date.
        """
        return "{0!s}-{1!s}".format(es_index_name, datetime.datetime.now().strftime('%Y.%m.%d'))

    @staticmethod
    def _get_weekly_index_name(es_index_name):
        """ Return elasticsearch index name
        :param: index_name the prefix to be used in the index
        :return: A srting containing the elasticsearch indexname used which should include the date and specific week
        """
        current_date = datetime.datetime.now()
        start_of_the_week = current_date - datetime.timedelta(days=current_date.weekday())
        return "{0!s}-{1!s}".format(es_index_name, start_of_the_week.strftime('%Y.%m.%d'))

    @staticmethod
    def _get_monthly_index_name(es_index_name):
        """ Return elasticsearch index name
        :param: index_name the prefix to be used in the index
        :return: A srting containing the elasticsearch indexname used which should include the date and specific moth
        """
        return "{0!s}-{1!s}".format(es_index_name, datetime.datetime.now().strftime('%Y.%m'))

    @staticmethod
    def _get_yearly_index_name(es_index_name):
        """ Return elasticsearch index name
        :param: index_name the prefix to be used in the index
        :return: A srting containing the elasticsearch indexname used which should include the date and specific year
        """
        return "{0!s}-{1!s}".format(es_index_name, datetime.datetime.now().strftime('%Y'))

    @staticmethod
    def _get_never_index_name(es_index_name):
        """ Return elasticsearch index name
        :param: index_name the prefix to be used in the index
        :return: A srting containing the elasticsearch indexname used which should include just the index name
        """
        return "{0!s}".format(es_index_name)

    _INDEX_FREQUENCY_FUNCION_DICT = {
        IndexNameFrequency.DAILY: _get_daily_index_name,
        IndexNameFrequency.WEEKLY: _get_weekly_index_name,
        IndexNameFrequency.MONTHLY: _get_monthly_index_name,
        IndexNameFrequency.YEARLY: _get_yearly_index_name,
        IndexNameFrequency.NEVER: _get_never_index_name
    }

    def __init__(self,
                 hosts=__DEFAULT_ELASTICSEARCH_HOST,
                 auth_details=(__DEFAULT_AUTH_USER, __DEFAULT_AUTH_PASSWD),
                 aws_access_key=__DEFAULT_AWS_ACCESS_KEY,
                 aws_secret_key=__DEFAULT_AWS_SECRET_KEY,
                 aws_region=__DEFAULT_AWS_REGION,
                 auth_type=__DEFAULT_AUTH_TYPE,
                 use_ssl=__DEFAULT_USE_SSL,
                 verify_ssl=__DEFAULT_VERIFY_SSL,
                 buffer_size=__DEFAULT_BUFFER_SIZE,
                 flush_frequency_in_sec=__DEFAULT_FLUSH_FREQ_INSEC,
                 es_index_name=__DEFAULT_ES_INDEX_NAME,
                 index_name_frequency=__DEFAULT_INDEX_FREQUENCY,
                 es_additional_fields=__DEFAULT_ADDITIONAL_FIELDS,
                 es_additional_fields_in_env=__DEFAULT_ADDITIONAL_FIELDS_IN_ENV,
                 flush_failure_folder=None):
        """ Handler constructor

        :param hosts: The list of hosts that elasticsearch clients will connect. The list can be provided
                    in the format ```[{'host':'host1','port':9200}, {'host':'host2','port':9200}]``` to
                    make sure the client supports failover of one of the instertion nodes
        :param auth_details: When ```ElasticECSMetricsLogger.AuthType.BASIC_AUTH``` is used this argument must contain
                    a tuple of string with the user and password that will be used to authenticate against
                    the Elasticsearch servers, for example```('User','Password')
        :param aws_access_key: When ```ElasticECSMetricsLogger.AuthType.AWS_SIGNED_AUTH``` is used
                    this argument must contain the AWS key id of the  the AWS IAM user
        :param aws_secret_key: When ```ElasticECSMetricsLogger.AuthType.AWS_SIGNED_AUTH``` is used
                    this argument must contain the AWS secret key of the  the AWS IAM user
        :param aws_region: When ```ElasticECSMetricsLogger.AuthType.AWS_SIGNED_AUTH``` is used
                    this argument must contain the AWS region of the the AWS Elasticsearch servers,
                    for example```'us-east'
        :param auth_type: The authentication type to be used in the connection ```ElasticECSMetricsLogger.AuthType```
                    Currently, NO_AUTH, BASIC_AUTH, KERBEROS_AUTH are supported
                    You can pass a str instead of the enum value. It is useful if you are using a config file for
                    configuring the logging module.
        :param use_ssl: A boolean that defines if the communications should use SSL encrypted communication
        :param verify_ssl: A boolean that defines if the SSL certificates are validated or not
        :param buffer_size: An int, Once this size is reached on the internal buffer results are flushed into ES
        :param flush_frequency_in_sec: A float representing how often and when the buffer will be flushed, even
                    if the buffer_size has not been reached yet
        :param es_index_name: A string with the prefix of the elasticsearch index that will be created. Note a
                    date with YYYY.MM.dd, ```python_logger``` used by default
        :param index_name_frequency: Defines what the date used in the postfix of the name would be. available values
                    are selected from the IndexNameFrequency class (IndexNameFrequency.DAILY,
                    IndexNameFrequency.WEEKLY, IndexNameFrequency.MONTHLY, IndexNameFrequency.YEARLY,
                    IndexNameFrequency.NEVER). By default it uses daily indices.
                    You can pass a str instead of the enum value. It is useful if you are using a config file for
                    configuring the logging module.
        :param es_additional_fields: A dictionary with all the additional fields that you would like to add
                    to the logs, such the application, environment, etc. You can nest dicts to follow ecs convention.
        :param es_additional_fields_in_env: A dictionary with all the additional fields that you would like to add
                    to the logs, such the application, environment, etc. You can nest dicts to follow ecs convention.
                    The values are environment variables keys. At each elastic document created, the values of these
                    environment variables will be read. If an environment variable for a field doesn't exists, the value
                    of the same field in es_additional_fields will be taken if it exists. In last resort, there will be
                    no value for the field.
        :param flush_failure_folder: A folder where the logger will put the elastic documents in
                                     JSON files when the flush operation failed.
                                     If None, this feature is disabled.
        :return: A ready to be used ElasticECSMetricsLogger.
        """
        self.hosts = hosts
        self.auth_details = auth_details
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_region = aws_region
        if isinstance(auth_type, str):
            self.auth_type = ElasticECSMetricsLogger.AuthType[auth_type]
        else:
            self.auth_type = auth_type
        self.use_ssl = use_ssl
        self.verify_certs = verify_ssl
        self.buffer_size = buffer_size
        self.flush_frequency_in_sec = flush_frequency_in_sec
        self.es_index_name = es_index_name
        if isinstance(index_name_frequency, str):
            self.index_name_frequency = ElasticECSMetricsLogger.IndexNameFrequency[index_name_frequency]
        else:
            self.index_name_frequency = index_name_frequency

        self.es_additional_fields = copy.deepcopy(es_additional_fields.copy())
        self.es_additional_fields.setdefault('ecs', {})['version'] = ElasticECSMetricsLogger.__ECS_VERSION

        self.flush_failure_folder = flush_failure_folder

        agent_dict = self.es_additional_fields.setdefault('agent', {})
        agent_dict['ephemeral_id'] = str(uuid.uuid4())
        agent_dict['type'] = ElasticECSMetricsLogger.__AGENT_TYPE
        agent_dict['version'] = ElasticECSMetricsLogger.__AGENT_VERSION

        host_dict = self.es_additional_fields.setdefault('host', {})
        host_name = socket.gethostname()
        host_dict['hostname'] = host_name
        host_dict['name'] = host_name
        host_dict['id'] = host_name
        host_dict['ip'] = socket.gethostbyname(socket.gethostname())

        self.es_additional_fields_in_env = copy.deepcopy(es_additional_fields_in_env.copy())

        self._client = None
        self._buffer = []
        self._buffer_lock = Lock()
        self._timer = None
        self._index_name_func = ElasticECSMetricsLogger._INDEX_FREQUENCY_FUNCION_DICT[self.index_name_frequency]

    def __del__(self):
        self.flush()

    def __schedule_flush(self):
        if self._timer is None:
            self._timer = Timer(self.flush_frequency_in_sec, self.flush)
            self._timer.setDaemon(True)
            self._timer.start()

    def __get_es_client(self):
        if self.auth_type == ElasticECSMetricsLogger.AuthType.NO_AUTH:
            if self._client is None:
                self._client = Elasticsearch(hosts=self.hosts,
                                             use_sl=self.use_ssl,
                                             verify_certs=self.verify_certs,
                                             connection_class=RequestsHttpConnection)
            return self._client

        if self.auth_type == ElasticECSMetricsLogger.AuthType.BASIC_AUTH:
            if self._client is None:
                return Elasticsearch(hosts=self.hosts,
                                     http_auth=self.auth_details,
                                     use_ssl=self.use_ssl,
                                     verify_certs=self.verify_certs,
                                     connection_class=RequestsHttpConnection)
            return self._client

        if self.auth_type == ElasticECSMetricsLogger.AuthType.KERBEROS_AUTH:
            if not CMR_KERBEROS_SUPPORTED:
                raise EnvironmentError("Kerberos module not available. Please install \"requests-kerberos\"")
            # For kerberos we return a new client each time to make sure the tokens are up to date
            return Elasticsearch(hosts=self.hosts,
                                 use_ssl=self.use_ssl,
                                 verify_certs=self.verify_certs,
                                 connection_class=RequestsHttpConnection,
                                 http_auth=HTTPKerberosAuth(mutual_authentication=DISABLED))

        if self.auth_type == ElasticECSMetricsLogger.AuthType.AWS_SIGNED_AUTH:
            if not AWS4AUTH_SUPPORTED:
                raise EnvironmentError("AWS4Auth not available. Please install \"requests-aws4auth\"")
            if self._client is None:
                awsauth = AWS4Auth(self.aws_access_key, self.aws_secret_key, self.aws_region, 'es')
                self._client = Elasticsearch(
                    hosts=self.hosts,
                    http_auth=awsauth,
                    use_ssl=self.use_ssl,
                    verify_certs=True,
                    connection_class=RequestsHttpConnection)
            return self._client

        raise ValueError("Authentication method not supported")

    def test_es_source(self):
        """ Returns True if the handler can ping the Elasticsearch servers

        Can be used to confirm the setup of a handler has been properly done and confirm
        that things like the authentication is working properly

        :return: A boolean, True if the connection against elasticserach host was successful
        """
        return self.__get_es_client().ping()

    def flush(self):
        """
        Flushes the buffer into ES
        :param reraise_exception: Reraise exception that happened when sending elastic documents.
        :return: None
        """
        if self._timer is not None and self._timer.is_alive():
            self._timer.cancel()
        self._timer = None

        if self._buffer:
            documents_buffer = []
            try:
                with self._buffer_lock:
                    documents_buffer = self._buffer
                    self._buffer = []
                actions = (
                    {
                        '_index': self._index_name_func.__func__(self.es_index_name),
                        '_source': document_record
                    }
                    for document_record in documents_buffer
                )
                eshelpers.bulk(
                    client=self.__get_es_client(),
                    actions=actions,
                    stats_only=True
                )
            except Exception:
                logger.exception("Cannot send documents to Elastic.")
                if self.flush_failure_folder is not None:
                    try:
                        _write_flush_failure_file(documents_buffer, self.flush_failure_folder, self.es_index_name)
                    except Exception:
                        logger.exception("Cannot write flush failure file.")

    def log_time_metric(self, metric_name, start_datetime, time_us):
        """
        Log a new time metric.

        param metric_name: The metric's name.
        param start_datetime: The datetime where the the timer was started. The datetime object must be timezone aware.
        param time_us: The time in microsecond.
        """
        elastic_document = copy.deepcopy(self.es_additional_fields)
        elastic_document.update({
            '@timestamp': _get_es_datetime_str(start_datetime),
            'metrics': {
                'name': metric_name,
                'time': {
                    'us': int(time_us)
                }
            }
        })
        self._send_document(elastic_document)

    @contextlib.contextmanager
    def log_time_metric_timer(self, metric_name):
        """
        Return a new context manager with a timer to log a new time metric.
        It is when the __exit__ method of the context manager is called that a new time metric is logged.

        param: metric_name: The metric's name.
        return: A context manager with a timer to log a new time metric.
        """
        start_datetime = datetime.datetime.now(tzlocal())
        start_time = time.time()
        yield
        end_time = time.time()
        time_us = int((end_time - start_time) * 1000000)
        self.log_time_metric(metric_name, start_datetime, time_us)

    def _send_document(self, document):
        """
        Put the Elastic document to the buffer for sending later.

        :param document: A dict representing a elastic document.
        :return: None
        """
        self._add_additional_fields_in_env(document)
        with self._buffer_lock:
            self._buffer.append(document)

        if len(self._buffer) >= self.buffer_size:
            self.flush()
        else:
            self.__schedule_flush()

    def _add_additional_fields_in_env(self, document):
        """
        Add the additional fields with their values in environment variables.
        :param es_record: The record where the additional fields with
                          their values fetched in environment variables will be added or overridden.
        """
        additional_fields_in_env_values = _fetch_additional_fields_in_env(self.es_additional_fields_in_env)
        _update_nested_dict(document, additional_fields_in_env_values)


def _fetch_additional_fields_in_env(additional_fields_env_keys):
    """
    Walk the additional_fields_env_keys and fetch the values from the environment variables.
    :param additional_fields_env_keys: A dictionnary with the additional_fields_in_env with their keys.
    :return: A dictionary with the additional_fields_in_env with their values fetched instead of their keys.
    """
    additional_fields_env_values = {}
    for dict_key, dict_value in additional_fields_env_keys.items():
        if isinstance(dict_value, collections.Mapping):
            nested_dict_env_keys = dict_value
            additional_fields_env_values[dict_key] = _fetch_additional_fields_in_env(nested_dict_env_keys)
        else:
            if dict_value in os.environ:
                additional_fields_env_values[dict_key] = os.environ[dict_value]
    return additional_fields_env_values


def _update_nested_dict(source, override):
    """
    Update the source dictionary with the override dictionary.

    :param source: The dictionary to update.
    :param override: The dictionary that will update the source dictionary.
    """
    for key, value in override.items():
        if isinstance(value, collections.Mapping):
            _update_nested_dict(source.setdefault(key, {}), value)
        else:
            source[key] = value


def _write_flush_failure_file(documents_buffer, flush_failure_folder, index_name):
    """
    Write a JSON file with the contents of documents_buffer.

    :param documents_buffer: The documents the logger tried to send to Elasticsearch.
    :param flush_failure_folder: The folder where the the JSON files will be written.
    :param index_name: The elasticsearch index's name.
    """
    flush_file_path = _compute_unique_flush_file_path(flush_failure_folder, index_name)
    with open(flush_file_path, 'w') as flush_file:
        json.dump(documents_buffer, flush_file)


def _compute_unique_flush_file_path(flush_failure_folder, index_name):
    """
    Return a file name for the JSON file that doesn't exist yet.

    :param flush_failure_folder: The folder where the the JSON files will be written.
    :param index_name: The elasticsearch index's name.
    :return: A file name for the JSON file that doesn't exist yet.
    """
    while True:
        es_datetime_str = _get_es_datetime_str(datetime.datetime.now(tzlocal()))
        file_name = "failed_flush_{}_{}.json".format(index_name, es_datetime_str)
        file_path = os.path.join(flush_failure_folder, file_name)
        if not os.path.isfile(file_path):
            return file_path


def _get_es_datetime_str(datetime_object):
    """
    Returns elasticsearch utc formatted time for a datetime object

    :param timestamp: epoch, including milliseconds
    :return: A string valid for elasticsearch time record
    """
    if datetime_object.tzinfo is None or datetime_object.tzinfo.utcoffset(datetime_object) is None:
        raise NaiveDatetimeError('"{}" is not timezone aware.'.format(datetime_object))
    return "{0!s}.{1:03d}{2}".format(datetime_object.strftime('%Y-%m-%dT%H:%M:%S'),
                                     int(datetime_object.microsecond / 1000),
                                     datetime_object.strftime('%z'))


class NaiveDatetimeError(Exception):
    """
    Datetime should be timezone aware because Elasticsearch need timezone aware dates.
    """
    pass
