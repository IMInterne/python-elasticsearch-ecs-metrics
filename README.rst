=========================
ElasticECSMetricLogger.py
=========================

|  |ci_status| |codecov|

Python Elasticsearch ECS Metric Logger
**************************************

This library provides an Elasticsearch metric logger. You can send simple metrics like the time it takes to do an operation. 
There is also Elastic APM that does somewhat the same thing. If you want to do Application Performance Monitoring, use Elastic APM.
If you want to take a lot of small metrics that doesn't have parent/child relationship, use this library instead.
This follows the `Elastic Common Schema (ECS) <https://www.elastic.co/guide/en/ecs/current/index.html>`_ for the field names.

To follow the ECS mapping, please use an index template.
Look at `ECS Github repository <https://github.com/elastic/ecs>`_ for already generated ECS mappings objects or
in the mappings folder of this repository where you will find
one mapping file with all ecs fields plus some added that is used by this logger and another
one with just the fields used by this logger.

The code source is in github at `https://github.com/innovmetric/python-elasticsearch-ecs-metrics
<https://github.com/innovmetric/python-elasticsearch-ecs-metrics>`_.

Installation
============
Install using pip::

    pip install ElasticECSMetrics

Requirements Python 2
=====================
This library requires the following dependencies
 - elasticsearch
 - requests
 - python-dateutil
 - enum34

Requirements Python 3
=====================
This library requires the following dependencies
 - elasticsearch
 - requests
 - python-dateutil

Additional requirements for Kerberos support
============================================
Additionally, the package support optionally kerberos authentication by adding the following dependecy
 - requests-kerberos

.. warning::
  Unfortunately, we don't have the time to test kerberos authenticationon support. We let the code here because it is simple and it should work.

Additional requirements for AWS IAM user authentication (request signing)
=========================================================================
Additionally, the package support optionally AWS IAM user authentication by adding the following dependecy
 - requests-aws4auth

.. warning::
  Unfortunately, we don't have the time to test AWS IAM user authentication support. We let the code here because it is simple and it should work.

Using the metric logger in your program
=======================================
Initialise and create the metric logger as follow ::

    from elasticecsmetrics import ElasticECSMetricsLogger, now
    logger = ElasticECSMetricsLogger(hosts=[{'host': 'localhost', 'port': 9200}],
                                     auth_type=ElasticECSMetricsLogger.AuthType.NO_AUTH,
                                     es_index_name="pythontest",
                                     use_ssl=False)

After, you can take a metric with the log_time_metric_timer function that returns a context manager as follow ::

    with logger.log_time_metric_timer('testTimer'):
        time.sleep(1)

The logger is supposed to flush when its destructor is called. It is safer however to call flush manually as follow ::

    logger.flush()

Initialisation parameters
=========================
The constructors takes the following parameters:
 - hosts:  The list of hosts that elasticsearch clients will connect, multiple hosts are allowed, for example ::

    [{'host':'host1','port':9200}, {'host':'host2','port':9200}]


 - auth_type: The authentication currently support ElasticECSMetricsLogger.AuthType = NO_AUTH, BASIC_AUTH, KERBEROS_AUTH
 - auth_details: When ElasticECSMetricsLogger.AuthType.BASIC_AUTH is used this argument must contain a tuple of string with the user and password that will be used to authenticate against the Elasticsearch servers, for example ('User','Password')
 - aws_access_key: When ``ElasticECSMetricsLogger.AuthType.AWS_SIGNED_AUTH`` is used this argument must contain the AWS key id of the  the AWS IAM user
 - aws_secret_key: When ``ElasticECSMetricsLogger.AuthType.AWS_SIGNED_AUTH`` is used this argument must contain the AWS secret key of the  the AWS IAM user
 - aws_region: When ``ElasticECSMetricsLogger.AuthType.AWS_SIGNED_AUTH`` is used this argument must contain the AWS region of the  the AWS Elasticsearch servers, for example ``'us-east'``
 - use_ssl: A boolean that defines if the communications should use SSL encrypted communication
 - verify_ssl: A boolean that defines if the SSL certificates are validated or not
 - buffer_size: An int, Once this size is reached on the internal buffer results are flushed into ES
 - flush_frequency_in_sec: A float representing how often and when the buffer will be flushed
 - es_index_name: A string with the prefix of the elasticsearch index that will be created. Note a date with
   YYYY.MM.dd, ``python_logger`` used by default
 - index_name_frequency: The frequency to use as part of the index naming. Currently supports
   ``ElasticECSMetricsLogger.IndexNameFrequency.DAILY``, ``ElasticECSMetricsLogger.IndexNameFrequency.WEEKLY``,
   ``ElasticECSMetricsLogger.IndexNameFrequency.MONTHLY``, ``ElasticECSMetricsLogger.IndexNameFrequency.YEARLY`` and
   ``ElasticECSMetricsLogger.IndexNameFrequency.NEVER``. By default the daily rotation is used.
 - es_additional_fields: A nested dictionary with all the additional fields that you would like to add to the logs.
 - es_additional_fields_in_env: A nested dictionary with all the additional fields that you would like to add to the logs.
   The values are environment variables keys. At each elastic document created, the values of these environment variables will be read.
   If an environment variable for a field doesn't exists, the value of the same field in es_additional_fields will be taken if it exists.
   In last resort, there will be no value for the field.
 - flush_failure_folder: A path to a folder where the metrics will be flushed in JSON files if the logger cannot send them to the Elasticsearch servers.
 - param flush_failure_folder: A folder where the logger will put the elastic documents in JSON files when the flush operation failed. If None, this feature is disabled.

Building the sources & Testing
------------------------------
To create the package follow the standard python setup.py to compile.
To test, just execute the python tests within the test folder

Contributing back
-----------------
Feel free to use this as is or even better, feel free to fork and send your pull requests over.

.. |ci_status| image:: https://travis-ci.com/innovmetric/python-elasticsearch-ecs-metrics.svg?branch=master
    :target: https://travis-ci.com/innovmetric/python-elasticsearch-ecs-metrics
    :alt: Continuous Integration Status
.. |codecov| image:: https://codecov.io/github/innovmetric/python-elasticsearch-ecs-metrics/coverage.svg?branch=master
    :target: https://codecov.io/github/innovmetric/python-elasticsearch-ecs-metrics?branch=master
    :alt: Coverage!
