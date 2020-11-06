import argparse

from http_sensor import sensor
from kafka_db_writer import kafka_db_writer as consumer


def main():
    parser = argparse.ArgumentParser(
        description='''Control http-sensor components

Both components read a config file in yaml format. It should contain a
the necessary runtime configuration, similar to this example:

urls: # a list of urls to scrape and how often
  - url: <URL>
    repeat: 15 # repeat interval in seconds
    regex: foo # an optional regex to check the response content agains
kafka:
  bootstrap_urls:
    - <Kafka URL>
  topic: topic
  key_file: key.key # key to connect to Kafka
  cert_file: cert.pem # certificate to connect to Kafka
  ca_file: ca.pem # CA to connect to Kafka
db:
  # the db connect string will be fed straight to psycopg2 connect
  connect: dbname=<db_name> user=<user> password=<user password> host=<db host name> port=<port>
  tables:
    - <list of tables>

Not all keys are needed by all components but a missing component might
impact functionality. For example the sensor component can run without
the 'kafka' key, but in turn will not send its results to Kafka.
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.set_defaults(func=lambda _x: parser.print_help())

    subparsers = parser.add_subparsers()

    _setup_consumer_parser(subparsers)

    _setup_sensor_parser(subparsers)

    args = parser.parse_args()
    args.func(args)


def _setup_consumer_parser(subparsers):
    consumer_parser = subparsers.add_parser(
        'consumer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='''Get the status from and control the consumer component

The consumer will run as a daemon process in the background and poll a Kafka
instance according to the configuration. If it receives messages it
will write them to a database. Only after the message has been
persisted it will commit the read message(s) to Kafka, so nothing
should get lost.
        '''
    )
    consumer_parser.set_defaults(func=lambda _x: consumer_parser.print_help())
    consumer_parser.add_argument(
        '--max_workers',
        help='Configure a worker limit, ideally corresponds to your '
             'topic partitions',
        type=int,
        default=8,
    )
    consumer_parser.add_argument(
        '--pid-file',
        help='Configure the pid file location',
        default='/tmp/http-consumer.pid',
    )
    consumer_parser.add_argument(
        '--log-file',
        help='Configure the log file location',
        default='/tmp/http-consumer.log',
    )
    consumer_parser.add_argument(
        '--log-level', '-l',
        help='Configure the log level',
        default='INFO',
        choices=['INFO', 'DEBUG', 'ERROR'],
    )
    consumer_subparsers = consumer_parser.add_subparsers(
        dest='consumer_command',
    )

    def _add_config_arg(p):
        p.add_argument(
            '--config', '-c',
            help='Path to config file',
            default='config.yaml',
        )

    consumer_start_parser = consumer_subparsers.add_parser(
        'start',
        help='Start the consumer')
    _add_config_arg(consumer_start_parser)
    consumer_start_parser.set_defaults(func=consumer_ctl)

    consumer_status_parser = consumer_subparsers.add_parser(
        'status',
        help='Get the consumer status')
    consumer_status_parser.set_defaults(func=consumer_ctl)

    consumer_stop_parser = consumer_subparsers.add_parser(
        'stop',
        help='Stop the consumer')
    consumer_stop_parser.set_defaults(func=consumer_ctl)

    consumer_restart_parser = consumer_subparsers.add_parser(
        'restart',
        help='Restart the consumer')
    _add_config_arg(consumer_restart_parser)
    consumer_restart_parser.set_defaults(func=consumer_ctl)

    consumer_reload_parser = consumer_subparsers.add_parser(
        'reload',
        help='Reload the configuration file of the consumer')
    consumer_reload_parser.set_defaults(func=consumer_ctl)


def consumer_ctl(args):
    getattr(consumer, args.consumer_command)(args)


def _setup_sensor_parser(subparsers):
    sensor_parser = subparsers.add_parser(
        'sensor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='''Get status from and control the sensor component
The sensor will run as a daemon process in the background and make requests to
the URLs configured. If no URLs are configured it will just exit. URLs
are repeatedly requested according to the URLs 'repeat' property (see
http_sensor_ctl -h for an example).
Please note that the repeat interval is not exact _and_ defines the
wait interval between the end of the previous request (including the
write operation to Kafka) and the next attempt. So slow webservers,
large responses and slow Kafka instances will add to the interval.

Requests are scheduled according to their projected next execution time.
This can however fall behind if the worker thread pool is overloaded.
In this case try to configure more workers or reduce the number of URLs
to be scraped.
        '''
    )
    sensor_parser.set_defaults(func=lambda _x: sensor_parser.print_help())
    sensor_parser.add_argument(
        '--max_workers',
        help='Configure a worker limit',
        type=int,
        default=32,
    )
    sensor_parser.add_argument(
        '--pid-file',
        help='Configure the pid file location',
        default='/tmp/http-sensor.pid',
    )
    sensor_parser.add_argument(
        '--log-file',
        help='Configure the log file location',
        default='/tmp/http-sensor.log',
    )
    sensor_parser.add_argument(
        '--log-level', '-l',
        help='Configure the log level',
        default='INFO',
        choices=['INFO', 'DEBUG', 'ERROR'],
    )
    sensor_subparsers = sensor_parser.add_subparsers(
        dest='sensor_command',
    )

    def _add_config_arg(p):
        p.add_argument(
            '--config', '-c',
            help='Path to config file',
            default='config.yaml',
        )

    sensor_start_parser = sensor_subparsers.add_parser(
        'start',
        help='Start the sensor')
    _add_config_arg(sensor_start_parser)
    sensor_start_parser.set_defaults(func=sensor_ctl)

    sensor_status_parser = sensor_subparsers.add_parser(
        'status',
        help='Get the sensor status')
    sensor_status_parser.set_defaults(func=sensor_ctl)

    sensor_stop_parser = sensor_subparsers.add_parser(
        'stop',
        help='Stop the sensor')
    sensor_stop_parser.set_defaults(func=sensor_ctl)

    sensor_restart_parser = sensor_subparsers.add_parser(
        'restart',
        help='Restart the sensor')
    _add_config_arg(sensor_restart_parser)
    sensor_restart_parser.set_defaults(func=sensor_ctl)

    sensor_reload_parser = sensor_subparsers.add_parser(
        'reload',
        help='Reload the configuration file of the sensor')
    sensor_reload_parser.set_defaults(func=sensor_ctl)

    sensor_add_parser = sensor_subparsers.add_parser(
        'add',
        help='Add a url to the sensor config')
    _add_config_arg(sensor_add_parser)
    sensor_add_parser.set_defaults(func=sensor_ctl)


def sensor_ctl(args):
    getattr(sensor, args.sensor_command)(args)
