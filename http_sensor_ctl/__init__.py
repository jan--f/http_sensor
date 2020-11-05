import argparse

from http_sensor import sensor
from kafka_db_writer import kafka_db_writer as consumer


def main():
    # TODO add func default to parser for help message
    parser = argparse.ArgumentParser(
        description='Control http-sensor components'
    )

    subparsers = parser.add_subparsers()

    _setup_consumer_parser(subparsers)

    _setup_sensor_parser(subparsers)

    args = parser.parse_args()
    args.func(args)


def _setup_consumer_parser(subparsers):
    # TODO cleanup uneeded comands
    # TODO add func default to consumer parser for help message
    consumer_parser = subparsers.add_parser(
        'consumer',
        help='Status and control for consumer component'
    )
    consumer_parser.add_argument(
        '--max_workers',
        help='Configure a worker limit',
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
    # TODO add func default to sensor_parser for help message
    sensor_parser = subparsers.add_parser(
        'sensor',
        help='Status and control for sensor component'
    )
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
