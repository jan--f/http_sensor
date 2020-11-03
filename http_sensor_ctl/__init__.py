import argparse

from http_sensor import sensor


def main():
    parser = argparse.ArgumentParser(description='Control http-sensor components')

    subparsers = parser.add_subparsers()
    status_parser = subparsers.add_parser(
        'status',
        help='Get status of local components'
    )

    _setup_sensor_parser(subparsers)

    args = parser.parse_args()
    args.func(args)


def _setup_sensor_parser(subparsers):
    sensor_parser = subparsers.add_parser(
        'sensor',
        help='Status and control for sensor component'
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
