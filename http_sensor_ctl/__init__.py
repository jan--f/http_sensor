import argparse

from http_sensor import sensor


def main():
    parser = argparse.ArgumentParser(description='Control http-sensor components')

    subparsers = parser.add_subparsers()
    status_parser = subparsers.add_parser(
        'status',
        help='Get status of local components'
    )

    sensor_parser = subparsers.add_parser(
        'sensor',
        help='Status and control for sensor component'
    )
    sensor_parser.add_argument(
        'sensor_command',
        help='Control command for sensor component',
        choices=['start', 'stop', 'restart', 'status', 'reload', 'add'],
    )
    sensor_parser.set_defaults(func=sensor_ctl)

    args = parser.parse_args()
    args.func(args)


def sensor_ctl(args):
    getattr(sensor, args.sensor_command)()
