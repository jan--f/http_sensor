![tox](https://github.com/jan--f/http_sensor/workflows/tox/badge.svg)

# http_sensor
A simple http sensor that makes http requests to configured URLs repeatedly with 
a sleep period between.

## Installation
To install this:

- clone this repo
- (optionally) setup a virtualenv
- run `python3 setup.py install`

## Usage
You get two daemons `http_sensor` and `TBD`.

Both are controlled via a script called `http-sensor-ctl`. Please refer to the 
`--help` messages for details on how to run things.

## Contributing
To hack on this, follow the installation instructions. Tests are run via `tox`.
PRs are welcome!
