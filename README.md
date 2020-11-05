![tox](https://github.com/jan--f/http_sensor/workflows/tox/badge.svg)

# http_sensor
A simple http sensor that makes http requests to configured URLs repeatedly with 
a sleep period between and a Kafka consumer, that writes to a database.

## Installation
To install this:

- clone this repo
- (optionally) setup a virtualenv
- run `python3 setup.py install`

## Usage
You get two daemons `sensor` and `consumer`.

Both are controlled via a script called `http-sensor-ctl`. Please refer to the 
`--help` messages for details on how to run things.

### Concurrency
Both daemons offer to run with multiple threads. For the `sensor` components it 
is recommended to run with a good number of threads, as a worker waits until its 
time to scrape a website.
For the `consumer` component this is trickier. Ideally its worker count 
corresponds to the number of partitions in a topic. It is not clear though how 
this can be made dynamic.
A `consumer` thread would have to check the current number of partitions and 
return periodically in order to give the daemon a chance to reschedule 
additional workers (or less for that matter).

## Contributing
To hack on this, follow the installation instructions. Tests are run via `tox`.
PRs are welcome!
