#!/usr/bin/env python

import daemon # type: ignore
import logging
import os
import pathlib
import pickle
import re
import requests
import signal
import time
import yaml

from concurrent import futures
from dataclasses import dataclass, field
from datetime import datetime
from kafka import KafkaProducer # type: ignore
from pidlockfile import PIDLockFile # type: ignore
from queue import PriorityQueue, Empty as QueueEmpty


NAME = 'sensor'
PID_FILE_PATH = f'/tmp/http_{NAME}.pid'
LOG_FILE_PATH = f'/tmp/http_{NAME}.log'


@dataclass(order=True)
class PrioritizedUrl:
    data: dict = field(compare=False)
    priority: int = field(default=0)


def setup_logging(level):
    log = logging.getLogger(NAME)
    log.setLevel(getattr(logging, level, logging.INFO))
    fh = logging.FileHandler(LOG_FILE_PATH)
    # fh.setLevel(logging.INFO)
    fh.setLevel(getattr(logging, level, logging.INFO))
    log_format = '%(asctime)s|%(levelname)s|%(message)s'
    fh.setFormatter(logging.Formatter(log_format))
    log.addHandler(fh)
    log.debug('Done setting up logging')
    return log


# test if worker takes correct code paths on various errors
def worker(prio_url, config):
    '''
    This does the actual work.
    We send out http request, process the results, send it off to kafka.
    Returns the data item on sucess or an error.
    It is considered an error if we don't send anything to kafka. HTTP errors
    are generally sent to kafka. We don't send anything if no request is sent,
    e.g. the url is invalid.
    Returns a boolean signaling whether the work item should be rescheduled or
    not. E.g. we don't want to reschedule an invalid URL.
    '''
    log = config.get('log')
    log.debug('Worker found logger')
    try:
        wait_until = prio_url.priority
        url = prio_url.data['url']
        if wait_until:
            wait_time = wait_until - datetime.now().timestamp()
            log.info(f'Waiting {wait_time} seconds')
            time.sleep(wait_time)
        try:
            response = requests.get(url)
        except requests.exceptions.InvalidURL as e_url:
            log.error(f'Worker caught {e_url}, returning False')
            return False
        except requests.exceptions.RequestException as e_req:
            log.error(f'Worker caught {e_req.get("message", e_req)}')
            return True

        elapsed = response.elapsed.total_seconds()
        status_code = response.status_code
        log.debug(f'Request to {url} got response {status_code}, '
                  f'took {elapsed} seconds')

        regex_match = False
        if 'regex' in prio_url.data and prio_url.data['regex']:
            regex = prio_url.data['regex']
            matches = regex.search(response.text)
            if matches:
                log.debug(f'worker found regex in response from {url}')
                regex_match = True
            else:
                log.debug(f'worker regex NOT found in response from {url}')

        kafka_prod = config.get('kafka_producer')
        if kafka_prod:
            kafka_prod.send((url, status_code, elapsed, regex_match))
        return True
    except Exception as e:
        log.error(f'Worker raised uncaught exception: {e}, returning False')
        return False


# test that we never get more then max_num and that it returns on empty
def get_queue_slice(q, max_num=32):
    '''
    A generator that dequeues at most `max_num` items if available.
    If the queue has nothing available it will stop.
    '''
    n = 0
    while n < max_num:
        try:
            item = q.get_nowait()
            yield item
            n += 1
        except QueueEmpty:
            return


class Sensors(object):

    # make sure there is always a queue
    queue: PriorityQueue = PriorityQueue()
    # and create the kafka_prod name
    kafka_prod = None

    def __init__(self, args):
        self.args = args

    def run(self):
        '''
        Defer some setup work that would belong in the ctor.
        As the ctor is called before daemonizing (we need to pass our signal
        handler to DaemonContext), doing this in the ctor would close our log
        file handle. Seems more awkward to wrestle the file handle from
        logging in order to whitelist it..
        '''
        self.log = setup_logging(self.args.log_level)

        self.load_config()

        try:
            self.start_sensors()
        except Exception as e:
            self.log.critical(f'Caught unhandled exception: {getattr(e, "message", e)}')
            self.log.error('Exiting')
            exit(1)

    def load_config(self):
        conf = {}
        try:
            # read and parse the config file
            config_file_path = self.args.config
            config_file_location = pathlib.PosixPath(config_file_path).absolute()
            self.log.debug(f'loading config file at {config_file_path}')
            try:
                with open(config_file_location) as fd:
                    conf = yaml.safe_load(fd)
            except yaml.YAMLError as e_yml:
                self.log.error(f'Can\'t reload config at {config_file_location}: {e_yml}')
                return
            except OSError as e_os:
                self.log.error(f'Can\'t open config at {config_file_location}: {e_os}')
                return

            # attempt to load our urls and if successful populate the inital queue.
            urls = conf.get('urls', [])
            if not isinstance(urls, list):
                self.log.error('Expected a list under \'urls\' key, abort config load')
                return
            self.log.info(f'Found {len(urls)} urls, will process now')
            new_queue = PriorityQueue()
            for url in urls:
                if 'regex' in url and url['regex']:
                    # precompile regex if present
                    url['regex'] = re.compile(url['regex'])
                    # TODO maybe introduce a random element for first scheduling
                new_queue.put(PrioritizedUrl(url))
            self.log.info('loaded new queue from config, will start processing new queue')
            self.queue = new_queue

            # attempt to create our Kafka producer
            kafka_conf = conf.get('kafka', {})
            self.kafka_prod = MyKafkaProducer(kafka_conf, self.log)
        except Exception as e:
            self.log.error(f'Caught unhandled exception during config load: '
                           f'{getattr(e, "message", e)}')
            self.log.error(f'Due to error above config was only partially '
                           f'reloaded: {conf}')

    def _submit_work(self, thread_pool, data, config):
        '''
        Only log and submit here
        '''
        self.log.debug(f'Starting on work item {data}')
        return thread_pool.submit(worker, data, config)

    # test that things get rescheduled
    def start_sensors(self):
        '''
        Start a thread pool of sensor workers. Initially start as many workers
        as `get_queue_slice` returns. Then re-schedule workers as mor items
        become (or already are) available in the queue.
        When the queue doesn't return any more items, we consider ourselfes
        done.
        '''
        self.log.info('Starting threadpool')
        with futures.ThreadPoolExecutor() as thread_pool:
            worker_context = {
                'log': self.log,
                'kafka_producer': self.kafka_prod,
            }
            sensor_futures = {self._submit_work(thread_pool, data, worker_context): data
                              for data in get_queue_slice(self.queue)}

            while sensor_futures:
                done, _working = futures.wait(sensor_futures,
                                              return_when=futures.FIRST_COMPLETED)
                for f in done:
                    prioritized_url = sensor_futures.pop(f)
                    data = prioritized_url.data
                    self.log.info(f'sensor done for {data}')

                    # check the result and re-enqueue if successful
                    if f.result:
                        now = datetime.now().timestamp()
                        prio = now + data['repeat']
                        self.log.debug(f'Successful scrape for {data["url"]}, '
                                       f'requeueing with priority {prio}')
                        self.queue.put(PrioritizedUrl(data, prio))
                    else:
                        self.log.error(f'Scrape for {data} returned an error, '
                                       'dropping')

                for data in get_queue_slice(self.queue, len(done)):
                    f = thread_pool.submit(worker, data, worker_context)
                    sensor_futures[f] = data

            self.log.info('Seems like we\'re done here, bye')

    def handle_sigusr1(self, _signum, _frame):
        self.log.info('Received SIGUSR1, reloading config...')
        self.load_config()

    def handle_sigterm(self, _signum, _frame):
        self.log.info('Received SIGTERM, exiting...')
        os.remove(PID_FILE_PATH)
        exit(0)


def start(args):

    sensors = Sensors(args)
    sig_handlers = {
        signal.SIGUSR1: sensors.handle_sigusr1,
        signal.SIGTERM: sensors.handle_sigterm,
    }

    cwd = os.getcwd()
    print(f'setup done, starting daemon now with work_dir {cwd}')

    with daemon.DaemonContext(
        umask=0o002,
        pidfile=PIDLockFile(PID_FILE_PATH, timeout=5),
        signal_map=sig_handlers,
        working_directory=cwd,
        prevent_core=False,
    ):
        sensors.run()


def stop(args):
    pid = _get_pid(PID_FILE_PATH)
    if pid:
        os.kill(pid, signal.SIGTERM)
    else:
        print(f'{NAME}: NOT running')


def restart(args):
    stop(args)
    start(args)


def status(args):
    pid = _get_pid(PID_FILE_PATH)
    if pid:
        print(f'{NAME}: running as pid {pid}')
    else:
        print(f'{NAME}: NOT running')


def reload(args):
    pid = _get_pid(PID_FILE_PATH)
    if pid:
        os.kill(pid, signal.SIGUSR1)
    else:
        print(f'{NAME}: NOT running')


# TODO either implement or remove this and the argparser command
def add(args):
    pass


def _get_pid(pidfile_path):
    pidfile = PIDLockFile(pidfile_path)
    return pidfile.is_locked()
