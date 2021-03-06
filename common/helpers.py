'''
Some common helpers
'''
import logging
import os
import pathlib
import signal
import time

from concurrent import futures
from datetime import datetime
from random import randint

import yaml

from pidlockfile import PIDLockFile  # type: ignore


class DaemonThreadRunner:
    '''
    Runs threads in a daemon process
    '''

    # initialize log name
    log = None

    def __init__(self, name, args):
        self.name = name
        self.args = args
        self.thread_pool = futures.ThreadPoolExecutor(
            max_workers=args.max_workers)

    def __del__(self):
        self.log.info('Waiting on thread_pool shutdown')
        self.thread_pool.shutdown()

    def run(self):
        '''
        Defer some setup work that would belong in the ctor.
        As the ctor is called before daemonizing (we need to pass our signal
        handler to DaemonContext), doing this in the ctor would close our log
        file handle. Seems more awkward to wrestle the file handle from
        logging in order to whitelist it..
        Catches Exception for logging when running as a daemon.
        '''
        self.setup_logging()

        self.load_config()

        try:
            self.start()
        except Exception as e:
            self.log.critical('Caught unhandled exception: %s',
                              getattr(e, "message", e))
            self.log.error('Exiting')
            exit(1)

    def setup_logging(self):
        '''
        logging setup with the requested level.
        '''
        log = logging.getLogger(self.name)
        log.setLevel(getattr(logging, self.args.log_level, logging.INFO))
        f_handler = logging.FileHandler(self.args.log_file)
        f_handler.setLevel(getattr(logging, self.args.log_level, logging.INFO))
        log_format = '%(asctime)s|%(levelname)s|%(message)s'
        f_handler.setFormatter(logging.Formatter(log_format))
        log.addHandler(f_handler)
        log.debug('Done setting up logging')
        self.log = log

    def load_config(self):
        '''
        Load the config, to be implemented in child class
        '''
        raise NotImplementedError

    def start(self):
        '''
        Start the workers, to be implemented in child class
        '''
        raise NotImplementedError

    def handle_sigusr1(self, _signum, _frame):
        '''
        Reload config on SIGUSR1
        '''
        self.log.info('Received SIGUSR1, reloading config...')
        self.load_config()


def load_and_parse_config_file(config, log):
    '''
    Read and parse the config file
    '''
    config_file_path = config
    config_file_location = pathlib.PosixPath(config_file_path).absolute()
    log.debug('loading config file at %s', config_file_path)
    try:
        with open(config_file_location) as file_:
            return yaml.safe_load(file_)
    except yaml.YAMLError as e_yml:
        log.error('Can\'t reload config at %s: %s',
                  config_file_location, {e_yml})
    except OSError as e_os:
        log.error('Can\'t open config at %s: %s',
                  config_file_location, e_os)
        return {}


def wait_for_shutdown_or_kill(pid):
    '''
    Wait for a process to terminate or kill it after 5 seconds
    '''
    for _i in range(5):
        try:
            os.kill(pid, 0)
        except OSError:
            return
        time.sleep(1)
    os.kill(pid, signal.SIGKILL)


def get_pid(pidfile_path):
    '''
    Return a PID from a PIDLockFile
    '''
    pidfile = PIDLockFile(pidfile_path)
    return pidfile.is_locked()


def get_kafka_connection_details(kafka_conf):
    '''
    Extract Kafka connection details from dict kafka_conf
    '''
    con_details = {}
    con_details['bootstrap_servers'] = kafka_conf.get('bootstrap_urls', [])
    con_details['ssl_keyfile'] = kafka_conf.get('key_file')
    con_details['ssl_certfile'] = kafka_conf.get('cert_file')
    con_details['ssl_cafile'] = kafka_conf.get('ca_file')
    con_details['security_protocol'] = 'SSL'
    return con_details


def get_current_timestamp() -> float:
    '''
    Just return the timestamp of now. Easier to mock in testing
    '''
    return datetime.now().timestamp()


def randomize_start_time(data_item):
    '''
    Return radmon value between 0 and data_item['repeat']
    '''
    repeat = data_item.get('repeat', 0)
    now = int(get_current_timestamp())
    return randint(now, repeat + now)
