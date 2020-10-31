#!/usr/bin/env python

import daemon
import logging
import os
import pathlib
import queue
import signal
import time
import yaml

from dataclasses import dataclass, field
from pidlockfile import PIDLockFile
from typing import Any


NAME = 'sensor'
PID_FILE_PATH = f'/tmp/http_{NAME}.pid'
LOG_FILE_PATH = f'/tmp/http_{NAME}.log'


@dataclass(order=True)
class PrioritizedUrl:
    data: Any = field(compare=False)
    priority: int = field(default=0)


class Sensors(object):

    def run(self):
        '''
        Defer some setup work that would belong in the ctor.
        As the ctor is called before daemonizing (we need to pass our signal
        handler to DaemonContext), doing this in the ctor would close our log
        file handle. Seems more awkward to wrestle the file handle from
        logging.
        '''
        self.log = logging.getLogger(NAME)
        self.log.setLevel(logging.INFO)
        fh = logging.FileHandler(LOG_FILE_PATH)
        # fh.setLevel(logging.INFO)
        fh.setLevel(logging.DEBUG)
        log_format = '%(asctime)s|%(levelname)s|%(message)s'
        fh.setFormatter(logging.Formatter(log_format))
        self.log.addHandler(fh)
        self.log.debug('Done setting up logging')

        self.queue = queue.PriorityQueue()

        self.start_sensors()

    def start_sensors(self):
        while True:
            '''
            start threadpool and hav them check a queue
            main thread reads a file once for url to check
            reread file on signal, ctl can add urls to file and signal
            urls plus interval (default 5s) is the workload
            once worker has finished scrape, re-enquue url with wait
            sort queue by wait
            results are added to queue and send to kafka or worker thread sends to
            kafka?
            cfg: url file, kafka endpoint, default kafka topics, num workers
            url file: url repeat schedule [kafka topics]

            use a prioroty queue with prio determined by projected next exec time,
            when enqueueing get now+repeat_interval
            '''
            self.log.info("sample INFO message")
            time.sleep(5)

    def handle_sigusr1(self, _signum, _frame):
        self.log.info('Received SIGUSR1, reloading config...')

    def handle_sigterm(self, _signum, _frame):
        self.log.info('Received SIGTERM, exiting...')
        os.remove(PID_FILE_PATH)
        exit(0)


def start():

    sensors = Sensors()
    sig_handlers = {
        signal.SIGUSR1: sensors.handle_sigusr1,
        signal.SIGTERM: sensors.handle_sigterm,
    }

    cwd = os.getcwd()
    print(f'setup done, starting daemon now with work_dir {cwd}')

    try:
        with daemon.DaemonContext(
            umask=0o002,
            pidfile=PIDLockFile(PID_FILE_PATH, timeout=5),
            signal_map=sig_handlers,
            working_directory=cwd,
        ):
            sensors.run()
    except Exception:
        print('caught expcetion')


def stop():
    pid = _get_pid(PID_FILE_PATH)
    if pid:
        os.kill(pid, signal.SIGTERM)
    else:
        print(f'{NAME}: NOT running')


def restart():
    stop()
    start()


def status():
    pid = _get_pid(PID_FILE_PATH)
    if pid:
        print(f'{NAME}: running as pid {pid}')
    else:
        print(f'{NAME}: NOT running')


def reload():
    pid = _get_pid(PID_FILE_PATH)
    if pid:
        os.kill(pid, signal.SIGUSR1)
    else:
        print(f'{NAME}: NOT running')


def _get_pid(pidfile_path):
    pidfile = PIDLockFile(pidfile_path)
    return pidfile.is_locked()
