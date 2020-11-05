'''
This implements a Kafka consumer that writes messages to a databas
'''

import os
import pathlib
import pickle
import signal

from concurrent import futures

import daemon  # type: ignore
import psycopg2 as db  # type: ignore
import yaml

from kafka import KafkaConsumer  # type: ignore
from pidlockfile import PIDLockFile  # type: ignore

from common import helpers


NAME = 'kafka-db-writer'
PID_FILE_PATH = f'/tmp/{NAME}.pid'
LOG_FILE_PATH = f'/tmp/{NAME}.log'


class DBWriter:
    '''
    A writer that adds to tables via DB-API 2.0
    '''

    insert = '''INSERT INTO %s (url, status_code, elapsed, match)
    VALUES (%s, %s, %s, %s);
    '''
    create = '''CREATE TABLE %s (
    id SERIAL PRIMARY KEY,
    url TEXT,
    status_code INTEGER,
    elapsed TEXT,
    match INTEGER); '''

    def __init__(self, my_db, connect, tables, log):
        self.log = log
        self.conn = my_db.connect(connect)
        self.tables = tables
        for table_name in tables:
            with self.conn.cursor() as cur:
                try:
                    cur.execute(self.create % table_name)  # yikes
                    self.log.debug('Created table %s', table_name)
                except db.Error as error:
                    self.log.debug('Database raised %s', error)
                    self.log.debug('Table %s exists, skip create', table_name)

    def __del__(self):
        self.conn.close()

    def write_message(self, msg):
        with self.conn.cursor() as cursor:
            for table_name in self.tables:
                cursor.execute(self.insert, (table_name,) + msg)


def kafka_worker(config):
    '''
    Worker function that creates a DBWriter and a KafkaConsumer. Once both are
    created it subscribes to the Kafka topic and then enters a poll loop and
    listens for new messages on the topic. If any messages are delivered, they
    are written to the DB.
    '''
    try:
        log = config.get('log')
        log.debug('Kafka worker found logger')

        kafka_config = config.get('kafka_config')
        bootstrap_urls = kafka_config.get('bootstrap_urls', [])
        topic = kafka_config.get('topic', '')
        key_file = kafka_config.get('key_file')
        cert_file = kafka_config.get('cert_file')
        ca_file = kafka_config.get('ca_file')

        db_config = config.get('db_config')
        db_connect_string = db_config.get('connect')
        db_tables = db_config.get('tables')

        db_writer = DBWriter(db, db_connect_string, db_tables, log)

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_urls,
            ssl_keyfile=key_file,
            ssl_certfile=cert_file,
            ssl_cafile=ca_file,
            security_protocol='SSL',
            value_deserializer=pickle.loads,
            group_id='sensor-consumers',
            enable_auto_commit=True,  # we might wanna do out own committing
        )
        log.info('Successfully created KafkaProducer')

        log.info('Attempting to subscribe to topic %s', topic)
        consumer.subscribe(topics=[topic])

        while True:
            log.debug('Kafka worker entering poll loop')
            msg_dict = consumer.poll(timeout_ms=5000)
            log.debug('Kafka worker poll returned with %s', msg_dict)
            if topic in msg_dict:
                msgs = msg_dict[topic]
                log.info('Recevied %s messages on topic %s', len(msgs), topic)

                for msg in msgs:
                    # write to db and commit to kafka after
                    log.debug('Storing message %s)', msg)
                    db_writer.write_message(msg)

    except Exception as e:
        log.error('Kafka worker raised uncaught exception: %s',
                  getattr(e, "message", e))


class KafkaDBWriters:
    '''
    Create and manage a bunch of Kafka writers.
    '''

    # and create the kafka_config name
    kafka_config = None
    # and create the db_config name
    db_config = None
    # initialize log name
    log = None

    def __init__(self, args):
        self.args = args

    def run(self):
        '''
        Defer some setup work that would belong in the ctor.
        As the ctor is called before daemonizing (we need to pass our signal
        handler to DaemonContext), doing this in the ctor would close our log
        file handle. Seems more awkward to wrestle the file handle from
        logging in order to whitelist it..
        Catches Exception for logging when running as a daemon.
        '''
        self.log = helpers.setup_logging(NAME,
                                         self.args.log_level,
                                         LOG_FILE_PATH)

        self.load_config()

        try:
            self.start()
        except Exception as e:
            self.log.critical('Caught unhandled exception: %s', e)
            self.log.error('Exiting')
            exit(1)

    def load_config(self):
        '''
        (re)load the config file. This tries to be fault tolerant, e.g. the
        queue is only replaced with a new queue (containing the initial data
        set) if loading the urls doesn't raise an Exception.
        Catches Exception for logging when running as a daemon.
        '''
        conf = {}
        try:
            # read and parse the config file
            config_file_path = self.args.config
            config_file_location = pathlib.PosixPath(config_file_path).absolute()
            self.log.debug('loading config file at %s', config_file_path)
            try:
                with open(config_file_location) as file_:
                    conf = yaml.safe_load(file_)
            except yaml.YAMLError as e_yml:
                self.log.error('Can\'t reload config at %s: %s',
                               config_file_location, {e_yml})
                return
            except OSError as e_os:
                self.log.error('Can\'t open config at %s: %s',
                               config_file_location, e_os)
                return

            # attempt to create our Kafka producer
            self.kafka_config = conf.get('kafka', {})
            self.db_config = conf.get('db', {})
        except Exception as e:
            self.log.error('Caught unhandled exception during config load: '
                           '%s', getattr(e, "message", e))
            self.log.error('Due to error above config was only partially '
                           'reloaded: %s', conf)

    def _submit_work(self, thread_pool, worker_num, config):
        '''
        Only log and submit here
        '''
        self.log.debug('Starting kafka consumer %s', worker_num)
        return thread_pool.submit(kafka_worker, config)

    def start(self):
        '''
        Start our workers.
        So far this is a static worker pool.
        '''
        self.log.debug('Starting threadpool')
        # TODO make this self adjusting. consumers now if they have none or too
        # many partitions assigned. have them stop, return how many parts they
        # worked on and start noew threads accordingly...or not
        initial_consumer_count = 8
        with futures.ThreadPoolExecutor(max_workers=initial_consumer_count) as thread_pool:
            worker_context = {
                'log': self.log,
                'kafka_config': self.kafka_config,
                'db_config': self.db_config,
            }
            kafka_futures = [
                self._submit_work(thread_pool, i, worker_context) for i in
                range(initial_consumer_count)]

            while kafka_futures:
                done, _working = futures.wait(
                    kafka_futures,
                    return_when=futures.FIRST_COMPLETED)
                for k_future in done:
                    kafka_futures.remove(k_future)
                    self.log.info('kafka writer done')

                    # check the result and maybe queue up two new workers
                    if k_future.result:
                        pass
                    else:
                        self.log.error('kafka worker didn\'t have a partition'
                                       'assigned')

            self.log.info('Seems like we\'re done here, bye')

    def handle_sigusr1(self, _signum, _frame):
        '''
        Reload config on SIGUSR1
        '''
        self.log.info('Received SIGUSR1, reloading config...')
        self.load_config()

    def handle_sigterm(self, _signum, _frame):
        '''
        On SIGTERM remove pid file and log exit.
        '''
        self.log.info('Received SIGTERM, exiting...')
        os.remove(PID_FILE_PATH)
        exit(0)


def start(args):
    '''
    Daemon interface - start the dameon
    '''

    k_db_writers = KafkaDBWriters(args)
    sig_handlers = {
        signal.SIGUSR1: k_db_writers.handle_sigusr1,
        signal.SIGTERM: k_db_writers.handle_sigterm,
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
        k_db_writers.run()


def stop(_args):
    '''
    Daemon interface - stop the dameon
    '''
    pid = _get_pid(PID_FILE_PATH)
    if pid:
        os.kill(pid, signal.SIGTERM)
    else:
        print(f'{NAME}: NOT running')


def restart(args):
    '''
    Daemon interface - restart the dameon
    '''
    stop(args)
    start(args)


def status(_args):
    '''
    Daemon interface - check if the dameon is running
    '''
    pid = _get_pid(PID_FILE_PATH)
    if pid:
        print(f'{NAME}: running as pid {pid}')
    else:
        print(f'{NAME}: NOT running')


def reload(_args):
    '''
    Daemon interface - send SIGUSR1 to the dameon
    '''
    pid = _get_pid(PID_FILE_PATH)
    if pid:
        os.kill(pid, signal.SIGUSR1)
    else:
        print(f'{NAME}: NOT running')


def _get_pid(pidfile_path):
    pidfile = PIDLockFile(pidfile_path)
    return pidfile.is_locked()
