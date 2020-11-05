'''
This implements a Kafka consumer that writes messages to a databas
'''

import os
import pickle
import signal
import time

from concurrent import futures

import daemon  # type: ignore
import psycopg2 as db  # type: ignore

from kafka import KafkaConsumer  # type: ignore
from pidlockfile import PIDLockFile  # type: ignore

from common import helpers


NAME = 'kafka-db-writer'


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
        '''
        Write message to all db tables
        '''
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

        log.debug('Kafka worker entering poll loop')
        while True:
            msg_dict = consumer.poll()
            log.debug('Kafka worker poll returned with %s', msg_dict)
            if topic in msg_dict:
                msgs = msg_dict[topic]

                log.info('Recevied %s messages on topic %s', len(msgs), topic)

                for msg in msgs:
                    # write to db and commit to kafka after
                    log.debug('Storing message %s)', msg)
                    db_writer.write_message(msg)
            else:
                log.info('Got no message, sleeping for a bit')
                time.sleep(1)

    except Exception as e:
        log.error('Kafka worker raised uncaught exception: %s',
                  getattr(e, "message", e))


class KafkaDBWriters(helpers.DaemonThreadRunner):
    '''
    Create and manage a bunch of Kafka writers.
    '''

    # and create the kafka_config name
    kafka_config = None
    # and create the db_config name
    db_config = None

    def load_config(self):
        '''
        (re)load the config file. This tries to be fault tolerant, e.g. the
        queue is only replaced with a new queue (containing the initial data
        set) if loading the urls doesn't raise an Exception.
        Catches Exception for logging when running as a daemon.
        '''
        conf = {}
        try:
            conf = helpers._load_and_parse_config_file(self.args.config,
                                                       self.log)

            # attempt to create our Kafka producer
            self.kafka_config = conf.get('kafka', {})
            self.db_config = conf.get('db', {})
        except Exception as e:
            self.log.error('Caught unhandled exception during config load: '
                           '%s', getattr(e, "message", e))
            self.log.error('Due to error above config was only partially '
                           'reloaded: %s', conf)

    def _submit_work(self, worker_num, config):
        '''
        Only log and submit here
        '''
        k_futures = []
        try:
            for _i in range(worker_num):
                self.log.debug('Starting kafka consumer %s', _i)
                k_futures.append(self.thread_pool.submit(kafka_worker, config))
        except RuntimeError:
            self.log.debug('thread_pool submit failed, maybe we\'re '
                           'shutting down')
        return k_futures

    def start(self):
        '''
        Start our workers.
        So far this is a static worker pool.
        '''
        self.log.debug('Starting threadpool')
        # TODO make this self adjusting. can consumers know if they have none
        # or too # many partitions assigned? have them stop, return how many
        # parts they worked on and start now threads accordingly...or not
        initial_consumer_count = 8
        worker_context = {
            'log': self.log,
            'kafka_config': self.kafka_config,
            'db_config': self.db_config,
        }
        kafka_futures = self._submit_work(initial_consumer_count,
                                          worker_context)

        while kafka_futures:
            # additional_workers = 0
            done, _working = futures.wait(
                kafka_futures,
                return_when=futures.FIRST_COMPLETED)
            for k_future in done:
                kafka_futures.remove(k_future)
                self.log.info('kafka writer done')

                # check the result and maybe queue up two new workers
                # additional_workers += k_future.result()
                # kafka_futures.extend(self._submit_work(additional_workers,
                #                                        worker_context))

        self.log.info('Seems like we\'re done here, bye')


def start(args):
    '''
    Daemon interface - start the dameon
    '''

    k_db_writers = KafkaDBWriters(NAME, args)
    sig_handlers = {
        signal.SIGUSR1: k_db_writers.handle_sigusr1,
    }

    cwd = os.getcwd()
    print(f'setup done, starting daemon now with work_dir {cwd}')

    with daemon.DaemonContext(
        umask=0o002,
        pidfile=PIDLockFile(args.pid_file, timeout=5),
        signal_map=sig_handlers,
        working_directory=cwd,
        prevent_core=False,
    ):
        k_db_writers.run()


def stop(args):
    '''
    Daemon interface - stop the dameon
    '''
    pid = _get_pid(args.pid_file)
    if pid:
        os.kill(pid, signal.SIGTERM)
        helpers._wait_for_shutdown_or_kill(pid)
    else:
        print(f'{NAME}: NOT running')


def restart(args):
    '''
    Daemon interface - restart the dameon
    '''
    stop(args)
    start(args)


def status(args):
    '''
    Daemon interface - check if the dameon is running
    '''
    pid = _get_pid(args.pid_file)
    if pid:
        print(f'{NAME}: running as pid {pid}')
    else:
        print(f'{NAME}: NOT running')


def reload(args):
    '''
    Daemon interface - send SIGUSR1 to the dameon
    '''
    pid = _get_pid(args.pid_file)
    if pid:
        os.kill(pid, signal.SIGUSR1)
    else:
        print(f'{NAME}: NOT running')


def _get_pid(pidfile_path):
    pidfile = PIDLockFile(pidfile_path)
    return pidfile.is_locked()
