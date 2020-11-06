'''
The kafka db writer has not too many interesting parts to test. The main
invariant for the worker is that it should only ever commit to kafka if the
message was successfully writen to the database.

Once a more flexible multithreading model is implemented, the rescheduling of
workers would be interesting as well.
'''


from unittest.mock import MagicMock, patch

import pytest

from kafka import KafkaConsumer

from kafka_db_writer import kafka_db_writer


@pytest.fixture
def worker_config():
    mock_config = {
        'log': MagicMock(),
        'kafka_config': MagicMock(topic='test'),
        'db_config': MagicMock(),
    }
    return mock_config


@patch('kafka_db_writer.kafka_db_writer.DBWriter', autospec=True)
@patch('kafka_db_writer.kafka_db_writer.KafkaConsumer', autospec=True)
def test_worker_no_commit_on_db_fail(mocked_kafka_consumer,
                                     mocked_db_writer,
                                     worker_config):
    mocked_kafka_consumer.return_value = MagicMock(poll={'test': 'foo'})
    mocked_db_writer.write_message.side_effect = ValueError
    kafka_db_writer.kafka_worker(worker_config)
    mocked_kafka_consumer.commit.assert_not_called()
