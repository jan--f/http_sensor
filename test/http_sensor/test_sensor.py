'''
Tests for the worker function. This mainly tests a few invariants that
should hold true:
    - return False iff: an invalid URL is passed or the outer try..catch
    catches something. All other failure cases should return true, as this
    causes the work item to be rescheduled.
    - time.sleep is called with non-negative number and only if the
    priority (or scheduled time) is in the future
    - a call to requests.get is made
    - if a KafkaProducer is passed, call its send function

'''


from queue import PriorityQueue
from unittest.mock import MagicMock, patch, create_autospec

import pytest
import hypothesis.strategies as st
from hypothesis import given, settings

from http_sensor import sensor


@pytest.fixture
def worker_config():
    mock_kafka = create_autospec(sensor.MyKafkaProducer)
    return MagicMock(), mock_kafka


@pytest.fixture
def data_item():
    return MagicMock(priority=1, data=MagicMock())


@patch('common.helpers.logging')
@patch('http_sensor.sensor.worker')
def test_sensor_no_rescheduling_all_false(mocked_worker, mocked_logging):
    mocked_worker.return_value = False
    args = MagicMock(
        max_workers=8,
        log_level='DEBUG'
    )
    sens = sensor.Sensors('test_sensor', args)
    sens.load_config = MagicMock()
    p_queue = PriorityQueue()
    mocked_data = {
        'repeat': 2,
        'url': 'foo'
    }
    [p_queue.put(sensor.PrioritizedUrl(mocked_data, 1)) for i in range(100)]
    sens.queue = p_queue
    sens.run()
    mocked_worker.assert_called()


@patch('common.helpers.logging')
@patch('http_sensor.sensor.worker')
def test_sensor_rescheduling_exactly_n(mocked_worker, mocked_logging):

    mocked_worker.side_effect = [True, False]
    args = MagicMock(
        max_workers=1,
        log_level='DEBUG'
    )
    sens = sensor.Sensors('test_sensor', args)
    sens.load_config = MagicMock()
    p_queue = PriorityQueue()
    mocked_data = {
        'repeat': 1,
        'url': 'foo'
    }
    p_queue.put(sensor.PrioritizedUrl(mocked_data, 1))
    sens.queue = p_queue
    sens.run()
    mocked_worker.assert_called()
    assert mocked_worker.call_count == 2


@patch('http_sensor.sensor.get_current_timestamp')
@patch('http_sensor.sensor.requests.get')
@given(sleep_until=st.floats(min_value=0.1))
def test_sleep_is_called_if_wait_time(_mock_get,
                                      mocked_now,
                                      worker_config,
                                      sleep_until):
    mocked_now.return_value = 0
    with patch('http_sensor.sensor.time.sleep') as mocked_sleep:
        mock_data_item = MagicMock(
            priority=sleep_until,
            data=MagicMock()
        )
        sensor.worker(mock_data_item, worker_config[0], worker_config[1])
        mocked_sleep.assert_called_once()


@patch('http_sensor.sensor.get_current_timestamp')
@patch('http_sensor.sensor.requests.get')
@given(sleep_until=st.floats(max_value=1))
def test_sleep_is_not_called_if_wait_time_is_negative(_mock_get,
                                                      mocked_now,
                                                      worker_config,
                                                      data_item,
                                                      sleep_until):
    mocked_now.return_value = 2
    with patch('http_sensor.sensor.time.sleep') as mocked_sleep:
        sensor.worker(data_item, worker_config[0], worker_config[1])
        mocked_sleep.assert_not_called()


@patch('http_sensor.sensor.requests.get')
def test_requests_get_is_called(mocked_get, worker_config, data_item):
    sensor.worker(data_item, worker_config[0], worker_config[1])
    mocked_get.assert_called_once()


@patch('http_sensor.sensor.requests.get')
def test_kafka_send_is_called_once(_mocked_get, worker_config, data_item):
    sensor.worker(data_item, worker_config[0], worker_config[1])
    worker_config[1].send.assert_called_once()


@patch('http_sensor.sensor.requests.get')
def test_kafka_send_not_called_when_no_producer(_mocked_get, data_item):
    mock_kafka = MagicMock(__bool__=MagicMock(return_value=False))
    sensor.worker(data_item, MagicMock(), mock_kafka)
    mock_kafka.send.assert_not_called()


def test_return_on_empty():
    test_queue = PriorityQueue()
    items = sensor.get_queue_slice(test_queue, 1)
    assert not list(items)


@given(queue_items=st.integers(min_value=0, max_value=10000),
       items_to_get=st.integers())
@settings(max_examples=500, deadline=None)
def test_dequeueing(queue_items: int,
                    items_to_get: int):
    test_queue: PriorityQueue = PriorityQueue()
    [test_queue.put((i, i)) for i in range(queue_items)]
    item_list = list(sensor.get_queue_slice(test_queue,
                                            max_num=items_to_get))

    assert len(item_list) == min(queue_items, max(0, items_to_get))
