import asyncio
from types import SimpleNamespace

import pytest

from flink_jobs.replay.service import ReplayConfig, ReplayService


class DummyRecord(SimpleNamespace):
    pass


class DummyTopicPartition:
    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition

    def __hash__(self):
        return hash((self.topic, self.partition))

    def __eq__(self, other):
        return (
            isinstance(other, DummyTopicPartition)
            and self.topic == other.topic
            and self.partition == other.partition
        )


class DummyProducer:
    def __init__(self):
        self.messages = []
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send_and_wait(self, topic, value=None, key=None, headers=None):
        self.messages.append((topic, value, key, headers))


class DummyConsumer:
    def __init__(self, records, offsets):
        self.records = records
        self.offsets = offsets
        self.seek_calls = []
        self.seek_to_beginning_called = False
        self.partitions = []

    async def start(self):
        return

    async def stop(self):
        return

    async def partitions_for_topic(self, topic):
        return {0}

    async def assign(self, partitions):
        self.partitions = partitions

    async def offsets_for_times(self, mapping):
        return {tp: SimpleNamespace(offset=self.offsets.get(tp)) for tp in mapping}

    async def seek(self, tp, offset):
        self.seek_calls.append((tp, offset))

    async def seek_to_beginning(self, *tps):
        self.seek_to_beginning_called = True

    def __aiter__(self):
        self._iter = iter(self.records)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


@pytest.mark.asyncio
async def test_replay_service_timestamp_bounds(monkeypatch):
    topic_partition = DummyTopicPartition("prices.replay", 0)
    records = [
        DummyRecord(timestamp=1000, value=b"one", key=b"1", headers=None),
        DummyRecord(timestamp=2000, value=b"two", key=b"2", headers=None),
    ]

    dummy_consumer = DummyConsumer(records, {topic_partition: 5})
    dummy_producer = DummyProducer()

    monkeypatch.setattr(
        "flink_jobs.replay.service.AIOKafkaConsumer",
        lambda *args, **kwargs: dummy_consumer,
    )
    monkeypatch.setattr(
        "flink_jobs.replay.service.AIOKafkaProducer",
        lambda *args, **kwargs: dummy_producer,
    )
    monkeypatch.setattr(
        "flink_jobs.replay.service.TopicPartition",
        lambda topic, partition: DummyTopicPartition(topic, partition),
    )

    config = ReplayConfig(
        bootstrap_servers="kafka:9092",
        source_topic="prices.replay",
        target_topic="prices.raw",
        start_timestamp_ms=500,
        end_timestamp_ms=1500,
    )

    service = ReplayService(config)
    await service.run()

    assert dummy_producer.started and dummy_producer.stopped
    assert dummy_consumer.seek_calls[0][1] == 5  # offset derived from timestamp lookup
    # Only first record should be replayed due to end timestamp
    assert len(dummy_producer.messages) == 1
    assert dummy_producer.messages[0][1] == b"one"
