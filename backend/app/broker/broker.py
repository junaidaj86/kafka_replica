from typing import List

from backend.app.storage.topic_store import TopicStore
from backend.app.broker.topic import Topic
from backend.app.storage.log_manager import LogManager
from backend.app.broker.message import Message


class Broker:
    def __init__(self, topic_store: TopicStore, log_store: LogManager):
        self.topic_store = topic_store
        self.log_store = log_store

    def create_topic(self, topic: Topic) -> Topic:
        return self.topic_store.create_topic(topic)

    def produce(self, topic_name: str, partition_id: int, message: Message) -> int:
        topic = self.topic_store.get_topic(topic_name)
        self._validate_partition(topic, partition_id)
        return self.log_store.append(topic_name, partition_id, message)

    def consume(
        self, topic_name: str, partition_id: int, offset: int, max_records: int = 100
    ) -> List[Message]:
        topic = self.get_topic(topic_name)
        self._validate_partition(topic, partition_id)
        return self.log_store.read_from_offset(
            topic_name, partition_id, offset, max_records=max_records
        )

    def get_topic(self, topic_name: str) -> Topic:
        return self.topic_store.get_topic(topic_name)

    def _validate_partition(self, topic: Topic, partition_id: int) -> None:
        if partition_id < 0 or partition_id >= topic.partition_count:
            raise ValueError(
                f"Invalid partition ID {partition_id} for topic '{topic.name}'."
            )
