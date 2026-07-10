from backend.app.partitioner.hash_partitioner import HashPartitioner
from backend.app.broker.message import Message
from datetime import datetime
from backend.app.broker.broker import Broker


class Producer:
    def __init__(self, broker: Broker, partitioner: HashPartitioner):
        self.partitioner = partitioner
        self.broker = broker

    def produce(self, topic_name: str, key: str, value: str) -> int:
        message = Message(key=key, value=value, timestamp=datetime.now())
        topic = self.broker.get_topic(topic_name)
        partition_id = self.partitioner.get_partition(topic, message)
        return self.broker.produce(topic_name, partition_id, message)
