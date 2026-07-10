from backend.app.partitioner.partitioner import Partitioner
from backend.app.broker.topic import Topic
from backend.app.broker.message import Message
import hashlib


class HashPartitioner(Partitioner):
    """
    Deterministically maps messages with the same key
    to the same partition.
    """

    def __init__(self):
        pass

    def get_partition(
        self,
        topic: Topic,
        message: Message,
    ) -> int:
        if message.key is None:
            raise ValueError("HashPartitioner requires a message key.")
        # Use the hash of the message key to determine the partition
        key_hash = int(hashlib.sha256(message.key.encode("utf-8")).hexdigest(), 16)
        return key_hash % topic.partition_count
