from backend.app.partitioner.partitioner import Partitioner
from backend.app.broker.topic import Topic
from backend.app.broker.message import Message


class RoundRobinPartitioner(Partitioner):
    def __init__(self):
        self._counter = 0

    def get_partition(
        self,
        topic: Topic,
        message: Message,
    ) -> int:
        partition_id = self._counter % topic.partition_count
        self._counter = (self._counter + 1) % topic.partition_count
        return partition_id
