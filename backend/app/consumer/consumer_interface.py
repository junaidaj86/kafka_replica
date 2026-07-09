from abc import ABC, abstractmethod
from backend.app.broker.message import Message

class ConsumerInterface(ABC):
    @abstractmethod
    def consume(self, topic_name: str, partition_id: int, offset: int) -> Message:
        pass