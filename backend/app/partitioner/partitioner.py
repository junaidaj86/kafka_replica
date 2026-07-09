from abc import ABC, abstractmethod
from backend.app.broker.message import Message

class Partitioner(ABC):
    @abstractmethod
    def get_partition(self, topic_name: str, message: Message,) -> int:
        pass