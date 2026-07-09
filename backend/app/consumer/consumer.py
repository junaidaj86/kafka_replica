from backend.app.broker.broker import Broker
from backend.app.broker.message import Message

class Consumer():
    def __init__(self, broker: Broker, topic_name: str, max_records: int = 100):
        self.broker = broker
        self.topic_name = topic_name  
        topic = self.broker.get_topic(topic_name)
        self.assigned_partitions = list(range(topic.partition_count))
        self.offsets: dict[int, int] = {
            partition_id: 0 for partition_id in self.assigned_partitions

        }
        self.max_records = max_records
        
    def poll(self):
        all_messages: list[Message] = []
        for partition_id in self.assigned_partitions:
            messages = self.broker.consume(
                    topic_name=self.topic_name, 
                    partition_id=partition_id,  # Consume from the current assigned partition
                    offset=self.offsets[partition_id],
                    max_records=self.max_records
            )
            if messages:
                self.offsets[partition_id] = messages[-1].offset + 1  # Increment the offset after consuming a message
                all_messages.extend(messages)
        return all_messages