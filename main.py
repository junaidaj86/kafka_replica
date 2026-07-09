from backend.app.storage.topic_store import TopicStore
from backend.app.storage.log_store import LogStore
from backend.app.broker.topic import Topic
from pathlib import Path
from backend.app.broker.broker import Broker
from backend.app.partitioner.hash_partitioner import HashPartitioner
from backend.app.producer.producer import Producer
from backend.app.consumer.consumer import Consumer
from backend.app.broker.message import Message

def print_func():
    print("PyKafka Core starting...")
    topic = Topic(name="test-topic", partition_count=3, replication_factor=1, retention_ms=60000, retention_bytes=1048576)
    _consumer_topic = Topic(name="_consumer_topic", partition_count=3, replication_factor=1, retention_ms=60000, retention_bytes=1048576)
    topic_store = TopicStore(base_path=Path(__file__).parent / "data")
    log_store = LogStore(base_path=Path(__file__).parent / "data")
    broker = Broker(topic_store=topic_store, log_store=log_store)
    try:
        broker.create_topic(topic)
        broker.create_topic(_consumer_topic)
    except ValueError:
        pass
    producer = Producer(broker=broker, partitioner=HashPartitioner())
    producer.produce(topic_name="test-topic", key="key1", value="value1")
    producer.produce(topic_name="test-topic", key="key2", value="value2")
    producer.produce(topic_name="test-topic", key="key3", value="value3")
    producer.produce(topic_name="test-topic", key="key4", value="value4")
    producer.produce(topic_name="test-topic", key="key5", value="value5")
    
    
    consumer = Consumer(broker=broker, topic_name="test-topic")
    messages = consumer.poll()
    for message in messages:
        print(f"Consumed message: key={message.key}, value={message.value}, timestamp={message.timestamp}")
        


if __name__ == "__main__":
    print_func()
