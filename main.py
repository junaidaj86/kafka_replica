from pathlib import Path
import shutil

from backend.app.broker.broker import Broker
from backend.app.broker.topic import Topic
from backend.app.consumer.consumer import Consumer
from backend.app.partitioner.hash_partitioner import HashPartitioner
from backend.app.producer.producer import Producer
from backend.app.storage.log_manager import LogManager
from backend.app.storage.topic_store import TopicStore


def main() -> None:
    print("PyKafka Core starting...")

    topic_name = "test-topic"
    partition_count = 3
    message_count = 12

    data_path = Path(__file__).parent / "data"

    # Test-only cleanup: start with an empty storage directory.
    if data_path.exists():
        shutil.rmtree(data_path)

    data_path.mkdir(parents=True, exist_ok=True)

    topic_store = TopicStore(base_path=data_path)

    # Small value used deliberately to trigger segment rolling quickly.
    log_manager = LogManager(
        base_path=data_path,
        segment_size=5000,
    )

    broker = Broker(
        topic_store=topic_store,
        log_store=log_manager,
    )

    topic = Topic(
        name=topic_name,
        partition_count=partition_count,
        replication_factor=1,
        retention_ms=60_000,
        retention_bytes=1_048_576,
    )

    broker.create_topic(topic)

    print("\n==============================")
    print("Testing LogManager Cache")
    print("==============================")

    partition_log_1 = log_manager.get_partition_log(
        topic_name="test-topic",
        partition_id=0,
    )

    partition_log_2 = log_manager.get_partition_log(
        topic_name="test-topic",
        partition_id=0,
    )

    print(f"Object 1 id : {id(partition_log_1)}")
    print(f"Object 2 id : {id(partition_log_2)}")

    if partition_log_1 is partition_log_2:
        print("PASS: Same PartitionLog instance reused.")
    else:
        print("FAIL: Different PartitionLog instances created.")

    print(f"Cached PartitionLogs: {len(log_manager.logs)}")

    print("Current cache:")

    for key, value in log_manager.logs.items():
        print(f"{key} -> PartitionLog(id={id(value)})")

    producer = Producer(
        broker=broker,
        partitioner=HashPartitioner(),
    )

    produced_offsets: list[int] = []

    print("\nProducing messages...")

    # The same key ensures that every message is routed to the same partition.
    for index in range(message_count):
        offset = producer.produce(
            topic_name=topic_name,
            key="customer-123",
            value=f"message-{index}-" + ("x" * 200),
        )

        produced_offsets.append(offset)

        print(f"Produced message-{index}: offset={offset}")

    print("\nInspecting partition directories...")

    populated_partition_id: int | None = None

    for partition_id in range(partition_count):
        partition_directory = data_path / f"{topic_name}-{partition_id}"

        segment_files = sorted(partition_directory.glob("*.log"))

        print(f"\nPartition {partition_id}:")

        for segment_file in segment_files:
            file_size = segment_file.stat().st_size

            print(f"  {segment_file.name} size={file_size} bytes")

            if file_size > 0:
                populated_partition_id = partition_id

    if populated_partition_id is None:
        raise RuntimeError("No populated partition was found.")

    populated_partition_directory = data_path / f"{topic_name}-{populated_partition_id}"

    rolled_segments = sorted(populated_partition_directory.glob("*.log"))

    print(f"\nMessages were written to partition {populated_partition_id}.")

    print(f"Number of segment files: {len(rolled_segments)}")

    assert len(rolled_segments) > 1, (
        "Segment rollover did not occur. Reduce segment_size or increase message size."
    )

    consumer = Consumer(
        broker=broker,
        topic_name=topic_name,
        max_records=100,
    )

    print("\nConsuming messages...")

    consumed_messages = consumer.poll()

    for message in consumed_messages:
        print(
            f"Consumed: "
            f"offset={message.offset}, "
            f"key={message.key}, "
            f"value={message.value[:30]}..."
        )

    consumed_offsets = [
        message.offset for message in consumed_messages if message.key == "customer-123"
    ]

    expected_offsets = list(range(message_count))

    print("\nVerification:")
    print(f"Produced offsets: {produced_offsets}")
    print(f"Consumed offsets: {consumed_offsets}")
    print(f"Segment files: {[path.name for path in rolled_segments]}")

    assert produced_offsets == expected_offsets, "Produced offsets are not continuous."

    assert consumed_offsets == expected_offsets, (
        "Consumer did not read all messages across segments."
    )

    print("\nSegment rollover test passed successfully.")


if __name__ == "__main__":
    main()
