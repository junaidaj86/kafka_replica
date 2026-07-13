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
    initial_message_count = 12

    data_path = Path(__file__).parent / "data"

    # Clean only before the initial broker startup.
    if data_path.exists():
        shutil.rmtree(data_path)

    data_path.mkdir(parents=True, exist_ok=True)

    topic_store = TopicStore(base_path=data_path)

    first_log_manager = LogManager(
        base_path=data_path,
        segment_size=1_500,
        index_interval_bytes=500,
    )

    first_broker = Broker(
        topic_store=topic_store,
        log_store=first_log_manager,
    )

    topic = Topic(
        name=topic_name,
        partition_count=partition_count,
        replication_factor=1,
        retention_ms=60_000,
        retention_bytes=1_048_576,
    )

    first_broker.create_topic(topic)

    first_producer = Producer(
        broker=first_broker,
        partitioner=HashPartitioner(),
    )

    print("\nProducing before restart...")

    produced_offsets_before_restart: list[int] = []

    for index in range(initial_message_count):
        offset = first_producer.produce(
            topic_name=topic_name,
            key="customer-123",
            value=f"before-restart-{index}-" + ("x" * 200),
        )

        produced_offsets_before_restart.append(offset)
        print(f"Produced before restart: offset={offset}")

    populated_partition_id: int | None = None

    for partition_id in range(partition_count):
        partition_directory = data_path / f"{topic_name}-{partition_id}"

        if any(
            segment_file.stat().st_size > 0
            for segment_file in partition_directory.glob("*.log")
        ):
            populated_partition_id = partition_id
            break

    if populated_partition_id is None:
        raise RuntimeError("No populated partition was found.")

    original_partition_log = first_log_manager.get_partition_log(
        topic_name=topic_name,
        partition_id=populated_partition_id,
    )

    original_active_segment = original_partition_log.active_segment()

    bytes_before_restart = (
        original_active_segment.bytes_since_last_index
    )

    active_segment_before_restart = (
        original_active_segment.file_path.name
    )

    print("\nBefore restart:")
    print(f"Partition: {populated_partition_id}")
    print(f"Active segment: {active_segment_before_restart}")
    print(
        "bytes_since_last_index: "
        f"{bytes_before_restart}"
    )

    # ------------------------------------------------------------------
    # Simulated broker restart
    # ------------------------------------------------------------------
    print("\n==============================")
    print("Simulating Broker Restart")
    print("==============================")

    # Do not delete any files.
    # Create completely new runtime objects using the same data directory.
    recovered_topic_store = TopicStore(
        base_path=data_path,
    )

    recovered_log_manager = LogManager(
        base_path=data_path,
        segment_size=1_500,
        index_interval_bytes=500,
    )

    recovered_broker = Broker(
        topic_store=recovered_topic_store,
        log_store=recovered_log_manager,
    )

    recovered_producer = Producer(
        broker=recovered_broker,
        partitioner=HashPartitioner(),
    )

    recovered_partition_log = (
        recovered_log_manager.get_partition_log(
            topic_name=topic_name,
            partition_id=populated_partition_id,
        )
    )

    recovered_active_segment = (
        recovered_partition_log.active_segment()
    )

    bytes_after_restart = (
        recovered_active_segment.bytes_since_last_index
    )

    active_segment_after_restart = (
        recovered_active_segment.file_path.name
    )

    print("\nAfter restart:")
    print(f"Active segment: {active_segment_after_restart}")
    print(
        "bytes_since_last_index: "
        f"{bytes_after_restart}"
    )

    assert (
        active_segment_before_restart
        == active_segment_after_restart
    ), "Recovered active segment does not match."

    assert (
        bytes_before_restart
        == bytes_after_restart
    ), "bytes_since_last_index was not recovered correctly."

    expected_next_offset = (
        produced_offsets_before_restart[-1] + 1
    )

    print("\nProducing after restart...")

    next_offset = recovered_producer.produce(
        topic_name=topic_name,
        key="customer-123",
        value="after-restart-message-" + ("y" * 200),
    )

    print(f"Produced after restart: offset={next_offset}")

    assert next_offset == expected_next_offset, (
        f"Expected offset {expected_next_offset}, "
        f"but received {next_offset}."
    )

    recovered_consumer = Consumer(
        broker=recovered_broker,
        topic_name=topic_name,
        max_records=100,
    )

    consumed_messages = recovered_consumer.poll()

    consumed_offsets = [
        message.offset
        for message in consumed_messages
        if message.key == "customer-123"
    ]

    expected_offsets = list(
        range(initial_message_count + 1)
    )

    print("\nConsumed offsets after restart:")
    print(consumed_offsets)

    assert consumed_offsets == expected_offsets, (
        "Records before and after restart were not read correctly."
    )

    print("\nBroker restart recovery test passed successfully.")


if __name__ == "__main__":
    main()