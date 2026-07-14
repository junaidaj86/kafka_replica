from pathlib import Path
import shutil

from backend.app.broker.broker import Broker
from backend.app.broker.topic import Topic
from backend.app.consumer.consumer import Consumer
from backend.app.partitioner.hash_partitioner import HashPartitioner
from backend.app.producer.producer import Producer
from backend.app.storage.log_manager import LogManager
from backend.app.storage.topic_store import TopicStore


def find_populated_partition(
    data_path: Path,
    topic_name: str,
    partition_count: int,
) -> int:
    for partition_id in range(partition_count):
        partition_directory = data_path / f"{topic_name}-{partition_id}"

        if any(
            log_file.stat().st_size > 0
            for log_file in partition_directory.glob("*.log")
        ):
            return partition_id

    raise RuntimeError("No populated partition was found.")


def main() -> None:
    print("PyKafka Core starting...")

    topic_name = "test-topic"
    partition_count = 3
    initial_message_count = 20

    data_path = Path(__file__).parent / "data"

    # Clean only before the first broker startup.
    if data_path.exists():
        shutil.rmtree(data_path)

    data_path.mkdir(parents=True, exist_ok=True)

    topic_store = TopicStore(base_path=data_path)

    first_log_manager = LogManager(
        base_path=data_path,
        segment_size=2_000,
        index_interval_bytes=400,
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

    print("\nProducing records before corruption...")

    produced_offsets: list[int] = []

    for index in range(initial_message_count):
        offset = first_producer.produce(
            topic_name=topic_name,
            key="customer-123",
            value=f"message-{index}-" + ("x" * 150),
        )

        produced_offsets.append(offset)
        print(f"Produced offset={offset}")

    populated_partition_id = find_populated_partition(
        data_path=data_path,
        topic_name=topic_name,
        partition_count=partition_count,
    )

    partition_directory = data_path / f"{topic_name}-{populated_partition_id}"

    log_files = sorted(partition_directory.glob("*.log"))

    index_files = sorted(partition_directory.glob("*.index"))

    if not log_files:
        raise RuntimeError("No log files were created.")

    if not index_files:
        raise RuntimeError("No index files were created.")

    # Use the active segment for this recovery test.
    active_log_path = log_files[-1]
    active_index_path = active_log_path.with_suffix(".index")

    if not active_index_path.exists():
        raise RuntimeError(f"Expected index file '{active_index_path}' was not found.")

    valid_index_size = active_index_path.stat().st_size

    print("\nBefore corruption:")
    print(f"Partition: {populated_partition_id}")
    print(f"Active log: {active_log_path.name}")
    print(f"Active index: {active_index_path.name}")
    print(f"Valid index size: {valid_index_size} bytes")

    assert valid_index_size % 8 == 0, (
        "Index was already structurally invalid before the test."
    )

    # ---------------------------------------------------------------
    # Simulate a broker crash during an index-entry write.
    # A complete entry is 8 bytes, so append only 3 bytes.
    # ---------------------------------------------------------------
    print("\nInjecting truncated index bytes...")

    with active_index_path.open("ab") as index_file:
        index_file.write(b"\x01\x02\x03")

    corrupted_index_size = active_index_path.stat().st_size

    print(f"Corrupted index size: {corrupted_index_size} bytes")
    print(f"Remainder: {corrupted_index_size % 8}")

    assert corrupted_index_size % 8 == 3

    # ---------------------------------------------------------------
    # Simulate broker restart.
    # Do not delete data.
    # ---------------------------------------------------------------
    print("\n==============================")
    print("Simulating Broker Restart")
    print("==============================")

    recovered_topic_store = TopicStore(
        base_path=data_path,
    )

    recovered_log_manager = LogManager(
        base_path=data_path,
        segment_size=2_000,
        index_interval_bytes=400,
    )

    recovered_broker = Broker(
        topic_store=recovered_topic_store,
        log_store=recovered_log_manager,
    )

    # Loading the PartitionLog causes Segment construction and recovery.
    recovered_partition_log = recovered_log_manager.get_partition_log(
        topic_name=topic_name,
        partition_id=populated_partition_id,
    )

    recovered_active_segment = recovered_partition_log.active_segment()

    repaired_index_path = recovered_active_segment.index_path

    repaired_index_size = repaired_index_path.stat().st_size

    print("\nAfter recovery:")
    print(f"Repaired index: {repaired_index_path.name}")
    print(f"Repaired index size: {repaired_index_size} bytes")
    print(f"bytes_since_last_index: {recovered_active_segment.bytes_since_last_index}")

    assert repaired_index_size % 8 == 0, (
        "Recovered index size is not aligned to 8-byte entries."
    )

    assert repaired_index_size >= valid_index_size, (
        "Recovered index lost valid entries."
    )

    # ---------------------------------------------------------------
    # Verify indexed reads still work after repair.
    # ---------------------------------------------------------------
    target_offset = 7

    recovered_messages = recovered_log_manager.read_from_offset(
        topic_name=topic_name,
        partition_id=populated_partition_id,
        offset=target_offset,
        max_records=5,
    )

    recovered_offsets = [message.offset for message in recovered_messages]

    expected_recovered_offsets = list(range(target_offset, target_offset + 5))

    print("\nRead verification:")
    print(f"Expected: {expected_recovered_offsets}")
    print(f"Actual:   {recovered_offsets}")

    assert recovered_offsets == expected_recovered_offsets, (
        "Indexed reads failed after index repair."
    )

    # ---------------------------------------------------------------
    # Verify append continues with the correct next offset.
    # ---------------------------------------------------------------
    recovered_producer = Producer(
        broker=recovered_broker,
        partitioner=HashPartitioner(),
    )

    expected_next_offset = produced_offsets[-1] + 1

    next_offset = recovered_producer.produce(
        topic_name=topic_name,
        key="customer-123",
        value="after-index-recovery-" + ("y" * 150),
    )

    print("\nAppend verification:")
    print(f"Expected next offset: {expected_next_offset}")
    print(f"Actual next offset:   {next_offset}")

    assert next_offset == expected_next_offset, (
        "Offset did not continue correctly after recovery."
    )

    # ---------------------------------------------------------------
    # Verify all messages remain readable.
    # ---------------------------------------------------------------
    recovered_consumer = Consumer(
        broker=recovered_broker,
        topic_name=topic_name,
        max_records=100,
    )

    all_messages = recovered_consumer.poll()

    all_offsets = [
        message.offset for message in all_messages if message.key == "customer-123"
    ]

    expected_all_offsets = list(range(initial_message_count + 1))

    print("\nComplete-log verification:")
    print(f"Expected offsets: {expected_all_offsets}")
    print(f"Actual offsets:   {all_offsets}")

    assert all_offsets == expected_all_offsets, (
        "Records were lost or duplicated after index recovery."
    )

    print("\nTruncated index recovery test passed successfully.")


if __name__ == "__main__":
    main()
