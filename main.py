from pathlib import Path
from datetime import datetime, timezone
import shutil
import os

from backend.app.broker.broker import Broker
from backend.app.broker.topic import Topic
from backend.app.storage.topic_store import TopicStore
from backend.app.storage.log_manager import LogManager
from backend.app.producer.producer import Producer
from backend.app.partitioner.hash_partitioner import HashPartitioner


def main():
    data_path = Path(__file__).parent / "data"

    if data_path.exists():
        shutil.rmtree(data_path)

    data_path.mkdir(parents=True)

    topic_name = "orders"

    topic = Topic(
        name=topic_name,
        partition_count=3,
        replication_factor=1,
        retention_ms=60_000,
        retention_bytes=1_048_576,
    )

    topic_store = TopicStore(data_path)

    log_manager = LogManager(
        base_path=data_path,
        segment_size=1000,
        index_interval_bytes=300,
    )

    broker = Broker(
        topic_store=topic_store,
        log_store=log_manager,
    )

    broker.create_topic(topic)

    producer = Producer(
        broker=broker,
        partitioner=HashPartitioner(),
    )

    print("Producing messages...")

    for i in range(40):
        producer.produce(
            topic_name=topic_name,
            key="customer-1",
            value="x" * 180,
        )

    # --------------------------------------------------------
    # Find populated partition
    # --------------------------------------------------------

    partition_log = None

    for partition_id in range(topic.partition_count):
        current = log_manager.get_partition_log(
            topic_name,
            partition_id,
        )

        if current.total_size_in_bytes() > 0:
            partition_log = current
            break

    if partition_log is None:
        raise RuntimeError("No populated partition found.")

    print("\n============================")
    print("Before Time Retention")
    print("============================")

    for segment in partition_log.segments:
        print(
            segment.file_path.name,
            segment.size_in_bytes(),
        )

    print(
        "Segment Count:",
        len(partition_log.segments),
    )

    active_segment = partition_log.active_segment()

    # --------------------------------------------------------
    # Make every inactive segment old
    # --------------------------------------------------------

    now_ms = int(
        datetime.now(
            timezone.utc
        ).timestamp()
        * 1000
    )

    two_minutes_ago_seconds = (
        now_ms - 120_000
    ) / 1000

    for segment in partition_log.segments:
        if segment is active_segment:
            continue

        os.utime(
            segment.file_path,
            (
                two_minutes_ago_seconds,
                two_minutes_ago_seconds,
            ),
        )

        os.utime(
            segment.index_path,
            (
                two_minutes_ago_seconds,
                two_minutes_ago_seconds,
            ),
        )

    print("\nApplying Time Retention...")

    deleted_segments = (
        partition_log.delete_expired_segments(
            retention_ms=60_000,
            now_ms=now_ms,
        )
    )

    print(
        f"Deleted Segments: {len(deleted_segments)}"
    )

    print("\n============================")
    print("After Time Retention")
    print("============================")

    for segment in partition_log.segments:
        print(
            segment.file_path.name,
            segment.size_in_bytes(),
        )

    print(
        "Remaining Segment Count:",
        len(partition_log.segments),
    )

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    assert (
        active_segment
        in partition_log.segments
    )

    assert active_segment.file_path.exists()

    assert active_segment.index_path.exists()

    for segment in deleted_segments:
        assert (
            not segment.file_path.exists()
        )

        assert (
            not segment.index_path.exists()
        )

        assert (
            segment
            not in partition_log.segments
        )

    print("\nTime Retention Test Passed")


if __name__ == "__main__":
    main()