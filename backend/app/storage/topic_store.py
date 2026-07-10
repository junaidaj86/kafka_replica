import json
from pathlib import Path

from backend.app.broker.topic import Topic


class TopicStore:
    METADATA_FILE = "topic_metadata.json"
    INITIAL_SEGMENT_FILE = "00000000000000000000.log"

    def __init__(self, base_path: Path):
        if base_path is None:
            raise ValueError("Base path is not set.")

        self.base_path = base_path

    def create_topic(self, topic: Topic) -> Topic:

        metadata = {
            "name": topic.name,
            "partition_count": topic.partition_count,
            "replication_factor": topic.replication_factor,
            "retention_ms": topic.retention_ms,
            "retention_bytes": topic.retention_bytes,
        }
        metadata_path = self.base_path / self.METADATA_FILE
        with metadata_path.open("w", encoding="utf-8") as file:
            json.dump(metadata, file, indent=2)
        for partition_id in range(topic.partition_count):
            topic_path = self.base_path / f"{topic.name}-{partition_id}"
            if not topic_path.exists():
                topic_path.mkdir(parents=True)
            segment_path = topic_path / self.INITIAL_SEGMENT_FILE
            if not segment_path.exists():
                segment_path.touch()
        return topic

    def get_topic(self, topic_name: str) -> Topic:
        if topic_name is None or topic_name.strip() == "":
            raise ValueError("Topic name must not be empty.")
        topic_name = topic_name.strip()
        metadata_path = self.base_path / self.METADATA_FILE
        if not metadata_path.exists():
            raise ValueError(f"Metadata for topic '{topic_name}' is missing.")

        with metadata_path.open("r", encoding="utf-8") as file:
            metadata = json.load(file)
        return Topic(
            name=metadata["name"],
            partition_count=metadata["partition_count"],
            replication_factor=metadata["replication_factor"],
            retention_ms=metadata["retention_ms"],
            retention_bytes=metadata["retention_bytes"],
        )
