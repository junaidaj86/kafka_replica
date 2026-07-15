from backend.app.broker.message import Message
from pathlib import Path
from backend.app.storage.segment import Segment
from backend.app.storage.partition_log import PartitionLog


class LogManager:
    def __init__(
        self,
        base_path: Path,
        segment_size: int = 1_073_741_824,
        index_interval_bytes: int = 4096,
    ):
        if base_path is None:
            raise ValueError("Base path is not set.")
        if segment_size <= 0:
            raise ValueError("Segment size must be greater than zero.")

        if index_interval_bytes <= 0:
            raise ValueError("index_interval_bytes must be greater than zero.")
        self.base_path = base_path
        self.segment_size = segment_size
        self.logs: dict[tuple[str, int], PartitionLog] = {}
        self.index_interval_bytes = index_interval_bytes

    def append(self, topic_name: str, partition_id: int, message: Message) -> int:
        self._validate_partition_request(topic_name, partition_id)
        self._validate_message(message)
        partition_log = self.get_partition_log(topic_name, partition_id)
        return partition_log.append(message)

    def read_from_offset(
        self, topic_name: str, partition_id: int, offset: int, max_records: int = 100
    ) -> list[Message]:
        if offset < 0:
            raise ValueError("Offset must not be negative.")
        if max_records <= 0:
            raise ValueError("max_records must be greater than zero.")
        partition_log = self.get_partition_log(topic_name, partition_id)
        return partition_log.read_from_offset(offset=offset, max_records=max_records)

    def get_partition_directory(self, topic_name: str, partition_id: int) -> Path:
        self._validate_partition_request(topic_name, partition_id)
        partition_directory = self.base_path / f"{topic_name}-{partition_id}"
        if not partition_directory.exists():
            raise ValueError(
                f"Partition directory '{partition_directory}' does not exist."
            )
        return partition_directory

    def _validate_partition_request(self, topic_name: str, partition_id: int) -> None:
        if topic_name is None or topic_name.strip() == "":
            raise ValueError("Topic name must not be empty.")

        if partition_id < 0:
            raise ValueError("Partition id must not be negative.")

    def _validate_message(self, message: Message) -> None:
        if message.key is None or message.key.strip() == "":
            raise ValueError("Message key must not be empty.")
        if message.value is None:
            raise ValueError("Message value must not be None.")
        if message.timestamp is None:
            raise ValueError("Message timestamp must not be None.")

    def get_partition_log(self, topic_name: str, partition_id: int) -> PartitionLog:
        key = (topic_name, partition_id)
        if key not in self.logs:
            directory = self.get_partition_directory(topic_name, partition_id)
            self.logs[key] = PartitionLog(
                topic_name=topic_name,
                partition_id=partition_id,
                directory=directory,
                segment_size=self.segment_size,
                index_interval_bytes=self.index_interval_bytes,
            )
        return self.logs[key]

    def apply_retention(
        self,
        topic_name: str,
        partition_id: int,
        retention_ms: int,
        retention_bytes: int,
    ) -> list[Segment]:
        partition_log = self.get_partition_log(
            topic_name,
            partition_id,
        )

        return partition_log.apply_retention(
            retention_ms=retention_ms,
            retention_bytes=retention_bytes,
        )
