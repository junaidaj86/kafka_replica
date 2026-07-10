from pathlib import Path
from backend.app.storage.segment import Segment


class PartitionLog:
    def __init__(self, topic_name: str, partition_id: int, directory: Path):
        if not topic_name or not topic_name.strip():
            raise ValueError("Topic name must not be empty.")

        if partition_id < 0:
            raise ValueError("Partition id must not be negative.")

        if not directory.exists() or not directory.is_dir():
            raise ValueError(f"Partition directory '{directory}' does not exist.")
        self.topic_name = topic_name
        self.partition_id = partition_id
        self.directory = directory
        self.segments = self._load_segments()

    def _load_segments(self) -> list[Segment]:
        segments: list[Segment] = []
        for log_file in self.directory.glob("*.log"):
            try:
                base_offset = int(log_file.stem)
            except ValueError:
                continue
            segments.append(
                Segment(
                    base_offset=base_offset,
                    file_path=log_file,
                )
            )
        segments.sort(key=lambda segment: segment.base_offset)
        if not segments:
            raise ValueError(f"No log segments found in '{self.directory}'.")
        return segments

    def active_segment(self) -> Segment:
        return self.segments[-1]
