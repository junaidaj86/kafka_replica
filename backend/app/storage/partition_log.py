from pathlib import Path
from backend.app.storage.segment import Segment
from backend.app.broker.message import Message


class PartitionLog:
    def __init__(
        self,
        topic_name: str,
        partition_id: int,
        directory: Path,
        segment_size: int,
        index_interval_bytes: int,
    ):
        if not topic_name or not topic_name.strip():
            raise ValueError("Topic name must not be empty.")

        if partition_id < 0:
            raise ValueError("Partition id must not be negative.")

        if segment_size <= 0:
            raise ValueError("Segment size cannot be zero or less")
        if index_interval_bytes <= 0:
            raise ValueError("index_interval_bytes must be greater than zero.")
        if not directory.exists() or not directory.is_dir():
            raise ValueError(f"Partition directory '{directory}' does not exist.")
        self.topic_name = topic_name.strip()
        self.partition_id = partition_id
        self.directory = directory
        self.segment_size = segment_size
        self.index_interval_bytes = index_interval_bytes
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
                    index_interval_bytes=self.index_interval_bytes,
                )
            )
        segments.sort(key=lambda segment: segment.base_offset)
        if not segments:
            raise ValueError(f"No log segments found in '{self.directory}'.")
        return segments

    def active_segment(self) -> Segment:
        return self.segments[-1]

    def find_segment_for_offset(self, offset: int) -> Segment:
        ### 0000005000, 0000006000, 0000007000
        if offset < 0:
            raise ValueError("Offset must not be negative.")
        selected = self.segments[0]
        if offset < selected.base_offset:
            raise ValueError(
                f"Offset {offset} is earlier than the earliest available "
                f"offset {selected.base_offset}."
            )
        for segment in self.segments:
            if segment.base_offset <= offset:
                selected = segment
            else:
                break
        return selected

    def append(self, message: Message) -> int:

        segment = self.active_segment()
        next_offset = segment.get_next_offset()
        if segment.should_roll(self.segment_size):
            self.roll(next_offset)
            segment = self.active_segment()
        message.offset = next_offset
        segment.append(message)
        return next_offset

    def read_from_offset(
        self,
        offset: int,
        max_records: int = 100,
    ) -> list[Message]:
        if offset < 0:
            raise ValueError("Offset must not be negative.")

        if max_records <= 0:
            raise ValueError("max_records must be greater than zero.")

        start_segment = self.find_segment_for_offset(offset)
        start_index = self.segments.index(start_segment)

        messages: list[Message] = []
        current_offset = offset

        for segment in self.segments[start_index:]:
            remaining = max_records - len(messages)

            if remaining <= 0:
                break

            segment_messages = segment.read_from_offset(
                offset=current_offset,
                max_records=remaining,
            )

            messages.extend(segment_messages)
            if len(messages) >= max_records:
                break
            if segment_messages:
                current_offset = segment_messages[-1].offset + 1

        return messages

    def roll(self, base_offset: int) -> Segment:
        if base_offset < 0:
            raise ValueError("Base offset must not be negative.")
        segment_path = self.directory / f"{base_offset:020d}.log"
        if segment_path.exists():
            raise ValueError(f"Segment with base offset {base_offset} already exists.")
        segment_path.touch()
        segment = Segment(
            base_offset=base_offset,
            file_path=segment_path,
            index_interval_bytes=self.index_interval_bytes,
        )
        self.segments.append(segment)
        self.segments.sort(key=lambda item: item.base_offset)
        return segment
