import json
from pathlib import Path

from backend.app.broker.message import Message


class Segment:
    def __init__(
        self,
        base_offset: int,
        file_path: Path,
        index_interval_bytes: int,
    ):
        if base_offset < 0:
            raise ValueError("Base offset must not be negative.")

        if file_path is None:
            raise ValueError("Segment file path must not be None.")

        if index_interval_bytes <= 0:
            raise ValueError(
                "index_interval_bytes must be greater than zero."
            )

        self.base_offset = base_offset
        self.file_path = file_path
        self.index_path = file_path.with_suffix(".index")
        self.index_interval_bytes = index_interval_bytes

        # TODO: Recover this value from the existing index and log
        # when implementing broker-startup recovery.
        self.bytes_since_last_index = 0

        if not self.file_path.exists():
            raise ValueError(
                f"Segment log file '{self.file_path}' does not exist."
            )

        self.index_path.touch(exist_ok=True)

    def append(self, message: Message) -> None:
        if message is None:
            raise ValueError("Message must not be None.")

        if message.offset is None:
            raise ValueError(
                "Message offset must be assigned before append."
            )

        if message.offset < self.base_offset:
            raise ValueError(
                f"Message offset {message.offset} cannot be lower than "
                f"segment base offset {self.base_offset}."
            )

        encoded_record = (
            json.dumps(message.to_dict()) + "\n"
        ).encode("utf-8")

        with self.file_path.open("ab") as log_file:
            position = log_file.tell()

            should_add_index = (
                position == 0
                or self.bytes_since_last_index
                >= self.index_interval_bytes
            )

            if should_add_index:
                self._append_index_entry(
                    offset=message.offset,
                    position=position,
                )
                self.bytes_since_last_index = 0

            log_file.write(encoded_record)

        self.bytes_since_last_index += len(encoded_record)

    def read_from_offset(
        self,
        offset: int,
        max_records: int,
    ) -> list[Message]:
        if offset < self.base_offset:
            raise ValueError(
                f"Offset {offset} is before segment base offset "
                f"{self.base_offset}."
            )

        if max_records <= 0:
            raise ValueError(
                "max_records must be greater than zero."
            )

        position = self.lookup_position(offset)
        messages: list[Message] = []

        with self.file_path.open("rb") as log_file:
            log_file.seek(position)

            for raw_line in log_file:
                if not raw_line.strip():
                    continue

                record = json.loads(
                    raw_line.decode("utf-8")
                )

                if record["offset"] < offset:
                    continue

                messages.append(
                    Message.from_dict(record)
                )

                if len(messages) >= max_records:
                    break

        return messages

    def get_next_offset(self) -> int:
        with self.file_path.open("r", encoding="utf-8") as file:
            lines = [
                line.strip()
                for line in file
                if line.strip()
            ]

        if not lines:
            return self.base_offset

        last_record = json.loads(lines[-1])

        return last_record["offset"] + 1

    def size_in_bytes(self) -> int:
        return self.file_path.stat().st_size

    def should_roll(self, max_segment_bytes: int) -> bool:
        if max_segment_bytes <= 0:
            raise ValueError(
                "max_segment_bytes must be greater than zero."
            )

        return self.size_in_bytes() >= max_segment_bytes

    def lookup_position(self, offset: int) -> int:
        if offset < self.base_offset:
            raise ValueError(
                f"Offset {offset} is before segment base offset "
                f"{self.base_offset}."
            )

        requested_relative_offset = (
            offset - self.base_offset
        )

        selected_position = 0

        with self.index_path.open(
            "r",
            encoding="utf-8",
        ) as index_file:
            for line in index_file:
                if not line.strip():
                    continue

                entry = json.loads(line)

                if (
                    entry["relative_offset"]
                    <= requested_relative_offset
                ):
                    selected_position = entry["position"]
                else:
                    break

        return selected_position

    def _append_index_entry(
        self,
        offset: int,
        position: int,
    ) -> None:
        if offset < self.base_offset:
            raise ValueError(
                "Offset cannot be lower than segment base offset."
            )

        if position < 0:
            raise ValueError(
                "Index position must not be negative."
            )

        index_entry = {
            "relative_offset": offset - self.base_offset,
            "position": position,
        }

        with self.index_path.open(
            "a",
            encoding="utf-8",
        ) as index_file:
            index_file.write(
                json.dumps(index_entry) + "\n"
            )