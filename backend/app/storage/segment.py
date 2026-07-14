import io
import json
import struct
from pathlib import Path

from backend.app.broker.message import Message


class Segment:
    INDEX_ENTRY_FORMAT = ">II"
    INDEX_ENTRY_SIZE = struct.calcsize(INDEX_ENTRY_FORMAT)

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

        if not self.file_path.exists():
            raise ValueError(
                f"Segment log file '{self.file_path}' does not exist."
            )

        self._recover_bytes_since_last_index()

        self.bytes_since_last_index = (
            self._recover_bytes_since_last_index()
        )

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

                try:
                    record = json.loads(
                        raw_line.decode("utf-8")
                    )
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        f"Corrupt log record in '{self.file_path}'."
                    ) from exc

                if record["offset"] < offset:
                    continue

                messages.append(
                    Message.from_dict(record)
                )

                if len(messages) >= max_records:
                    break

        return messages

    def get_next_offset(self) -> int:
        with self.file_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            lines = [
                line.strip()
                for line in file
                if line.strip()
            ]

        if not lines:
            return self.base_offset

        try:
            last_record = json.loads(lines[-1])
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Corrupt final record in '{self.file_path}'."
            ) from exc

        return last_record["offset"] + 1

    def size_in_bytes(self) -> int:
        return self.file_path.stat().st_size

    def should_roll(
        self,
        max_segment_bytes: int,
    ) -> bool:
        if max_segment_bytes <= 0:
            raise ValueError(
                "max_segment_bytes must be greater than zero."
            )

        return (
            self.size_in_bytes()
            >= max_segment_bytes
        )

    def lookup_position(self, offset: int) -> int:
        if offset < self.base_offset:
            raise ValueError(
                f"Offset {offset} is before segment base offset "
                f"{self.base_offset}."
            )

        requested_relative_offset = (
            offset - self.base_offset
        )

        index_size = self.index_path.stat().st_size

        if index_size == 0:
            return 0

        if index_size % self.INDEX_ENTRY_SIZE != 0:
            raise ValueError(
                f"Corrupt index file '{self.index_path}'."
            )

        entry_count = (
            index_size // self.INDEX_ENTRY_SIZE
        )

        low = 0
        high = entry_count - 1
        selected_position = 0

        with self.index_path.open("rb") as index_file:
            while low <= high:
                middle = (low + high) // 2

                index_file.seek(
                    middle * self.INDEX_ENTRY_SIZE
                )

                raw_entry = index_file.read(
                    self.INDEX_ENTRY_SIZE
                )

                if (
                    len(raw_entry)
                    != self.INDEX_ENTRY_SIZE
                ):
                    raise ValueError(
                        f"Corrupt index entry in "
                        f"'{self.index_path}'."
                    )

                relative_offset, position = (
                    struct.unpack(
                        self.INDEX_ENTRY_FORMAT,
                        raw_entry,
                    )
                )

                if (
                    relative_offset
                    <= requested_relative_offset
                ):
                    selected_position = position
                    low = middle + 1
                else:
                    high = middle - 1

        return selected_position

    def recover_index(self) -> None:
        if self.index_path.exists():
            return

        self.index_path.touch()

        bytes_since_last_index = 0

        with self.file_path.open("rb") as log_file:
            while True:
                position = log_file.tell()
                raw_line = log_file.readline()

                if not raw_line:
                    break

                if not raw_line.strip():
                    continue

                try:
                    record = json.loads(
                        raw_line.decode("utf-8")
                    )
                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                ) as exc:
                    raise ValueError(
                        f"Cannot rebuild index because "
                        f"log file '{self.file_path}' "
                        f"contains a corrupt record."
                    ) from exc

                message = Message.from_dict(record)

                if message.offset is None:
                    raise ValueError(
                        f"Cannot rebuild index because "
                        f"a record in '{self.file_path}' "
                        f"has no offset."
                    )

                should_add_index = (
                    position == 0
                    or bytes_since_last_index
                    >= self.index_interval_bytes
                )

                if should_add_index:
                    self._append_index_entry(
                        offset=message.offset,
                        position=position,
                    )
                    bytes_since_last_index = 0

                bytes_since_last_index += len(
                    raw_line
                )

    def _recover_bytes_since_last_index(
        self,
    ) -> int:
        log_size = self.file_path.stat().st_size
        index_size = self.index_path.stat().st_size

        if log_size == 0:
            return 0

        if index_size == 0:
            return log_size

        if (
            index_size % self.INDEX_ENTRY_SIZE
            != 0
        ):
            raise ValueError(
                f"Corrupt index file '{self.index_path}'."
            )

        with self.index_path.open("rb") as index_file:
            index_file.seek(
                -self.INDEX_ENTRY_SIZE,
                io.SEEK_END,
            )

            raw_entry = index_file.read(
                self.INDEX_ENTRY_SIZE
            )

        if len(raw_entry) != self.INDEX_ENTRY_SIZE:
            raise ValueError(
                f"Corrupt final index entry in "
                f"'{self.index_path}'."
            )

        _, last_index_position = struct.unpack(
            self.INDEX_ENTRY_FORMAT,
            raw_entry,
        )

        if last_index_position > log_size:
            raise ValueError(
                f"Index position {last_index_position} "
                f"exceeds log size {log_size}."
            )

        return log_size - last_index_position

    def _append_index_entry(
        self,
        offset: int,
        position: int,
    ) -> None:
        if position < 0:
            raise ValueError(
                "Log position must not be negative."
            )

        if offset < self.base_offset:
            raise ValueError(
                "Offset cannot be lower than "
                "segment base offset."
            )

        relative_offset = (
            offset - self.base_offset
        )

        if relative_offset > 0xFFFFFFFF:
            raise ValueError(
                "Relative offset exceeds the "
                "supported index range."
            )

        if position > 0xFFFFFFFF:
            raise ValueError(
                "Log position exceeds the "
                "supported index range."
            )

        entry = struct.pack(
            self.INDEX_ENTRY_FORMAT,
            relative_offset,
            position,
        )

        with self.index_path.open(
            "ab",
        ) as index_file:
            index_file.write(entry)
            
    def repair_truncated_index(self) -> None:
        if not self.index_path.exists():
            self.recover_index()
            return

        index_size = self.index_path.stat().st_size
        remainder = index_size % self.INDEX_ENTRY_SIZE

        if remainder == 0:
            return

        valid_index_size = index_size - remainder

        if valid_index_size == 0:
            self.index_path.unlink(missing_ok=True)
            self.recover_index()
            return

        last_entry_position = (
            valid_index_size - self.INDEX_ENTRY_SIZE
        )

        with self.index_path.open("rb") as index_file:
            index_file.seek(last_entry_position)
            raw_entry = index_file.read(
                self.INDEX_ENTRY_SIZE
            )

        if len(raw_entry) != self.INDEX_ENTRY_SIZE:
            self._rebuild_index_from_scratch()
            return

        relative_offset, log_position = struct.unpack(
            self.INDEX_ENTRY_FORMAT,
            raw_entry,
        )

        log_size = self.file_path.stat().st_size

        if log_position >= log_size:
            self._rebuild_index_from_scratch()
            return

        with self.file_path.open("rb") as log_file:
            log_file.seek(log_position)
            indexed_raw_line = log_file.readline()

            if not indexed_raw_line:
                self._rebuild_index_from_scratch()
                return

            try:
                indexed_record = json.loads(
                    indexed_raw_line.decode("utf-8")
                )
            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
            ):
                self._rebuild_index_from_scratch()
                return

            expected_offset = (
                self.base_offset + relative_offset
            )

            if indexed_record["offset"] != expected_offset:
                self._rebuild_index_from_scratch()
                return

        with self.index_path.open("r+b") as index_file:
            index_file.truncate(valid_index_size)

        bytes_since_last_index = len(indexed_raw_line)

        with self.file_path.open("rb") as log_file:
            log_file.seek(
                log_position + len(indexed_raw_line)
            )

            while True:
                position = log_file.tell()
                raw_line = log_file.readline()

                if not raw_line:
                    break

                if not raw_line.strip():
                    continue

                try:
                    record = json.loads(
                        raw_line.decode("utf-8")
                    )
                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                ) as exc:
                    raise ValueError(
                        f"Cannot repair index because "
                        f"'{self.file_path}' contains "
                        f"a corrupt record."
                    ) from exc

                message = Message.from_dict(record)

                if message.offset is None:
                    raise ValueError(
                        f"Record in '{self.file_path}' "
                        f"has no offset."
                    )

                if (
                    bytes_since_last_index
                    >= self.index_interval_bytes
                ):
                    self._append_index_entry(
                        offset=message.offset,
                        position=position,
                    )
                    bytes_since_last_index = 0

                bytes_since_last_index += len(raw_line)
                
    def _rebuild_index_from_scratch(self) -> None:
        self.index_path.unlink(missing_ok=True)
        self.recover_index()