from pathlib import Path
from backend.app.broker.message import Message
import json


class Segment:
    def __init__(self, base_offset: int, file_path: Path):
        self.base_offset = base_offset
        self.file_path = file_path

    def append(self, message: Message) -> None:
        segment_entry = json.dumps(message.to_dict()) + "\n"
        with self.file_path.open("a", encoding="utf-8") as file:
            file.write(segment_entry)

    def read_from_offset(self, offset: int, max_records: int) -> list[Message]:
        messages: list[Message] = []
        with self.file_path.open("r", encoding="utf-8") as file:
            for line in file:
                record = json.loads(line.strip())
                if record["offset"] >= offset:
                    if len(messages) > max_records:
                        break
                    messages.append(Message.from_dict(record))

        return messages

    def get_next_offset(self) -> int:
        with self.file_path.open("r", encoding="utf-8") as file:
            lines = file.readlines()
        if not lines:
            return 0
        last_line = lines[-1].strip()
        last_record = json.loads(last_line)
        last_offset = last_record["offset"]
        return last_offset + 1

    def size_in_bytes(self) -> int:
        return self.file_path.stat().st_size

    def should_roll(self, max_segment_bytes: int) -> bool:
        if max_segment_bytes <= 0:
            raise ValueError("max_segment_bytes must be greater than zero.")

        return self.size_in_bytes() >= max_segment_bytes
