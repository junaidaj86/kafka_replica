from backend.app.broker.message import Message
from pathlib import Path
import json

class LogStore:
    
    def __init__(self, base_path: Path):
        if base_path is None:
            raise ValueError("Base path is not set.")
        self.base_path = base_path
    
    def append(self, topic_name: str, partition_id: int, message: Message) -> int:
        self._validate_partition_request(topic_name, partition_id)
        self._validate_message(message)
        partition_path = self.get_partition_path(topic_name, partition_id)
        next_offset = self.get_next_offset(topic_name, partition_id)
        message.offset = next_offset
        log_entry = json.dumps(message.to_dict()) + "\n"
        with partition_path.open("a", encoding="utf-8") as file:
            file.write(log_entry)
        return next_offset
    
    def read_from_offset(self, topic_name: str, partition_id: int, offset: int, max_records: int = 100) -> list[Message]:
        if offset < 0:
            raise ValueError("Offset must not be negative.")
        if max_records <= 0:
            raise ValueError("max_records must be greater than zero.")
        partition_path = self.get_partition_path(topic_name, partition_id)
        messages = []
        with partition_path.open("r", encoding="utf-8") as file:
            for line in file:
                record = json.loads(line.strip())
                if record["offset"] >= offset:
                    if len(messages) >= max_records:
                        break
                    message = Message.from_dict(record)
                    messages.append(message)
        return messages
    
    def get_next_offset(self, topic_name: str, partition_id: int) -> int:
        partition_path = self.get_partition_path(topic_name, partition_id)

        with partition_path.open("r", encoding="utf-8") as file:
            lines = file.readlines()

        if not lines:
            return 0
        
        last_line = lines[-1].strip()

        last_record = json.loads(last_line)

        last_offset = last_record["offset"]

        return last_offset + 1
    
    def get_partition_path(self, topic_name: str, partition_id: int) -> Path:
        self._validate_partition_request(topic_name, partition_id)
        partition_path = self.base_path / topic_name / f"partition-{partition_id}.log"
        if not partition_path.exists():
            raise ValueError(f"Partition log file '{partition_path}' does not exist.")
        return partition_path
    
    def _validate_partition_request(self, topic_name: str, partition_id: int) -> None:
        if topic_name is None or topic_name.strip() == "":
            raise ValueError("Topic name must not be empty.")

        if partition_id < 0:
            raise ValueError("Partition id must not be negative.")
        
    
    def _validate_message(self, message: Message) -> None:
        if message.key is None or message.key.strip() == "":
            raise ValueError("Message key must not be empty.")
        if message.value is None :
            raise ValueError("Message value must not be None.")
        if message.timestamp is None:
            raise ValueError("Message timestamp must not be None.")
        
   
        with file_path.open("rb") as file:
            file.seek(0, 2)
            file_size = file.tell()
            if file_size == 0:
                return None
            position = file_size - 1
            while position >= 0:
                file.seek(position)
                char = file.read(1)
                if char == b"\n" and position != file_size - 1:
                    break
                position -= 1
            line = file.readline().decode("utf-8").strip()
            return line if line else None