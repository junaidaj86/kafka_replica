from backend.app.broker.message import Message
from pathlib import Path
import json
from backend.app.segment.segment import Segment
class LogStore:
    INITIAL_SEGMENT_FILE = "00000000000000000000.log"

    def __init__(self, base_path: Path):
        if base_path is None:
            raise ValueError("Base path is not set.")
        self.base_path = base_path
    
    def append(self, topic_name: str, partition_id: int, message: Message) -> int:
        self._validate_partition_request(topic_name, partition_id)
        self._validate_message(message)
        segment:Segment = self.get_active_segment(topic_name, partition_id)
        next_offset = segment.get_next_offset()
        message.offset = next_offset
        segment.append(message)
        return next_offset
    
    def read_from_offset(self, topic_name: str, partition_id: int, offset: int, max_records: int = 100) -> list[Message]:
        if offset < 0:
            raise ValueError("Offset must not be negative.")
        if max_records <= 0:
            raise ValueError("max_records must be greater than zero.")
        segment:Segment = self.get_active_segment(topic_name, partition_id)
        return segment.read_from_offset(offset, max_records)
        
    
    def get_partition_directory(self, topic_name: str, partition_id: int) -> Path:
        self._validate_partition_request(topic_name, partition_id)
        partition_directory = self.base_path / topic_name / f"partition-{partition_id}" 
        if not partition_directory.exists():
            raise ValueError(f"Partition directory '{partition_directory}' does not exist.")
        return partition_directory
    
    def get_active_segment(self, topic_name: str, partition_id: int) -> Segment:
        partition_directory = self.get_partition_directory(topic_name, partition_id)
        segment_path = partition_directory / self.INITIAL_SEGMENT_FILE
        if not segment_path.exists():
            raise ValueError(f"Active segment file '{segment_path}' does not exist.")
        return Segment(
            base_offset=0,
            file_path=segment_path,
        )
    
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
        
   
