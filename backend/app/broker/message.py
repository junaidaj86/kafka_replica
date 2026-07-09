from datetime import datetime


class Message:
    def __init__(
        self,
        key: str,
        value: str,
        timestamp: datetime,
        offset: int | None = None,
    ):
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.offset = offset

    def to_dict(self) -> dict:
        return {
            "offset": self.offset,
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
        }

    @staticmethod
    def from_dict(data: dict) -> "Message":
        return Message(
            key=data["key"],
            value=data["value"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            offset=data["offset"],
        )