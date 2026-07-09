from .message import Message


class Partition:
    def __init__(self, partition_id: int):
        self.partition_id = partition_id
        self.messages: list[Message] = []

    def append_message(self, message: Message):
        self.messages.append(message)

    def read_message_from(self, offset: int):
        pass
