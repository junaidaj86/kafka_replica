class Topic:
    def __init__(
        self,
        name: str,
        partition_count: int = 6,
        replication_factor: int = 3,
        retention_ms: int = 604800000,
        retention_bytes: int = 1073741824,
    ):
        if name is None or name.strip() == "":
            raise ValueError("Topic name cannot be empty.")
        if partition_count <= 0:
            raise ValueError("Number of partitions must be greater than zero.")
        if replication_factor <= 0:
            raise ValueError("Replication factor must be greater than zero.")
        if retention_ms <= 0:
            raise ValueError("Retention time must be greater than zero.")
        self.name = name
        self.partition_count = partition_count
        self.replication_factor = replication_factor
        self.retention_ms = retention_ms
        self.retention_bytes = retention_bytes
