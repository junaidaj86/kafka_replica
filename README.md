Domain and Component Responsibilities

Topic

Represents the logical configuration of a Kafka topic.

Owns:

* Topic name
* Partition count
* Replication factor
* Retention time
* Retention size

Does not own:

* Partition directories
* Segment files
* Messages
* Producer or consumer behavior

⸻

Message

Represents one record stored in a topic partition.

Owns:

* Key
* Value
* Timestamp
* Offset
* Serialization and deserialization helpers such as to_dict() and from_dict()

Important rule:

The producer creates the message, but the storage layer assigns its offset.

⸻

TopicStore

Manages topic metadata and topic lifecycle.

Current responsibilities:

* Create topic metadata
* Retrieve topic metadata
* Validate whether a topic exists
* Trigger the initial creation of topic-partition storage directories

Future responsibilities:

* Delete topics
* Replace JSON metadata with KRaft metadata records
* Reconstruct topic metadata from the metadata log

Does not own:

* Message append or fetch
* Segment rolling
* Offset indexes
* Consumer offsets

Topic directory creation is currently performed here as transitional scaffolding. In the target architecture, storage creation should be delegated to LogManager.

⸻

LogManager

Manages all partition logs available on a broker.

Owns:

* Broker log base directory
* Effective segment-size configuration
* Effective index-interval configuration
* In-memory cache of PartitionLog objects
* Mapping from (topic_name, partition_id) to PartitionLog
* Loading and locating partition logs

Example cache:

{
    ("orders", 0): PartitionLog(...),
    ("orders", 1): PartitionLog(...),
}

Delegates:

* Append operations to PartitionLog
* Read operations to PartitionLog
* Segment operations to PartitionLog and Segment

Future responsibilities:

* Load partition logs during broker startup
* Unload logs during topic deletion
* Coordinate retention cleanup
* Manage log directories
* Handle shutdown and recovery

Does not own:

* Individual segment file operations
* Sparse index entry creation
* Producer partition selection

⸻

PartitionLog

Represents the complete ordered log of one topic partition.

Example:

orders-0/
├── 00000000000000000000.log
├── 00000000000000000000.index
├── 00000000000000000100.log
└── 00000000000000000100.index

Owns:

* Topic name
* Partition ID
* Partition directory
* Ordered collection of segments
* Active segment selection
* Segment discovery
* Segment rolling
* Append orchestration
* Cross-segment reads
* Segment selection for a requested offset

Key behaviors:

active_segment()
find_segment_for_offset()
append()
read_from_offset()
roll()

Future responsibilities:

* Retention by size and time
* Log start and end offsets
* Recovery after broker restart
* Segment deletion
* Index rebuilding
* Offset-out-of-range handling

Does not own:

* All partitions on the broker
* Topic metadata
* Producer or consumer coordination

⸻

Segment

Represents one physical log segment and its associated index.

Example:

00000000000000000100.log
00000000000000000100.index

The filename identifies the segment’s base offset.

Owns:

* Base offset
* .log file path
* .index file path
* Sparse-index interval
* Segment file size
* Message append to the segment
* Indexed reads from the segment
* Binary index-entry creation
* Binary search over the offset index
* Next-offset recovery within the segment
* Segment rolling condition

Key behaviors:

append()
read_from_offset()
get_next_offset()
size_in_bytes()
should_roll()
lookup_position()

Index mapping:

relative offset → physical byte position

The index is sparse, so not every message has an index entry. A lookup finds the nearest indexed offset less than or equal to the requested offset, seeks to that byte position, and scans forward.

Future responsibilities:

* Recover index state after restart
* Validate and rebuild corrupt indexes
* Time index support
* Delete segment-related files
* Flush and close file resources
* Checksum and record corruption detection

⸻

Broker

Provides the server-side API boundary for clients.

Owns:

* Topic and partition validation
* Topic creation orchestration
* Produce request handling
* Consume/fetch request handling
* Delegation to TopicStore and LogManager

Produce flow:

Producer
→ Broker
→ LogManager
→ PartitionLog
→ Segment

Fetch flow:

Consumer
→ Broker
→ LogManager
→ PartitionLog
→ Segment and index

Future responsibilities:

* Request protocol handling
* Replica management
* Group coordination
* Transaction coordination
* Authorization
* Controller communication

Does not own:

* Partition-selection policy
* Physical file-format implementation
* Consumer position

⸻

Producer

Represents the client-side producer library.

Owns:

* Message creation
* Topic metadata retrieval
* Partition selection
* Sending records to the broker
* Producer-side validation

Produce flow:

Create Message
→ obtain topic metadata
→ select partition
→ send topic, partition, and message to Broker

Future responsibilities:

* Serialization
* Record batching
* Record accumulator
* Metadata cache
* Retries
* Acknowledgement handling
* Idempotent production
* Compression
* Network communication

Does not own:

* Offset assignment
* Log-file creation
* Segment rolling

⸻

Partitioner

Defines the producer-side contract for selecting a partition.

Owns:

* Partition-selection strategy only

Example contract:

def get_partition(
    self,
    topic: Topic,
    message: Message,
) -> int:
    ...

Implementations may include:

* Hash partitioner
* Round-robin partitioner
* Sticky partitioner
* Custom partitioner

Does not own:

* Message persistence
* Topic creation
* Broker-side validation

⸻

HashPartitioner

Selects a partition using a stable hash of the message key.

Guarantee:

same key → same partition

This preserves ordering for records with the same key, as long as the topic’s partition count remains unchanged.

Does not guarantee:

* Perfectly even distribution for a small number of keys
* Stability after changing partition count

⸻

Consumer

Represents the client-side consumer library.

Current responsibilities:

* Subscribe to a topic
* Discover assigned partitions
* Track a current position per partition
* Poll records through the broker
* Respect max_records
* Combine records read from assigned partitions

Example positions:

{
    0: 42,
    1: 17,
    2: 91,
}

Future responsibilities:

* Consumer-group membership
* Partition assignment
* Offset commit
* Offset reset policy
* Heartbeats
* Rebalancing
* Fetch buffering
* Network communication

Important distinction:

The consumer’s current in-memory position is not the same as a committed consumer-group offset. Committed offsets will eventually be persisted through __consumer_offsets.

⸻

Storage Hierarchy

Broker
└── LogManager
    ├── PartitionLog: orders-0
    │   ├── Segment: base offset 0
    │   ├── Segment: base offset 100
    │   └── Segment: base offset 200
    ├── PartitionLog: orders-1
    └── PartitionLog: payments-0

Responsibility Principle

Each layer manages exactly one scope:

TopicStore
→ topic metadata
LogManager
→ all topic-partition logs on one broker
PartitionLog
→ all segments belonging to one partition
Segment
→ one log file and its indexes
Broker
→ request orchestration
Producer and Consumer
→ client-side behavior

This separation prevents storage details from leaking into broker, producer, or consumer code and provides a scalable foundation for recovery, retention, replication, and KRaft metadata.