"""File-backed topic log — the local stand-in for a Kafka topic.

A Kafka topic is an append-only log where every record gets a sequential
offset and consumers track how far they have read. This class implements
exactly that contract on a JSON-lines file:

  - append(record)          -> assigned offset (line number)
  - read_from(offset, n)    -> (records, next_offset)
  - end_offset              -> number of records in the log

The consumer depends only on this contract, so swapping this class for a
real Kafka consumer (see spark/spark_streaming_job.py) changes the
transport, not the processing logic.
"""
import json
from pathlib import Path


class TopicLog:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: dict) -> int:
        offset = self.end_offset
        with self.path.open("a") as f:
            f.write(json.dumps(record) + "\n")
        return offset

    def append_many(self, records) -> int:
        start = self.end_offset
        with self.path.open("a") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return start

    @property
    def end_offset(self) -> int:
        if not self.path.exists():
            return 0
        with self.path.open() as f:
            return sum(1 for _ in f)

    def read_from(self, offset: int, max_records: int):
        records = []
        if not self.path.exists():
            return records, offset
        with self.path.open() as f:
            for i, line in enumerate(f):
                if i < offset:
                    continue
                if len(records) >= max_records:
                    break
                records.append(json.loads(line))
        return records, offset + len(records)

    def clear(self):
        self.path.unlink(missing_ok=True)
