"""Streaming consumer — local implementation of the Spark job's logic.

Mirrors spark/spark_streaming_job.py step for step, on the file-backed
topic (see topic_log.py):

  1. Read a micro-batch of records from the last committed offset.
  2. Deduplicate by transaction_id within a watermark window
     (Spark: withWatermark + dropDuplicates). A re-delivery whose
     event_time is within `watermark_minutes` of the first delivery is
     dropped; state for ids older than the watermark is evicted, so a
     later re-delivery would pass through — the same trade-off Spark
     makes to bound state.
  3. Flag suspicious payments with per-customer rolling state:
     velocity (this is at least the 5th transaction in 10 minutes),
     amount anomaly (>4x rolling average, needs >=3 prior),
     foreign country (country != customer's home country).
  4. Write the batch output file, then — and only then — commit the
     new offset to the checkpoint file (atomic rename).

Failure recovery: if the process dies after writing a batch but before
committing it, that batch file is uncommitted. On startup, recover()
deletes every batch file past the checkpoint, so the batch is simply
reprocessed. Nothing committed is ever lost, and nothing uncommitted
is ever double-counted: exactly the guarantee Spark's checkpointLocation
+ idempotent sink provides.
"""
import json
import os
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path


class SimulatedCrash(Exception):
    """Raised to simulate the consumer process being killed mid-stream."""


def _ts(record):
    return datetime.fromisoformat(record["event_time"])


class StreamingConsumer:
    def __init__(self, topic, output_dir, checkpoint_path,
                 batch_size: int = 1_000, watermark_minutes: int = 60):
        self.topic = topic
        self.output_dir = Path(output_dir)
        self.checkpoint_path = Path(checkpoint_path)
        self.batch_size = batch_size
        self.watermark_seconds = watermark_minutes * 60
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        # streaming state (rebuilt from committed output on restart)
        self.committed_offset = 0
        self.processed = 0
        self.duplicates_removed = 0
        self.suspicious_flagged = 0
        self.output_count = 0
        self.seen = {}                       # txn_id -> event_time (dedup state)
        self.max_event_time = None
        self._velocity = defaultdict(deque)  # customer -> deque[event_time]
        self._amounts = defaultdict(list)    # customer -> recent amounts
        self._restore()

    # ---------------------------------------------------------- state --
    def _checkpoint_data(self):
        return {
            "committed_offset": self.committed_offset,
            "processed": self.processed,
            "duplicates_removed": self.duplicates_removed,
            "suspicious_flagged": self.suspicious_flagged,
            "output_count": self.output_count,
        }

    def _commit(self):
        tmp = self.checkpoint_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._checkpoint_data(), indent=2))
        os.replace(tmp, self.checkpoint_path)  # atomic

    def _batch_file(self, start_offset):
        return self.output_dir / f"batch_{start_offset:08d}.jsonl"

    def recover(self):
        """Drop uncommitted batch files (written but never checkpointed)."""
        if not self.checkpoint_path.exists():
            for f in self.output_dir.glob("batch_*.jsonl"):
                f.unlink()
            return
        for f in self.output_dir.glob("batch_*.jsonl"):
            start = int(f.stem.split("_")[1])
            if start >= self.committed_offset:
                f.unlink()

    def _restore(self):
        """On (re)start: load checkpoint, drop uncommitted batches, and
        rebuild dedup/rule state by replaying committed output only."""
        if self.checkpoint_path.exists():
            data = json.loads(self.checkpoint_path.read_text())
            self.__dict__.update({k: data[k] for k in
                                  ("committed_offset", "processed",
                                   "duplicates_removed", "suspicious_flagged",
                                   "output_count")})
        self.recover()
        if self.committed_offset:
            for f in sorted(self.output_dir.glob("batch_*.jsonl")):
                with f.open() as fh:
                    for line in fh:
                        self._update_state(json.loads(line), count=False)

    # ------------------------------------------------------- scoring --
    def _update_state(self, record, count=True):
        """Feed one ACCEPTED record into dedup + rule state.
        Returns (is_duplicate, reasons)."""
        ts = _ts(record)
        if self.max_event_time is None or ts > self.max_event_time:
            self.max_event_time = ts
        # evict dedup state older than the watermark (bounded state)
        if self.max_event_time:
            cutoff = self.max_event_time.timestamp() - self.watermark_seconds
            self.seen = {k: v for k, v in self.seen.items()
                         if v.timestamp() >= cutoff}

        first_seen = self.seen.get(record["transaction_id"])
        if first_seen is not None and \
                abs((ts - first_seen).total_seconds()) <= self.watermark_seconds:
            return True, []
        self.seen[record["transaction_id"]] = ts

        reasons = []
        dq = self._velocity[record["customer_id"]]
        while dq and (ts - dq[0]).total_seconds() > 600:
            dq.popleft()
        if len(dq) >= 4:
            reasons.append("velocity")
        dq.append(ts)

        amts = self._amounts[record["customer_id"]]
        if len(amts) >= 3 and record["amount"] > 4 * (sum(amts) / len(amts)):
            reasons.append("amount_anomaly")
        amts.append(record["amount"])
        if len(amts) > 50:
            amts.pop(0)

        if record["country"] != record["home_country"]:
            reasons.append("foreign_country")
        return False, reasons

    # ------------------------------------------------------------ run --
    def run(self, crash_after_batch: int = None):
        """Process micro-batches until the topic is exhausted.
        crash_after_batch=N simulates a kill: batch N's output is written
        but its checkpoint is never committed, then SimulatedCrash."""
        batch_no = 0
        while True:
            records, _ = self.topic.read_from(self.committed_offset, self.batch_size)
            if not records:
                return self._checkpoint_data()
            start = self.committed_offset
            out_lines = []
            batch_dups = batch_susp = 0
            for record in records:
                is_dup, reasons = self._update_state(record)
                if is_dup:
                    batch_dups += 1
                    continue
                record = dict(record)
                record["is_suspicious"] = int(bool(reasons))
                record["suspicious_reasons"] = ",".join(reasons)
                batch_susp += record["is_suspicious"]
                out_lines.append(json.dumps(record))
            with self._batch_file(start).open("w") as f:
                f.write("\n".join(out_lines) + ("\n" if out_lines else ""))
            batch_no += 1
            if crash_after_batch is not None and batch_no == crash_after_batch:
                raise SimulatedCrash(
                    f"killed after writing batch {batch_no} "
                    f"(offset {start}) before checkpoint commit")
            self.committed_offset = start + len(records)
            self.processed += len(records)
            self.duplicates_removed += batch_dups
            self.suspicious_flagged += batch_susp
            self.output_count += len(out_lines)
            self._commit()

    def committed_output(self):
        """All committed output records, in offset order."""
        rows = []
        for f in sorted(self.output_dir.glob("batch_*.jsonl")):
            start = int(f.stem.split("_")[1])
            if start < self.committed_offset:
                with f.open() as fh:
                    rows.extend(json.loads(line) for line in fh)
        return rows
