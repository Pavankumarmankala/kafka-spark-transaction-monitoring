import json

import pytest

from consumer import SimulatedCrash, StreamingConsumer
from topic_log import TopicLog


def _rec(i, customer="CUST-00001", ts="2026-10-01T10:00:00", amount=50.0,
         country="US", home="US"):
    return {"transaction_id": f"TXN-{i:07d}", "customer_id": customer,
            "event_time": ts, "amount": amount, "country": country,
            "home_country": home, "payment_method": "credit_card",
            "status": "approved"}


def _setup(tmp_path, records, batch_size=10):
    topic = TopicLog(tmp_path / "topic.jsonl")
    topic.append_many(records)
    return topic, StreamingConsumer(topic, tmp_path / "batches",
                                    tmp_path / "checkpoint.json",
                                    batch_size=batch_size)


def test_duplicate_within_watermark_is_removed(tmp_path):
    recs = [_rec(1), _rec(2), _rec(1)]  # TXN-0000001 delivered twice
    _, consumer = _setup(tmp_path, recs)
    result = consumer.run()
    assert result["duplicates_removed"] == 1
    assert result["output_count"] == 2
    ids = [r["transaction_id"] for r in consumer.committed_output()]
    assert len(ids) == len(set(ids)) == 2


def test_duplicate_beyond_watermark_passes_through(tmp_path):
    # Same id re-delivered 2 hours later: outside the 60-minute watermark
    # the state has been evicted, so it is treated as a new transaction —
    # the documented Spark dropDuplicates trade-off.
    recs = [_rec(1, ts="2026-10-01T08:00:00"),
            _rec(2, ts="2026-10-01T09:30:00"),
            _rec(1, ts="2026-10-01T10:00:00")]
    _, consumer = _setup(tmp_path, recs)
    result = consumer.run()
    assert result["duplicates_removed"] == 0
    assert result["output_count"] == 3


def test_suspicious_rules_flag_foreign_and_amount(tmp_path):
    recs = [_rec(1), _rec(2), _rec(3),
            _rec(4, amount=900.0),                 # >4x rolling average
            _rec(5, country="DE")]                 # foreign country
    _, consumer = _setup(tmp_path, recs)
    result = consumer.run()
    assert result["suspicious_flagged"] == 2


def test_checkpoint_resume_after_crash_loses_nothing(tmp_path):
    recs = [_rec(i, ts=f"2026-10-01T10:{i:02d}:00") for i in range(50)]
    recs.append(_rec(7, ts="2026-10-01T10:07:30"))  # one duplicate
    topic, consumer = _setup(tmp_path, recs, batch_size=10)
    with pytest.raises(SimulatedCrash):
        consumer.run(crash_after_batch=3)  # killed mid-stream
    committed = json.loads((tmp_path / "checkpoint.json").read_text())
    assert committed["committed_offset"] == 20  # batches 1-2 only

    restarted = StreamingConsumer(topic, tmp_path / "batches",
                                  tmp_path / "checkpoint.json", batch_size=10)
    result = restarted.run()
    out = restarted.committed_output()
    ids = [r["transaction_id"] for r in out]
    assert result["processed"] == 51
    assert result["duplicates_removed"] == 1
    assert len(ids) == len(set(ids)) == 50  # zero lost, zero duplicated


def test_uncommitted_batch_is_reprocessed_not_double_counted(tmp_path):
    recs = [_rec(i, ts=f"2026-10-01T11:{i:02d}:00") for i in range(30)]
    topic, consumer = _setup(tmp_path, recs, batch_size=10)
    with pytest.raises(SimulatedCrash):
        consumer.run(crash_after_batch=2)  # batch 2 written, not committed
    restarted = StreamingConsumer(topic, tmp_path / "batches",
                                  tmp_path / "checkpoint.json", batch_size=10)
    result = restarted.run()
    assert result["output_count"] == 30
    assert len(restarted.committed_output()) == 30
