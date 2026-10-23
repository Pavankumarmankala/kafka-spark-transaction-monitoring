"""End-to-end run: produce -> consume with a mid-stream crash -> resume
from checkpoint -> verify -> publish Parquet + gold aggregates + metrics.

The crash is deliberate: the first consumer run is killed after writing
batch 20 (offset 19,000) but before committing it. The second run must
resume from the checkpoint and finish with zero transactions lost and
zero duplicates in the output — that verification IS the recovery test,
and its measured result lands in outputs/metrics.json.
"""
import json
import shutil
import time
from pathlib import Path

import pandas as pd

from consumer import SimulatedCrash, StreamingConsumer
from producer import produce
from topic_log import TopicLog

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"; OUT = ROOT / "outputs"
CRASH_AFTER_BATCH = 20


def _fresh_dir(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def run() -> dict:
    t0 = time.time()
    topic = TopicLog(DATA / "topic" / "transactions.jsonl")
    truth = produce(topic, DATA / "topic" / "ground_truth.json")

    batches = DATA / "checkpoints" / "batches"
    checkpoint = DATA / "checkpoints" / "checkpoint.json"
    _fresh_dir(batches); checkpoint.unlink(missing_ok=True)

    # --- run 1: killed mid-stream -------------------------------------
    consumer = StreamingConsumer(topic, batches, checkpoint)
    crashed_at_offset = None
    try:
        consumer.run(crash_after_batch=CRASH_AFTER_BATCH)
    except SimulatedCrash:
        crashed_at_offset = json.loads(checkpoint.read_text())["committed_offset"]

    # --- run 2: restart, recover, resume from checkpoint ---------------
    resumed_from = json.loads(checkpoint.read_text())["committed_offset"]
    consumer2 = StreamingConsumer(topic, batches, checkpoint)
    final = consumer2.run()

    rows = consumer2.committed_output()
    clean = pd.DataFrame(rows)
    clean["event_time"] = pd.to_datetime(clean["event_time"])

    # --- verify: no loss, no duplicates, dedup matches ground truth ----
    out_ids = clean["transaction_id"]
    output_duplicates = int(out_ids.duplicated().sum())
    unique_truth = truth["unique_transactions"]
    lost = int(unique_truth - out_ids.nunique())
    recovery = {
        "crash_after_batch": CRASH_AFTER_BATCH,
        "checkpoint_offset_at_crash": crashed_at_offset,
        "resumed_from_offset": resumed_from,
        "transactions_lost": lost,
        "duplicates_in_output": output_duplicates,
        "recovery_verified": bool(lost == 0 and output_duplicates == 0),
    }

    # --- publish: Parquet sink (Spark job's output format) + gold ------
    (DATA / "output").mkdir(parents=True, exist_ok=True)
    clean.to_parquet(DATA / "output" / "transactions_clean.parquet", index=False)
    gold_dir = DATA / "gold"; gold_dir.mkdir(parents=True, exist_ok=True)
    clean["event_hour"] = clean["event_time"].dt.floor("h")
    clean.groupby("event_hour").agg(
        transactions=("transaction_id", "count"),
        suspicious=("is_suspicious", "sum"),
        revenue=("amount", "sum")).reset_index().to_csv(
        gold_dir / "hourly_summary.csv", index=False)
    alerts = clean[clean.is_suspicious == 1]
    alerts.to_csv(gold_dir / "suspicious_alerts.csv", index=False)
    alerts.assign(rule=alerts.suspicious_reasons.str.split(",")) \
        .explode("rule").groupby("rule").size().rename("alerts") \
        .reset_index().to_csv(gold_dir / "alerts_by_rule.csv", index=False)
    clean.groupby("country").agg(
        transactions=("transaction_id", "count"),
        suspicious=("is_suspicious", "sum")).reset_index().to_csv(
        gold_dir / "risk_by_country.csv", index=False)

    elapsed = round(time.time() - t0, 2)
    metrics = {
        "transactions_produced": truth["messages_produced"],
        "unique_transactions": unique_truth,
        "duplicates_injected": truth["duplicates_injected"],
        "duplicates_removed": final["duplicates_removed"],
        "suspicious_injected": truth["suspicious_injected"],
        "suspicious_flagged": final["suspicious_flagged"],
        "output_transactions": final["output_count"],
        "recovery_test": recovery,
        "pipeline_runtime_seconds": elapsed,
        "throughput_msgs_per_sec": round(truth["messages_produced"] / elapsed, 1),
    }
    OUT.mkdir(exist_ok=True)
    (OUT / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))
    return metrics


if __name__ == "__main__":
    run()
