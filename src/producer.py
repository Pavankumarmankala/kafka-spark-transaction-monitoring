"""Transaction producer.

Generates a deterministic (seeded) stream of e-commerce transactions and
appends it to the topic log — the local equivalent of a Kafka producer.

Stream composition (60,000 messages total):
  - 58,800 unique transactions (58,350 legitimate + 450 injected suspicious)
  - 1,200 duplicate messages (2.0%): exact re-deliveries of an earlier
    transaction_id, inserted 50-500 offsets later with the original
    event_time — the classic at-least-once retry duplicate a streaming
    job must deduplicate.

Injected suspicious payments (labelled in ground_truth.json, NOT in the
topic records — production data carries no labels):
  - velocity bursts: one customer, 7 transactions in ~8 minutes
  - amount anomalies: 6-10x the customer's normal spend
  - foreign-country payments: country != customer's home country
"""
import json
from pathlib import Path
import numpy as np

COUNTRIES = ["US", "CA", "GB", "DE", "IN", "AU"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay"]


def produce(topic, truth_path, n_unique: int = 58_350, n_duplicates: int = 1_200,
            n_customers: int = 1_500, days: int = 3, seed: int = 42) -> dict:
    rng = np.random.default_rng(seed)
    start = np.datetime64("2026-10-01T00:00:00")

    home_country = rng.choice(COUNTRIES, n_customers, p=[.62, .08, .08, .06, .10, .06])
    avg_amount = np.round(rng.lognormal(mean=3.6, sigma=0.6, size=n_customers), 2)

    records, suspicious_ids = [], []
    offsets_sec = np.sort(rng.integers(0, days * 86400, n_unique))
    cust_idx = rng.integers(0, n_customers, n_unique)
    for i in range(n_unique):
        c = int(cust_idx[i])
        records.append({
            "transaction_id": f"TXN-{i:07d}",
            "customer_id": f"CUST-{c:05d}",
            "event_time": str(start + offsets_sec[i].astype("timedelta64[s]")),
            "amount": round(float(max(5, rng.normal(avg_amount[c], avg_amount[c] * 0.35))), 2),
            "country": str(home_country[c]),
            "home_country": str(home_country[c]),
            "payment_method": str(rng.choice(PAYMENT_METHODS, p=[.45, .30, .15, .10])),
            "status": "approved" if rng.random() > 0.03 else "failed",
        })

    # ---- injected suspicious payments (appended in time order below) --
    extra = []
    suspects = rng.choice(n_customers, 150, replace=False)
    base_ts = start + np.timedelta64(days * 86400 - 3600, "s")
    for j, c in enumerate(suspects):
        t0 = base_ts - np.timedelta64(int(rng.integers(0, 3600)), "s")
        pattern = ["velocity", "amount_anomaly", "foreign_country"][j % 3]
        if pattern == "velocity":
            for k in range(7):
                extra.append(_suspicious(rng, len(records) + len(extra), c, home_country, avg_amount,
                                         t0 + np.timedelta64(int(k * 70), "s"),
                                         avg_amount[c], home_country[c]))
                suspicious_ids.append(extra[-1]["transaction_id"])
        elif pattern == "amount_anomaly":
            extra.append(_suspicious(rng, len(records) + len(extra), c, home_country, avg_amount,
                                     t0, avg_amount[c] * rng.uniform(6, 10), home_country[c]))
            suspicious_ids.append(extra[-1]["transaction_id"])
        else:
            foreign = str(rng.choice([x for x in COUNTRIES if x != home_country[c]]))
            extra.append(_suspicious(rng, len(records) + len(extra), c, home_country, avg_amount,
                                     t0, avg_amount[c] * rng.uniform(1.5, 3), foreign))
            suspicious_ids.append(extra[-1]["transaction_id"])
    records.extend(extra)
    records.sort(key=lambda r: r["event_time"])
    # re-number ids after sorting so ids stay unique and stable
    id_map = {}
    for new_i, r in enumerate(records):
        id_map[r["transaction_id"]] = f"TXN-{new_i:07d}"
        r["transaction_id"] = f"TXN-{new_i:07d}"
    suspicious_ids = [id_map[s] for s in suspicious_ids]

    # ---- injected duplicates: exact copies, 50-500 offsets later -------
    dup_sources = rng.choice(len(records), n_duplicates, replace=False)
    insertions = {}
    for src in dup_sources:
        pos = min(len(records) - 1, int(src) + int(rng.integers(50, 500)))
        insertions.setdefault(pos, []).append(dict(records[int(src)]))
    stream = []
    duplicate_ids = []
    for i, rec in enumerate(records):
        stream.append(rec)
        for dup in insertions.get(i, []):
            stream.append(dup)
            duplicate_ids.append(dup["transaction_id"])
    total = len(stream)

    topic.clear()
    topic.append_many(stream)
    truth = {
        "messages_produced": total,
        "unique_transactions": len(records),
        "duplicates_injected": len(duplicate_ids),
        "duplicate_transaction_ids": sorted(set(duplicate_ids)),
        "suspicious_injected": len(suspicious_ids),
        "suspicious_transaction_ids": suspicious_ids,
    }
    Path(truth_path).parent.mkdir(parents=True, exist_ok=True)
    Path(truth_path).write_text(json.dumps(truth, indent=2))
    return truth


def _suspicious(rng, i, c, home_country, avg_amount, ts, amount, country):
    return {
        "transaction_id": f"TXN-{i:07d}",
        "customer_id": f"CUST-{c:05d}",
        "event_time": str(ts),
        "amount": round(float(amount), 2),
        "country": str(country),
        "home_country": str(home_country[c]),
        "payment_method": "credit_card",
        "status": "approved",
    }
