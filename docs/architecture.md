# Architecture

```
Producers (checkout / payments)
        |  JSON events, at-least-once delivery
        v
Kafka topic "transactions" (3 partitions, keyed by customer_id)
        |
        v
Spark Structured Streaming (spark/spark_streaming_job.py)
  - parse JSON, cast event_time
  - stream-static join: customer profile (avg spend)
  - withWatermark(event_time, 60 minutes) + dropDuplicates(transaction_id)
  - rules: amount anomaly, foreign country  (+ windowed velocity query)
  - checkpointLocation on durable storage  <-- failure recovery
        |
        v
Parquet sink (partitioned by event_date)  ->  S3
        |                                        |
        v                                        v
Redshift (COPY load, star schema)        Athena queries (sql/)
        |
        v
Monitoring: hourly volume / suspicious rate, alerts by rule,
revenue at risk (gold aggregates in data/gold/)
```

## Failure recovery, step by step
1. Spark writes a micro-batch to the Parquet sink.
2. Spark records the batch's offsets in the checkpoint.
3. If the job dies between 1 and 2, the batch is uncommitted: on
   restart Spark replays exactly that batch from the checkpointed
   offset — committed batches are never re-read, uncommitted work is
   never half-counted.
4. The local simulator (src/consumer.py) implements the same protocol
   with batch files + an atomically-renamed checkpoint file, and
   src/pipeline.py kills the consumer mid-stream on every run to
   prove it: the recovery result is in outputs/metrics.json.
