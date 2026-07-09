"""
speed_layer/consumer.py
========================
Main consumer loop — trái tim của Speed Layer.

FLOW:
  1. StateManager start() → background thread consume 4 profile topics
  2. Chờ State Table load đủ data (wait_for_initial_load)
  3. Consumer loop chính: đọc cdc.public.trans → enrich → publish

OUTPUT:
  - Enriched records → topic: transactions.events
  - Failed records   → topic: transactions.dlq (via DeadLetterQueue)

METRICS LOG (mỗi 1000 records):
  - Processed / Enriched / DLQ count
  - Enrich success rate
  - State Table size
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from .state_manager import StateManager
from .enrichment import enrich
from .dead_letter import DeadLetterQueue

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRANS       = os.environ.get("KAFKA_TOPIC_TRANS",    "cdc.public.trans")
TOPIC_ENRICHED    = os.environ.get("KAFKA_TOPIC_ENRICHED", "transactions.events")
TOPIC_DLQ         = os.environ.get("KAFKA_TOPIC_DLQ",      "transactions.dlq")
TOPIC_PREFIX      = os.environ.get("KAFKA_TOPIC_PREFIX",   "cdc")

# Số account tối thiểu trong State Table trước khi bắt đầu xử lý trans
MIN_ACCOUNTS_BEFORE_START = int(os.environ.get("MIN_ACCOUNTS_BEFORE_START", "100"))

# Log metrics mỗi N records
LOG_INTERVAL = int(os.environ.get("LOG_INTERVAL", "1000"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    logger.info("=" * 60)
    logger.info("Speed Layer — Stream-Table Join Enrichment")
    logger.info(f"  Kafka: {BOOTSTRAP_SERVERS}")
    logger.info(f"  Input: {TOPIC_TRANS}")
    logger.info(f"  Output: {TOPIC_ENRICHED}")
    logger.info(f"  DLQ: {TOPIC_DLQ}")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 1. Khởi động State Manager (background thread)
    # ------------------------------------------------------------------
    state = StateManager(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic_prefix=TOPIC_PREFIX,
    )
    state.start()

    # Chờ State Table load đủ data từ profile topics trước khi process trans
    logger.info(f"Chờ State Table load ít nhất {MIN_ACCOUNTS_BEFORE_START} accounts...")
    if not state.wait_for_initial_load(min_accounts=MIN_ACCOUNTS_BEFORE_START):
        logger.warning("State Table chưa đủ data nhưng vẫn tiếp tục — DLQ sẽ bắt referential violations")

    # ------------------------------------------------------------------
    # 2. Producer cho enriched output + DLQ
    # ------------------------------------------------------------------
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",            # đảm bảo không mất message
        retries=3,
    )

    dlq = DeadLetterQueue(producer, topic=TOPIC_DLQ)

    # ------------------------------------------------------------------
    # 3. Consumer cho transaction stream
    # ------------------------------------------------------------------
    consumer = KafkaConsumer(
        TOPIC_TRANS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="speed-layer-enrichment-job",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=500,
    )

    # Graceful shutdown khi nhận SIGTERM/SIGINT
    running = True
    def shutdown(sig, frame):
        nonlocal running
        logger.info(f"Nhận signal {sig}, đang shutdown...")
        running = False
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    # ------------------------------------------------------------------
    # 4. Main processing loop
    # ------------------------------------------------------------------
    processed = enriched_count = dlq_count = 0

    logger.info("Bắt đầu consume cdc.public.trans...")

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)

            for topic_partition, messages in records.items():
                for msg in messages:
                    if not running:
                        break

                    transaction = msg.value

                    # Skip tombstone records (delete events với value = null)
                    if transaction is None:
                        continue

                    # Enrich
                    enriched_record, dlq_record = enrich(transaction, state)

                    if enriched_record is not None:
                        # Publish enriched record
                        producer.send(
                            TOPIC_ENRICHED,
                            value=enriched_record,
                            key=enriched_record.get("trans_id"),
                        )
                        enriched_count += 1
                    else:
                        # Publish to DLQ
                        dlq.publish(dlq_record)
                        dlq_count += 1

                    processed += 1

                    # Log metrics định kỳ
                    if processed % LOG_INTERVAL == 0:
                        success_rate = enriched_count / processed * 100
                        stats = state.get_stats()
                        logger.info(
                            f"[{processed:,}] enriched={enriched_count:,} "
                            f"dlq={dlq_count:,} "
                            f"success_rate={success_rate:.1f}% "
                            f"state={stats}"
                        )

    finally:
        logger.info("Flushing producer buffer...")
        producer.flush()
        consumer.close()
        logger.info(
            f"Shutdown complete — processed={processed:,}, "
            f"enriched={enriched_count:,}, dlq={dlq_count:,}"
        )


if __name__ == "__main__":
    run()