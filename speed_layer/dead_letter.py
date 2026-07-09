"""
speed_layer/dead_letter.py
===========================
Publish enrichment failures vào Dead-Letter Queue topic.

DLQ topic: transactions.dlq
Format: JSON với original transaction + lý do fail + timestamp

Tách riêng thành module để:
  1. consumer.py không bị phình to
  2. Dễ test DLQ logic độc lập
  3. Sau này có thể thêm retry logic hoặc alert mà không sửa consumer
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    def __init__(self, producer: KafkaProducer, topic: str = "transactions.dlq"):
        self.producer = producer
        self.topic = topic
        self._count = 0

    def publish(self, dlq_record: dict) -> None:
        """
        Publish 1 failed record vào DLQ topic.
        Non-blocking: dùng async send với callback để không block main consumer loop.
        """
        payload = {
            **dlq_record,
            "dlq_published_at": datetime.now(timezone.utc).isoformat(),
        }

        self.producer.send(
            self.topic,
            value=json.dumps(payload, default=str).encode("utf-8"),
            key=str(dlq_record.get("trans_id", "unknown")).encode("utf-8"),
        )
        self._count += 1

        if self._count % 100 == 0:
            logger.warning(f"DLQ: {self._count} records tổng cộng")
        else:
            logger.debug(
                f"DLQ: trans_id={dlq_record.get('trans_id')}, "
                f"reason={dlq_record.get('dlq_reason')}, "
                f"account_id={dlq_record.get('account_id')}"
            )

    @property
    def count(self) -> int:
        return self._count