"""
speed_layer/state_manager.py
=============================
Consume 4 profile topics từ Kafka và giữ State Table trong memory.

State Table là dict Python đơn giản — không dùng RocksDB hay external store
vì scope hiện tại: single-node, single-process, không cần persistence.

4 dicts:
  accounts     {account_id: {district_id, frequency, date}}
  clients      {client_id: {birth_number, district_id}}
  dispositions {account_id: client_id}  ← chỉ lấy OWNER, bỏ DISPONENT
  districts    {district_id: {name, region, avg_salary, ...}}

UPDATE/DELETE handling:
  - __op = 'c' (create) hoặc 'u' (update) → upsert vào dict
  - __op = 'd' (delete) → xóa khỏi dict
  - __deleted = 'true' → xóa (Debezium rewrite mode)
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Optional

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class StateManager:
    """
    Thread-safe State Table cho 4 profile tables.
    Chạy consumer loop trong background thread riêng,
    main thread gọi lookup() để enrich transaction.
    """

    def __init__(self, bootstrap_servers: str, topic_prefix: str = "cdc"):
        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix = topic_prefix

        # State tables
        self._accounts: dict = {}
        self._clients: dict = {}
        self._dispositions: dict = {}  # account_id -> client_id (OWNER only)
        self._districts: dict = {}

        # Thread safety — reader nhiều, writer ít → dùng RLock đơn giản
        self._lock = threading.RLock()

        # Metrics
        self._stats = {
            "accounts": 0,
            "clients": 0,
            "dispositions": 0,
            "districts": 0,
            "updates": 0,
        }

    # ------------------------------------------------------------------
    # Consumer loop — chạy trong background thread
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Khởi động background thread consume 4 profile topics."""
        topics = [
            f"{self.topic_prefix}.public.account",
            f"{self.topic_prefix}.public.client",
            f"{self.topic_prefix}.public.disposition",
            f"{self.topic_prefix}.public.district",
        ]

        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",      # đọc từ đầu để load toàn bộ state
            group_id="speed-layer-state-manager",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=-1,            # loop vô hạn
        )

        logger.info(f"StateManager bắt đầu consume {len(topics)} profile topics...")

        thread = threading.Thread(
            target=self._consume_loop,
            args=(consumer,),
            daemon=True,                       # tự tắt khi main thread kết thúc
            name="state-manager-consumer",
        )
        thread.start()
        logger.info("StateManager background thread started")

    def _consume_loop(self, consumer: KafkaConsumer) -> None:
        for message in consumer:
            try:
                self._process_message(message.topic, message.value)
            except Exception as e:
                logger.error(f"Lỗi xử lý message từ {message.topic}: {e}")

    def _process_message(self, topic: str, payload: dict) -> None:
        """Route message đến đúng handler theo topic."""
        # Debezium rewrite mode: deleted records có __deleted = 'true'
        is_deleted = payload.get("__deleted") == "true"
        table = topic.split(".")[-1]  # cdc.public.account → account

        with self._lock:
            if table == "account":
                self._handle_account(payload, is_deleted)
            elif table == "client":
                self._handle_client(payload, is_deleted)
            elif table == "disposition":
                self._handle_disposition(payload, is_deleted)
            elif table == "district":
                self._handle_district(payload, is_deleted)
            self._stats["updates"] += 1

    def _handle_account(self, p: dict, deleted: bool) -> None:
        aid = p["account_id"]
        if deleted:
            self._accounts.pop(aid, None)
        else:
            self._accounts[aid] = {
                "account_id":  aid,
                "district_id": p.get("district_id"),
                "frequency":   p.get("frequency"),
                "date":        p.get("date"),
            }
            self._stats["accounts"] = len(self._accounts)

    def _handle_client(self, p: dict, deleted: bool) -> None:
        cid = p["client_id"]
        if deleted:
            self._clients.pop(cid, None)
        else:
            self._clients[cid] = {
                "client_id":    cid,
                "birth_number": p.get("birth_number"),
                "district_id":  p.get("district_id"),
            }
            self._stats["clients"] = len(self._clients)

    def _handle_disposition(self, p: dict, deleted: bool) -> None:
        aid = p["account_id"]
        # Chỉ track OWNER — đây là người chủ tài khoản chính
        # DISPONENT chỉ có quyền rút tiền, không đại diện cho profile account
        if p.get("type") == "OWNER":
            if deleted:
                self._dispositions.pop(aid, None)
            else:
                self._dispositions[aid] = p["client_id"]
                self._stats["dispositions"] = len(self._dispositions)

    def _handle_district(self, p: dict, deleted: bool) -> None:
        did = p["district_id"]
        if deleted:
            self._districts.pop(did, None)
        else:
            self._districts[did] = {
                "district_id":   did,
                "name":          p.get("name"),
                "region":        p.get("region"),
                "avg_salary":    p.get("avg_salary"),
                "num_inhabitants": p.get("num_inhabitants"),
            }
            self._stats["districts"] = len(self._districts)

    # ------------------------------------------------------------------
    # Lookup API — gọi từ enrichment.py
    # ------------------------------------------------------------------

    def lookup(self, account_id: int) -> Optional[dict]:
        """
        Lookup đầy đủ profile cho 1 account_id.

        Returns:
            dict với đầy đủ thông tin profile, hoặc None nếu không tìm thấy
            (referential violation → sẽ được đẩy vào DLQ)
        """
        with self._lock:
            account = self._accounts.get(account_id)
            if account is None:
                return None

            client_id = self._dispositions.get(account_id)
            if client_id is None:
                return None

            client = self._clients.get(client_id)
            if client is None:
                return None

            # Dùng district của client (nơi khách hàng sinh sống)
            # Fallback sang district của account nếu client district không có
            district_id = client.get("district_id") or account.get("district_id")
            district = self._districts.get(district_id, {})

            return {
                "account_id":      account_id,
                "client_id":       client_id,
                "birth_number":    client.get("birth_number"),
                "client_district_id": client.get("district_id"),
                "account_district_id": account.get("district_id"),
                "account_frequency": account.get("frequency"),
                "district_name":   district.get("name"),
                "district_region": district.get("region"),
                "avg_salary":      district.get("avg_salary"),
            }

    def get_stats(self) -> dict:
        with self._lock:
            return dict(self._stats)

    def wait_for_initial_load(self, min_accounts: int = 100, timeout: int = 120) -> bool:
        """
        Block cho đến khi State Table có đủ data từ snapshot.
        Dùng khi khởi động lần đầu để đảm bảo profile đã loaded
        trước khi bắt đầu xử lý transaction stream.
        """
        import time
        start = time.time()
        while time.time() - start < timeout:
            with self._lock:
                if len(self._accounts) >= min_accounts:
                    logger.info(
                        f"State Table ready: {len(self._accounts)} accounts, "
                        f"{len(self._clients)} clients, "
                        f"{len(self._dispositions)} dispositions, "
                        f"{len(self._districts)} districts"
                    )
                    return True
            time.sleep(2)

        logger.warning(f"Timeout sau {timeout}s — State Table chỉ có {len(self._accounts)} accounts")
        return False