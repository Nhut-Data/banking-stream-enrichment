# ADR 003 — Synthetic Data Generation để test hạ tầng

**Status:** Accepted  
**Date:** 2026-07-01  
**Deciders:** Nhựt (sole engineer)

---

## Context

Berka gốc có ~1 triệu dòng `trans`. Để test hạ tầng streaming thật
(Kafka throughput, Debezium CDC latency, Speed Layer enrichment rate),
cần đủ volume để:
1. Trigger meaningful load test (throughput, latency under load)
2. Verify deduplication logic hoạt động đúng ở scale
3. Demo pipeline xử lý continuous stream, không phải batch nhỏ

Có 2 hướng tiếp cận:
- **Dùng thêm dataset khác** (IBM AML, PaySim) làm data bổ sung
- **Sinh synthetic data** từ phân phối thống kê học được từ Berka gốc

## Decision

Chọn **Synthetic Data Generation** từ phân phối Berka gốc.

## Rationale

**Tại sao không dùng thêm dataset khác:**

Như đã phân tích trong ADR 002, IBM AML và PaySim không có shared FK
tự nhiên với Berka. Nếu dùng thêm, phải tạo synthetic entity-linking
giữa 2 dataset độc lập — đây là complexity không cần thiết và làm yếu
tính nhất quán của demo.

**Lý do nghiệp vụ cho synthetic generation (câu trả lời chuẩn khi được hỏi):**

Trong banking thực tế, GDPR và PCI-DSS cấm dùng dữ liệu giao dịch khách
hàng thật trong môi trường Dev/Staging. Quy trình chuẩn là:
1. Học phân phối thống kê từ production data (không copy data thật)
2. Sinh synthetic data "giống thật" theo phân phối đó
3. Dùng synthetic data trong Dev/Staging

Project này mô phỏng đúng quy trình này với Berka làm "production data".

**Thiết kế kỹ thuật:**

Không sinh random data — học phân phối thật:
- `operation`: categorical frequency distribution
- `amount | operation`: log-normal distribution (conditional per operation)
- `account_id`: weighted sampling theo tần suất giao dịch gốc
- `type`: deterministic mapping từ `operation` (không sample độc lập)
- `k_symbol | operation`: conditional categorical distribution

Joint/conditional sampling thay vì marginal sampling độc lập từng cột,
vì các cột này không độc lập với nhau trong data thật.

**Data corruption có chủ đích (3 loại, tỷ lệ 2-5%):**

1. **Duplicate records**: mô phỏng Kafka producer retry
2. **Referential integrity violation**: account_id không tồn tại
3. **Missing values**: null ở field không bắt buộc (k_symbol)

Corruption được log vào `docs/corruption_manifest.json` để đối chiếu
với kết quả pipeline (dbt test, Speed Layer DLQ).

## Consequences

**Positive:**
- Data "giống thật" về mặt thống kê — phân phối amount, frequency, account
  activity pattern giống Berka gốc
- Reproducible: cùng `RANDOM_SEED=42` → cùng output
- Corruption có kiểm soát và traceable qua manifest

**Negative:**
- `balance` không được sinh ra (cần stateful calculation per account)
  → set NULL, accepted limitation được document rõ
- Timeline synthetic bắt đầu từ 1999 (tiếp nối sau 1998 của Berka gốc)
  → có thể trông lạ khi demo nhưng không ảnh hưởng logic kỹ thuật

**2 tầng volume:**
- Dev: 100K dòng (dùng hằng ngày khi code/debug)
- Load test: 1M dòng (chạy riêng để prove throughput)