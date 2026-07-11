# ADR 002 — Berka dataset thay vì IBM AML hoặc PaySim

**Status:** Accepted  
**Date:** 2026-07-01  
**Deciders:** Nhựt (sole engineer)

---

## Context

Pipeline cần dataset banking để:
1. Simulate core banking system (Postgres)
2. Generate synthetic data theo phân phối thống kê thật
3. Demonstrate Stream-Table Join: transaction stream enrich với customer profile

Các dataset public phổ biến cho fraud detection:
- **Berka** (PKDD'99 Discovery Challenge): 8 bảng có quan hệ FK tự nhiên
- **IBM AML HI-Small**: transaction dataset cho Anti-Money Laundering
- **PaySim**: mobile money transaction simulation

## Decision

Chọn **Berka**, loại IBM AML và PaySim.

## Rationale

**Yêu cầu kỹ thuật cốt lõi của Stream-Table Join:**

Pattern này yêu cầu **shared foreign key tự nhiên** giữa transaction stream
và reference table: `transaction.account_id` → `account` → `disposition`
→ `client` → `district`.

**Tại sao loại IBM AML:**

IBM AML HI-Small là dataset giao dịch độc lập — không có bảng customer
profile đi kèm. Để JOIN với customer data, phải tạo "synthetic entity-linking":
gán account_id giả vào transaction rồi tạo profile giả tương ứng.

Đây là một lớp giả định không cần thiết làm yếu nền móng của demo. Khi
interviewer hỏi "tại sao account A có profile này?", câu trả lời là "vì tôi
tự gán ngẫu nhiên" — không thuyết phục.

**Tại sao loại PaySim:**

PaySim là simulation data cho mobile money (M-Pesa), không phải traditional
banking. Không có customer demographic table. Cùng vấn đề với IBM AML:
thiếu shared FK tự nhiên để demonstrate Stream-Table Join.

**Tại sao chọn Berka:**

1. **8 bảng có FK tự nhiên**: `trans` → `account` → `disposition` → `client`
   → `district`. Đây là schema đủ phức tạp để demonstrate multi-hop join
   trong Stream-Table Join, nhưng không quá phức tạp để maintain.

2. **Tách biệt transaction và profile rõ ràng**:
   - `trans`: event stream (1M rows, high volume)
   - `client`, `account`, `district`: reference data (5K rows, low volume, slow-changing)
   Đây là đúng điều kiện để áp dụng Stream-Table Join pattern.

3. **Data quality phong phú**: Berka có đủ loại: categorical, numeric, date,
   nullable fields — đủ để demonstrate 3 loại data corruption có ý nghĩa.

## Consequences

**Positive:**
- FK tự nhiên → không cần justify synthetic entity-linking
- Schema đủ phức tạp để demo multi-table enrichment
- Dataset nhỏ (~50MB) → dễ load, dễ reset khi dev

**Negative:**
- Volume nhỏ (~1M rows) → cần synthetic data generation để test throughput
- Data từ 1993-1998 → timeline cũ, không phải vấn đề kỹ thuật nhưng
  cần giải thích khi demo

**Mitigation:**
- Synthetic data generation (ADR 003) giải quyết vấn đề volume
- Timeline cũ không ảnh hưởng CDC logic — Debezium track WAL timestamp
  (khi INSERT xảy ra), không phải business date trong data