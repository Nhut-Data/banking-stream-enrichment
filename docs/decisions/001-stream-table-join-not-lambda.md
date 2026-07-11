# ADR 001 — Stream-Table Join thay vì Lambda Architecture

**Status:** Accepted  
**Date:** 2026-07-01  
**Deciders:** Nhựt (sole engineer)

---

## Context

Bài toán cần giải quyết: enrich transaction stream volume cao với customer profile
volume thấp trong real-time để phục vụ fraud detection downstream.

Có 2 pattern kiến trúc phổ biến cho loại bài toán này:

**Lambda Architecture** (Nathan Marz, 2011): xử lý cùng 1 data qua 2 path song song:
- Speed layer: xử lý real-time, kết quả gần đúng (approximate)
- Batch layer: re-process toàn bộ, kết quả chính xác (accurate)
- Serving layer: merge kết quả từ 2 layer trên

**Stream-Table Join / Streaming Enrichment**: 1 luồng sự kiện volume cao
(transaction) được enrich bằng 1 bảng tham chiếu volume thấp, ít thay đổi
(customer profile) được giữ trong memory (State Table).

## Decision

Chọn **Stream-Table Join**, không phải Lambda Architecture.

## Rationale

**Lambda Architecture không phù hợp vì:**

1. Lambda yêu cầu cùng 1 business logic được implement 2 lần (speed + batch),
   dẫn đến "dual maintenance problem" — bug fix phải apply ở 2 chỗ. Với team
   1 người, chi phí này không có lý do để chấp nhận.

2. Lambda giả định nguồn data duy nhất đi qua 2 path. Project này có 2 nguồn
   data khác nhau về bản chất: transaction (event stream) và customer profile
   (reference data). Đây là bài toán enrichment, không phải re-aggregation.

3. Lambda được thiết kế cho bài toán "tính aggregate từ stream" (vd: đếm số
   click theo giờ). Fraud detection cần enrichment (thêm context vào event),
   không cần re-aggregation.

**Stream-Table Join phù hợp vì:**

1. Đây là pattern chuẩn trong fraud detection thực tế: transaction stream
   (volume cao, latency thấp) + customer profile (volume thấp, ít thay đổi)
   → enriched event cho model scoring.

2. Customer profile thay đổi rất chậm (địa chỉ, demographic). Giữ trong
   memory và update qua CDC là đủ — không cần full re-process như Lambda.

3. Batch layer vẫn tồn tại trong thiết kế này, nhưng với mục đích khác:
   re-process để đảm bảo correctness (xử lý late-arriving data, deduplicate),
   không phải để compensate cho speed layer.

## Consequences

**Positive:**
- Single implementation của enrichment logic (speed layer Python code)
- Batch layer (dbt) tập trung vào data quality, không phải duplicate logic
- Kiến trúc phản ánh đúng bản chất bài toán fraud detection

**Negative:**
- State Table trong memory không persist qua restart — cần warm-up time
  khi restart service (consume lại toàn bộ profile topics từ đầu)
- Eventual consistency: profile update trong State Table có độ trễ nhỏ
  so với thay đổi thực tế trong database

**Mitigations:**
- Warm-up time được handle bởi `wait_for_initial_load()` trong StateManager
- Độ trễ profile update chấp nhận được vì customer profile thay đổi rất chậm
  (days/weeks, không phải seconds)