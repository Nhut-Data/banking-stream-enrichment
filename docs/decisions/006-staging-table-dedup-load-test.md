# ADR 006 — Staging Table + ON CONFLICT cho xử lý duplicate trong load test
**Status:** Accepted
**Date:** 2026-07-20
**Deciders:** Nhựt (sole engineer)
---
## Context
Khi load `trans_loadtest.csv` (~1 triệu dòng synthetic, có tiêm
`CORRUPTION_RATE_DUPLICATE=0.02` — xem ADR 003) vào bảng `trans` đã có
sẵn `PRIMARY KEY(trans_id)`, `COPY` trực tiếp vào bảng chính thất bại
ngay khi gặp dòng `trans_id` trùng đầu tiên:
Nguyên nhân: `COPY` trong Postgres chạy trong 1 transaction — gặp lỗi
constraint là rollback toàn bộ batch, không phải skip riêng dòng lỗi.

Có 2 hướng xử lý:
- **Pre-process ở tầng application**: dùng pandas/Python loại bỏ duplicate
  trong CSV trước khi COPY vào Postgres
- **Xử lý ở tầng database**: COPY vào staging table (không constraint),
  sau đó `INSERT ... ON CONFLICT DO NOTHING` để merge vào bảng chính

## Decision
Chọn **staging table + `ON CONFLICT DO NOTHING`**.

## Rationale
**Tại sao không pre-process ở tầng application:**
Corruption injection (duplicate trans_id) được thiết kế có chủ đích để
mô phỏng tình huống thực tế — Kafka producer retry gửi lại cùng 1 message.
Trong hệ thống thật, việc phát hiện và xử lý duplicate là trách nhiệm của
tầng lưu trữ (idempotent upsert), không phải được lọc sạch trước khi tới
database. Nếu pre-process loại bỏ duplicate bằng Python trước khi COPY,
corruption injection sẽ mất tác dụng ở đúng điểm quan trọng nhất — nó sẽ
không bao giờ thực sự chạm tới tầng database, làm giảm giá trị mô phỏng
của bài test.

**Lý do chọn staging table thay vì INSERT trực tiếp (row-by-row):**
- `COPY` vào staging table không có constraint vẫn giữ được tốc độ bulk-load
  (khác với `INSERT` từng dòng qua psycopg2, vốn chậm hơn nhiều với ~1 triệu dòng)
- `INSERT ... SELECT ... ON CONFLICT DO NOTHING` cho phép Postgres tự xử lý
  dedupe ở tầng set-based, một lần duy nhất, thay vì phải catch exception
  từng dòng trong code Python
- Pattern này tái sử dụng được cho mọi lần load dữ liệu lớn có khả năng
  trùng lặp trong tương lai, không giới hạn riêng cho load test

## Consequences
**Positive:**
- Xử lý đúng bản chất corruption injection — duplicate được database
  tự phát hiện và loại bỏ, không phải lọc trước bằng tay
- Giữ được throughput của bulk COPY, không giảm tốc vì xử lý duplicate
- Log rõ ràng số dòng trong staging (bao gồm duplicate) vs số dòng thực
  sự được insert — hữu ích khi cần verify tỷ lệ corruption injection
  có khớp với `CORRUPTION_RATE_DUPLICATE` đã cấu hình hay không

**Negative:**
- Tốn thêm disk I/O tạm thời (ghi vào staging trước, rồi merge, rồi xóa
  staging) so với COPY thẳng 1 lần — chấp nhận được vì đây là thao tác
  one-off, không chạy thường xuyên
- Script load phức tạp hơn 1 chút (4 bước: tạo staging → COPY → merge →
  drop) so với COPY đơn giản ban đầu
