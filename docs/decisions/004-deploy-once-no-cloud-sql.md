# ADR 004 — Deploy 1 lần, không dùng Cloud SQL

**Status:** Accepted  
**Date:** 2026-07-01  
**Deciders:** Nhựt (sole engineer)

---

## Context

Project cần demonstrate pipeline chạy ở cloud scale (1M rows, real throughput)
để làm bằng chứng đính kèm CV. Có 2 lựa chọn cho Postgres trên GCP:

- **Cloud SQL** (managed Postgres): tính phí theo giờ tồn tại, dù không có
  traffic. $0.017/giờ cho instance nhỏ nhất = ~$12/tháng nếu để chạy.
- **VM tạm thời** (Compute Engine): tạo VM, chạy Docker Compose, load test,
  xóa VM. Chỉ tính phí trong thời gian chạy thật (~2-3 giờ).

GCP free trial cung cấp $300 credit, không phải always-free tier.
BigQuery vẫn dùng managed service (nằm trong always-free: 1TiB query/tháng).

## Decision

Dùng **VM tạm thời** trên Compute Engine, **không dùng Cloud SQL**.
Deploy **1 lần duy nhất** để chạy load test và thu thập bằng chứng.

## Rationale

**Tại sao không dùng Cloud SQL:**

Cloud SQL tính phí theo giờ tồn tại (uptime), không theo usage. Nếu để
instance chạy 24/7 trong 1 tháng để demo bất cứ lúc nào: ~$12-15/tháng
chỉ cho Postgres. Với $300 credit có thể hết trong 20 tháng — nhưng
quan trọng hơn, đây là tiền thật (sau khi hết trial), không phù hợp
với mục tiêu "deploy để prove, không phải deploy để run forever".

**Triết lý "deployable > uptime":**

Project portfolio không cần 24/7 uptime như production system. Cần chứng
minh 2 thứ:
1. **Deployable**: `docker compose up` trên bất kỳ máy nào → pipeline chạy
2. **Scalable**: có screenshot/video khi chạy ở cloud scale với 1M rows

Cả 2 đều đạt được mà không cần Cloud SQL thường trực.

**Workflow cloud deployment:**

```
1. Tạo VM e2-standard-4 (4 vCPU, 16GB RAM) trên GCP
2. Install Docker + Docker Compose
3. Clone repo, docker compose up
4. Load 1M rows, chạy Speed Layer
5. Chụp screenshot dashboard (Kafka UI, metrics)
6. Quay video pipeline đang chạy
7. XÓA VM NGAY — không để tồn tại qua đêm
8. Giữ lại: screenshot, video, BigQuery dataset (always-free)
```

**BigQuery vẫn là managed service:**

BigQuery không tính phí theo uptime mà theo query scan volume.
1TiB query/tháng là free. Dataset được giữ lại vĩnh viễn trong
always-free tier (10GB storage/tháng free). Đây là exception hợp lý:
BigQuery serving layer luôn available để demo mà không tốn phí.

**Local environment vẫn là primary:**

`docker compose up` trên máy local (WSL2) là môi trường phát triển
hằng ngày và đủ để demo live khi phỏng vấn. Cloud chỉ là bằng chứng
bổ sung cho load test scale.

## Consequences

**Positive:**
- Tiết kiệm chi phí: chỉ tốn ~$2-5 credit GCP cho 2-3 giờ VM
- Không lo quên tắt service, không phát sinh phí ngoài ý muốn
- Đủ bằng chứng: screenshot + video + BigQuery data accessible

**Negative:**
- Không có live cloud demo 24/7 — phải dùng recording
- Setup cloud mất ~30 phút mỗi lần (nếu cần chạy lại)

**Mitigation:**
- Makefile có `make up` để spin up local ngay lập tức khi phỏng vấn
- Recording + BigQuery dashboard đủ để chứng minh cloud capability
- README có hướng dẫn cloud deployment để reviewer tự reproduce nếu muốn