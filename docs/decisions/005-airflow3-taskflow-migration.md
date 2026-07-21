# ADR 005 — Migration sang TaskFlow API cho Airflow 3.x
**Status:** Accepted
**Date:** 2026-07-19
**Deciders:** Nhựt (sole engineer)
---
## Context
DAG `banking_batch_enrichment` ban đầu viết theo style cổ điển:
`with DAG(...) as dag:` + `BashOperator` import từ `airflow.operators.bash`.
Khi chạy trên Airflow 3.2.2, phát hiện `airflow.operators.bash` là legacy
import path — core operator đã tách ra package riêng `apache-airflow-providers-standard`
kể từ Airflow 3.0. Tương tự, `from airflow import DAG` (qua `airflow.models`)
cũng là legacy path, deprecated, khuyến nghị dùng `airflow.sdk`.

Có 2 hướng xử lý:
- **Minimal fix**: chỉ đổi 2 dòng import (`airflow.providers.standard.operators.bash`
  + `airflow.sdk.DAG`), giữ nguyên style `with DAG(...) as dag:` + `BashOperator`
- **Full migration**: viết lại theo TaskFlow API (`@dag`, `@task.bash`)

## Decision
Chọn **full migration sang TaskFlow API**.

## Rationale
**Tại sao không chỉ minimal fix:**
Minimal fix giải quyết được lỗi import ngay lập tức, nhưng để lại code
theo style cũ (Airflow 1.x/2.x) trong khi TaskFlow là pattern chính thức
được khuyến nghị cho Airflow 3.x (AIP-72) — mọi ví dụ trong doc chính thức,
mọi DAG mẫu mới đều dùng TaskFlow. Giữ style cũ tạo ra "công nợ kỹ thuật"
sẽ phải sửa lại sau này.

**Lợi ích cụ thể của TaskFlow trong project này:**
- Cú pháp gọn hơn — không cần `task_id` lặp lại tên biến, không cần quản lý
  `with DAG(...) as dag:` context manager thủ công
- `@task.bash` cho phép trả về bash command dưới dạng return value của hàm
  Python — dễ mở rộng logic động (ví dụ build command string có điều kiện)
  hơn so với truyền `bash_command` string tĩnh vào `BashOperator`
- Nếu sau này cần thêm task Python xen giữa (ví dụ validate kết quả dbt test
  trước khi tiếp tục), TaskFlow cho phép trộn `@task` (Python) và `@task.bash`
  (shell) tự nhiên trong cùng 1 luồng dependency, không cần định nghĩa thêm
  operator riêng

**Đánh đổi được chấp nhận:**
`@task.bash` yêu cầu package `apache-airflow-providers-standard` — cần đảm bảo
package này có sẵn trong image `apache/airflow:3.2.2` (verify qua
`pip show apache-airflow-providers-standard` trong container).

## Consequences
**Positive:**
- DAG code khớp với pattern chính thức, dễ maintain lâu dài, dễ đọc với
  ai quen Airflow 3.x
- Sẵn sàng mở rộng thêm task Python mà không cần đổi kiến trúc DAG
- Không còn phụ thuộc legacy import path, tránh lỗi khi Airflow tiếp tục
  loại bỏ backward-compatibility trong các phiên bản sau

**Negative:**
- Cú pháp TaskFlow khác biệt với style cổ điển — cần thời gian làm quen
  nếu người review quen với `BashOperator` truyền thống
- Thêm 1 dependency package (`apache-airflow-providers-standard`) cần verify
  có sẵn trong môi trường deploy
