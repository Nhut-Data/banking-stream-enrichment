# Load Test Results — GCP Compute Engine

**Ngày thực hiện:** 20/07/2026
**Môi trường:** Google Compute Engine, `e2-standard-4` (4 vCPU, 16GB RAM), zone `asia-southeast1-b`
**Mục tiêu:** Kiểm chứng khả năng chịu tải của Batch Layer (Postgres → dbt → BigQuery) ở quy mô dữ liệu lớn hơn dataset gốc, đồng thời khai thác bằng chứng vận hành thực tế trên cloud.

---

## Kết quả tổng quan

| Giai đoạn | Số dòng |
|---|---|
| Dataset Berka gốc (1993–1998) | 1,056,320 |
| Dữ liệu synthetic thêm vào (`trans_loadtest.csv`) | 1,000,000 |
| **Tổng trong Postgres sau load test** | **2,056,320** |
| BigQuery `fct_enriched_transactions` (sau INNER JOIN) | 2,036,711 |
| Chênh lệch (referential integrity violations, loại bỏ có chủ đích) | 19,609 (~1.96%) |

Chênh lệch giữa Postgres và BigQuery không phải lỗi — model `fct_enriched_transactions` dùng `INNER JOIN` giữa `trans` và `int_customer_profile`, chủ động loại bỏ các giao dịch có `account_id` không tồn tại (triết lý *"correctness over completeness"* cho batch layer). Tỷ lệ loại bỏ (~1.96%) khớp gần như tuyệt đối với `CORRUPTION_RATE_REFERENTIAL=0.02` (2%) đã cấu hình khi sinh dữ liệu synthetic.

## dbt test — kết quả

Done. PASS=30 WARN=1 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=31
1 warning duy nhất (`relationships_stg_berka__trans_account_id...`, 19,609 kết quả) — được cấu hình `severity: warn` có chủ đích, vì đây chính là corruption injection đang hoạt động đúng thiết kế, không phải lỗi hệ thống.

## Bug phát hiện qua load test

**Vị trí:** `dbt/models/staging/berka/stg_berka__trans.sql` — logic parse cột `date` (kiểu `INTEGER`, format Berka `YYMMDD`) sang kiểu `DATE`.

**Nguyên nhân (root cause chính xác, xác nhận qua unit test viết sau đó):** Vấn đề bắt nguồn từ chính `data_generation/generator.py` — hàm `_date_to_berka_int()` dùng `int(ts.strftime('%y%m%d'))` để encode ngày tháng, và với năm 2000–2009 (`yy` = `00`-`09`), `int()` cắt mất số 0 đầu ngay tại thời điểm sinh data (VD: 2000-01-01 → sinh ra `101`, không phải `000101`). Postgres và BigQuery chỉ trung thực lưu lại đúng giá trị đã mất thông tin này — không phải nơi gây ra lỗi. Điều này khiến logic phân biệt thế kỷ ở `stg_berka__trans.sql` (dựa trên ký tự đầu của chuỗi, giả định luôn đủ 6 ký tự) tính sai, gây lỗi:
Failed to parse input string "191017"
**Fix:** Thêm `LPAD(CAST(date AS STRING), 6, '0')` ở tầng dbt để phục hồi số 0 đầu trước khi áp dụng logic phân biệt thế kỷ — đây là fix đúng vị trí (chặn tại staging layer, nơi mọi downstream consumer đều được bảo vệ), dù root cause thật nằm sớm hơn ở generator. Một unit test (`test_date_stored_as_raw_int_can_lose_leading_zero_documented_behavior`) được thêm sau đó để document rõ hành vi này, đảm bảo nếu generator được sửa để tự zero-pad trong tương lai, sẽ có cảnh báo để rà lại xem LPAD ở dbt còn cần thiết không.

**Ý nghĩa:** Đây là edge case mà dataset Berka gốc (giới hạn trong khung thời gian 1993–1998) không thể bộc lộ — chỉ xuất hiện khi mở rộng dữ liệu sang thập niên 2000+. Minh chứng rõ ràng cho giá trị thực tế của load testing: không chỉ đo hiệu năng, mà còn phát hiện lỗi tiềm ẩn nằm ngoài phạm vi dữ liệu test thông thường.

## Hạ tầng & vận hành

- VM tạo với `--max-run-duration=2h --instance-termination-action=DELETE` — tự động xóa sau 2 tiếng, không phụ thuộc thao tác thủ công, đảm bảo an toàn chi phí.
- Verify sau khi xóa: `gcloud compute instances/disks/addresses list` — đều trả về 0 kết quả, xác nhận không phát sinh chi phí ngoài dự kiến.
- Toàn bộ chi phí phát sinh nằm trong khoảng vài nghìn VND, sử dụng GCP Free Trial credit.

## Bằng chứng trực quan

Dashboard Looker Studio (kết nối live tới BigQuery, không phải ảnh chụp tĩnh) tự động phản ánh số liệu mới ngay sau khi load test hoàn tất — scorecard "Record Count" hiển thị đúng **2,036,711**, khớp với kết quả `dbt run`.

[📊 Xem Dashboard](<https://datastudio.google.com/reporting/d385a2f5-2e41-4454-acdc-3580f5df032d>)

## Các commit liên quan

- `d1a6d2c` — feat: script append synthetic load-test data vào bảng `trans`
- `ed28e50` — fix: dùng staging table + `ON CONFLICT` xử lý duplicate trans_id (corruption injection)
- `a7d503d` — fix: phục hồi số 0 đầu trong date field trước logic phân biệt thế kỷ
