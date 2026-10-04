# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Trần Mạnh Tùng - 2A202602879
**Repo:** https://github.com/manhtungai247/K4-Track02-Day17-TranManhTung-2A202602879-DataPipelineEngineering
**Commit mã đã kiểm tra:** `b00f6904507a5a6f84f1488908c361dc7dc5520f`
**AI đã dùng và phạm vi hỗ trợ:** OpenAI Codex hỗ trợ tìm lỗi, đưa ra đề xuất sửa code.
**Nguồn tham khảo khác:** README, docs và tests có sẵn trong repo.

## 1. Ba lỗi

| | Silver | Late data | CDC delete |
|---|---|---|---|
| **Triệu chứng** | 24 dòng/12 ticket; T-91 có 3 trạng thái. | u05 08/12 `(2,1,0)`, cần `(5,3,1)`; checksum lệch. | T-97 còn active trong snapshot mới và RAG. |
| **Nguyên nhân** | Chỉ dedup trong batch rồi `INSERT`; không có merge theo key/LSN. | Lookback `0`, nhưng Bronze P99 lateness `3` ngày. | `after=null`; staging bỏ khoá nằm trong `before`/Kafka key. |
| **Sửa / khái niệm** | `pipeline/silver.py`: MERGE ticket; update khi LSN mới hơn. Silver key, CDC ordering, idempotency. | `pipeline/config.py`: lookback `3`, overwrite partition theo event date. Event time, lateness, microbatch. | `pipeline/staging.py` và staging dbt: key = `coalesce(after,before,key)`; delete thành tombstone sạch PII, giữ LSN. Debezium delete ≠ Kafka tombstone. |

## 2. Các con số

- Bronze: 43 events; P50 `0.00`, P95 `2.90`, P99 `3.00`, max `3` ngày. Chọn lookback `3 = ceil(P99)`.
- Rerun **PASS**, C0=C1=C2=C3; Gold `39e115c510ecdf526800eac227158a4f`. dbt parity **PARITY**.

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE/key + điều kiện LSN giữ đúng trạng thái; overwrite partition tính lại feature chịu ảnh hưởng của event muộn. Tombstone giữ thứ tự CDC để replay không hồi sinh ticket, đồng thời xoá PII khỏi trạng thái hiện tại.
- Snapshot “as of” tái lập lịch sử; yêu cầu xoá thật cần purge/rebuild snapshot cũ, cache, backup và bản sao theo retention, kèm audit không PII. Bất biến không miễn trừ nghĩa vụ xoá.
- DuckDB/dbt đủ nhẹ cho seed cục bộ; dbt bổ sung merge, microbatch, tests và parity. Spark không đáng chi phí vận hành ở quy mô lab.

## 4. Hai câu hỏi suy ngẫm

1. Khi có yêu cầu xoá, purge hoặc rebuild snapshot, chunks, cache, backups và bản sao theo retention; giữ audit metadata không PII. Tính bất biến không có nghĩa giữ dữ liệu cá nhân mãi.
2. Đặt PII detection ở Silver gate trước vùng downstream; bổ sung entity detection và review. Đo precision/recall trên tập gán nhãn, missed PII và false positives; regex hiện tại không bắt tên.

## 5. Output kiểm chứng

```text
$ .venv\Scripts\python.exe -m scripts.verify
RESULT: 18/18 checks — ALL PASS

$ .venv\Scripts\python.exe -m pytest
34 passed in 2.26s

$ .venv\Scripts\python.exe -m scripts.rerun_check
fresh build             8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1  9370ca77af23  cb9ebd12fdcc  39e115c510ecdf526800eac227158a4f
RESULT: PASS — 3 re-runs, identical checksums

$ .venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .venv\Scripts\python.exe -m scripts.parity
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
