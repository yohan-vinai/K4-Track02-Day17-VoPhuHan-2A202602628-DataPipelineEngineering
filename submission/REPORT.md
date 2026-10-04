# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Võ Phú Hãn / 2A202602628
**Repo:** https://github.com/yohan-vinai/K4-Track02-Day17-VoPhuHan-2A202602628-DataPipelineEngineering
**Commit bài nộp:** Code đã kiểm tra: `33d7d5e13d31c8e3dd166551cb4caae7529c1dde`; REPORT/checksum được bổ sung trong commit kế tiếp.
**AI đã dùng và phạm vi hỗ trợ:** Codex hỗ trợ đọc đề, phân tích baseline, sửa ba lỗi, chạy kiểm tra và soạn báo cáo; học viên cần review diff và giải thích các thay đổi trước khi nộp.
**Nguồn tham khảo:** README, CHECKPOINTS, RULES, RUBRIC và SUBMISSION của đề bài; không dùng lời giải ngoài repo.

## 1. Ba lỗi

| | Silver | Late data | CDC delete |
|---|---|---|---|
| **Triệu chứng** | 24 hàng / 12 ticket; T-91 có cả trạng thái cũ; chunks bị nhân bản. | Feature lệch full recompute; u05 ngày 08-12 chỉ có 2 events, 0 down. | T-97 còn nội dung ở Silver, 1 hàng trong snapshot mới nhất, 2 chunks trong RAG. |
| **Nguyên nhân** | Dedup trong batch nhưng INSERT append giữa các batch; không có LSN guard. | LOOKBACK_DAYS=0 bỏ qua events tới muộn 3 ngày. | Khoá lấy từ after; delete có after=null nên bị lọc mất. |
| **Cách sửa** | silver.py: MERGE theo ticket_id; UPDATE chỉ khi LSN nguồn lớn hơn đích. | config.py: lookback=3 từ ceil(P99 đo ở Bronze); dùng logic overwrite partition có sẵn. | staging.py: op=d lấy ticket_id từ before; cột after vẫn null. Luồng Silver/Gold có sẵn áp dụng tombstone và lọc delete. |
| **Khái niệm** | Silver có khoá; keyed upsert; idempotency; thứ tự CDC. | Event time khác ingest time; đo lateness; recompute lookback. | CDC delete khác Kafka tombstone; xoá phải lan; chống hồi sinh khi replay. |

## 2. Các con số

- Baseline: verify 8/18; pytest 9 failed, 25 passed. Sau sửa: verify 18/18; pytest 34 passed.
- 43 records Bronze: P50=0, P95=2.90, P99=3, max=3 ngày → LOOKBACK_DAYS=3.
- Rerun PASS: C0=C1=C2=C3=`39e115c510ecdf526800eac227158a4f`.
- dbt PASS=19, ERROR=0; parity PARITY cho silver_tickets và gold_feature_daily.

## 3. Lựa chọn kỹ thuật

- MERGE giữ một thực thể theo khoá; LSN guard ngăn batch cũ ghi đè trạng thái mới. Feature là tổng hợp user/ngày nên overwrite partition tránh cộng lặp khi replay.
- Tombstone giữ khoá và LSN để chặn dữ liệu cũ hồi sinh; đổi lại vẫn giữ một hàng metadata.
- Training snapshot đọc Bronze as-of từng ngày để tránh rò rỉ thông tin tương lai và tái lập tập huấn luyện; snapshot cũ giữ nguyên, feedback muộn xuất hiện ở phiên bản mới.
- DuckDB phù hợp seed nhỏ, local, zero-key; dbt cung cấp model SQL, contracts/tests và parity. Spark thêm vận hành phân tán chưa cần thiết ở quy mô lab.

## 4. Hai câu hỏi suy ngẫm

1. Bất biến phục vụ tái lập, nhưng không nên ngăn xử lý yêu cầu xoá. Trong hệ thống thật, lập sổ yêu cầu xoá và truy dấu mọi bản sao: Bronze, snapshots, transcripts, cache, backup và model đã huấn luyện. Thu hồi quyền truy cập snapshot bị ảnh hưởng, tạo phiên bản đã loại dữ liệu và xoá bản cũ theo chính sách; giữ audit metadata không chứa PII. Đánh giá huấn luyện lại model. Lab chỉ loại T-97 khỏi snapshot mới nhất và RAG; chưa thực hiện xoá toàn hệ thống.
2. Đặt chốt phát hiện/che PII tại Bronze→Silver trước khi dữ liệu sang Gold: regex kết hợp NER cho tên người, địa chỉ và định danh; kiểm tra lại trước indexing/training, đưa trường hợp không chắc vào quarantine. Đo precision/recall từng loại PII trên tập gán nhãn, tỷ lệ rò rỉ sau masking và tỷ lệ chặn nhầm. Hạn chế truy cập Bronze vì vẫn giữ dữ liệu gốc; regex hiện tại chưa che tên người.

## 5. Output thực tế

Chạy ngày 04/10/2026 trên macOS, Python 3.11.15; dbt-core 1.12.5 và dbt-duckdb 1.11.0. Phần phân tích mục 1–4 tách khỏi output. Không thực hiện bonus trong bài nộp này.

### make verify

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

### make test

```text
$ make test
..................................                                       [100%]
34 passed in 0.87s
```

### make rerun3

```text
$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

### make lateness

```text
$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

### make dbt

```text
$ DBT_USE_COLORS=false make dbt
cd dbt_project && DBT_PROFILES_DIR=. /Users/vophuhan/Everything/Projects/VinAI/Lab/K4-Track02-Day17-Data-Pipeline-Engineering/.venv/bin/dbt build --event-time-start 2026-08-10 --event-time-end 2026-08-17
05:45:57  Running with dbt=1.12.5
05:45:57  Registered adapter: duckdb=1.11.0
05:45:58  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:45:58
05:45:58  Concurrency: 1 threads (target='dev')
05:45:58
05:45:58  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:45:58  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.04s]
05:45:58  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:45:58  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
05:45:58  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:45:58  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.07s]
05:45:58  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:45:58  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.07s]
05:45:58  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:45:58  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.07s]
05:45:58  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:45:58  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.02s]
05:45:58  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:45:58  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
05:45:58  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:45:58  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
05:45:58  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:45:58  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
05:45:58  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:45:58  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.01s]
05:45:58  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:45:58  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.01s]
05:45:58  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:45:58  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
05:45:58  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:45:58  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
05:45:58  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:45:58  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
05:45:58  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:45:58  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
05:45:58  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:45:58  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:45:58  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.02s]
05:45:58  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:45:58  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.01s]
05:45:58  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:45:58  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.01s]
05:45:58  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:45:58  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.01s]
05:45:58  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:45:58  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.01s]
05:45:58  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:45:58  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.01s]
05:45:58  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:45:58  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.01s]
05:45:58  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.10s]
05:45:58  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:45:58  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.01s]
05:45:58  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:45:58  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
05:45:58  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:45:58  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
05:45:58
05:45:58  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.61 seconds (0.61s).
05:45:58
05:45:58  Completed successfully
05:45:58
05:45:58  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

### make parity

```text
$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
