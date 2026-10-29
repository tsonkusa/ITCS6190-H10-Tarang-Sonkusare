# Hands-on L10: Report

**Name:** Tarang Sonkusare  
**Student ID:** 801352372  
**Email:** tsonkusa@charlotte.edu

---

## Seed and commands

Seed used for `datagen.py`: 801352372.

The data generator produced 12 files and 304 ride events, including 21 events late by 1–2 minutes and 8 events late by 8–10 minutes.

I used GitHub Codespaces, so I started the cluster with `docker-compose.codespaces.yml` rather than the local Docker Desktop Compose file. My commands, in order, were:

```bash
python3 datagen.py 801352372
ls -l shared-folder/stream/
wc -l shared-folder/stream/*.jsonl

docker compose -f docker-compose.codespaces.yml up -d
docker ps

docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task1 /opt/spark/work-dir/shared
cat shared-folder/output/task1/*.csv | grep -vc trip_id

docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared
cat shared-folder/output/task2/*.csv | grep -v window_start | sort

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared update

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task1 /opt/spark/work-dir/shared
ls -la shared-folder/checkpoint/task1
ls -la shared-folder/checkpoint/task2
```
---

## Results

### Task 1: ingest and parse

The source generated 304 events across twelve files. Task 1 processed all 304 rows and wrote 304 CSV data rows with the columns `trip_id`, `driver_id`, `distance_km`, `fare_amount`, `fare_per_km`, and `event_time`. The derived `fare_per_km` is rounded to two decimals.

```
=== task1: micro-batch summary ===
batch input rows            watermark state rows
    0         22                    -          -
    1         27                    -          -
    2         29                    -          -
    3         28                    -          -
    4         26                    -          -
    5         27                    -          -
    6         24                    -          -
    7         29                    -          -
    8         28                    -          -
    9         20                    -          -
   10         20                    -          -
   11         24                    -          -

$ cat shared-folder/output/task1/*.csv | grep -vc trip_id
304
```

How many micro-batches and why? There were 12 input micro-batches (batch IDs 0–11), because the generator created twelve files and `read_rides()` uses `maxFilesPerTrigger=1`. Each trigger processed one input file. Task 1 is stateless; it needs no event-time watermark or aggregation state.

### Task 2: trips and fares per window

Task 2 used 2-minute tumbling event-time windows with a 2-minute watermark. In append mode, five completed windows were emitted as CSV; two later windows were not yet eligible for append output.

Actual append-mode CSV rows:

```
2026-10-27 07:58:00,2026-10-27 08:00:00,3,144.85
2026-10-27 08:00:00,2026-10-27 08:02:00,53,2053.67
2026-10-27 08:02:00,2026-10-27 08:04:00,60,2529.01
2026-10-27 08:04:00,2026-10-27 08:06:00,48,1952.57
2026-10-27 08:06:00,2026-10-27 08:08:00,55,2263.51
```

Append-mode micro-batch summary:

```
=== task2: micro-batch summary ===
batch input rows            watermark state rows
    0         22                    -          2
    1         27  2026-10-27 07:58:57          2
    2         29  2026-10-27 07:59:48          3
    3         28  2026-10-27 08:00:52          2
    4         26  2026-10-27 08:01:43          3
    5         27  2026-10-27 08:02:57          2
    6         24  2026-10-27 08:03:51          3
    7         29  2026-10-27 08:04:48          2
    8         28  2026-10-27 08:05:58          3
    9         20  2026-10-27 08:06:48          2
   10         20  2026-10-27 08:07:41          3
   11         24  2026-10-27 08:08:52          2
   12          0  2026-10-27 08:09:52          2
```

The extra zero-input batch (12) is a stateful watermark-processing batch; the first twelve batches consumed the twelve input files.

Batch aggregation over all files (no watermark):

```
+-------------------+-------------------+-----+----------+
|window_start       |window_end         |trips|total_fare|
+-------------------+-------------------+-----+----------+
|2026-10-27 07:58:00|2026-10-27 08:00:00|4    |223.87    |
|2026-10-27 08:00:00|2026-10-27 08:02:00|60   |2311.04   |
|2026-10-27 08:02:00|2026-10-27 08:04:00|60   |2529.01   |
|2026-10-27 08:04:00|2026-10-27 08:06:00|48   |1952.57   |
|2026-10-27 08:06:00|2026-10-27 08:08:00|55   |2263.51   |
|2026-10-27 08:08:00|2026-10-27 08:10:00|38   |1498.3    |
|2026-10-27 08:10:00|2026-10-27 08:12:00|39   |1674.31   |
+-------------------+-------------------+-----+----------+
```

The batch calculation includes every event; the streaming append calculation has the expected late-event losses and deliberately does not emit unfinalized windows. The append output contains 219 trips in five finalized windows (3 + 53 + 60 + 48 + 55), compared with 227 events across the same five windows in the full batch calculation.

---

## Questions

### 1. Append versus update

In update mode, each window appears when events for that window first arrive, and it can be printed again as its aggregate changes. The first appearances in my console run were: 07:58–08:00 in batch 0, 08:00–08:02 in batch 0, 8:02–08:04 in batch 2, 08:04–08:06 in batch 4, 08:06–08:08 in batch 6, 08:08–08:10 in batch 8, and 08:10–08:12 in batch 10.

In append mode, the five completed windows become eligible when the watermark reaches their ending timestamp. Based on the micro-batch progress watermarks, this happens in batch 3 for 07:58–08:00 (watermark 08:00:52), batch 5 for 08:00–08:02 (08:02:57), batch 7 for 08:02–08:04 (08:04:48), batch 9 for 08:04–08:06 (08:06:48), and batch 11 for 08:06–08:08 (08:08:52). These are the batches that can finalize and write those windows in append mode. The 08:08–08:10 and 08:10–08:12 windows appear in update mode but are absent from append output because the last observed watermark is 08:09:52, before either window end.

The difference is that update mode reports changing, intermediate aggregates while append mode writes a window only after Spark considers it final according to the watermark. An update-mode batch can reprint a window when new rides are accepted into it.

### 2. Where did the rows go?

The 07:58–08:00 window has streaming trips versus 4 batch trips (fewer trip, fares $144.85 versus $223.87, or $79.02 less). The 08:00–08:02 window has 53 streaming trips versus 60 batch trips (7 fewer trips, fares $2,053.67 versus $2,311.04, or $257.37 less). The other three finalized windows match the batch counts and fares exactly: 08:02–08:04 (60 trips), 08:04–08:06 (48 trips), and 08:06–08:08 (55 trips).

These 8 excluded trips are explained by the generator's very-late events (8–10 minutes late), which arrived after the two-minute watermark had passed their original event-time windows. The full batch aggregation has no watermark and includes all 304 trips, so it still counts them.

Two windows are missing entirely from append output: 08:08–08:10 (38 trips, $1,498.30 in the batch comparison) and 08:10–08:12 (39 trips, $1,674.31 in batch). They are not necessarily dropped: the stream ends with a final watermark of 08:09:52, which has not advanced past either window's end. Later incoming events with sufficiently newer event times would advance the watermark and allow Spark to finalize and append these windows. Simply rerunning the same data with the same checkpoint would not do so.

### 3. Checkpoint

Running Task 1 again without deleting `shared-folder/checkpoint/task1` produced a new micro-batch (batch 12) with 0 input rows. Spark had already recorded the twelve input files as processed, so it did not ingest or rewrite the 304 existing trips.

The Task 1 checkpoint contained `metadata` (the query identifier), `offsets/` (recorded source progress), `commits/` (which micro-batches successfully committed), and `sources/` (the file source's processed-file tracking). Task 2 had the same entries plus `state/`, which stores state needed for the window aggregation and watermark-based cleanup. The `.metadata.crc` file is a filesystem checksum artifact, not a separate query checkpoint state component.

---

## Problems and fixes

No blocking errors occurred. The initial Codespaces cluster started successfully, and the master connected to both Spark workers. All twelve files were processed by Task 1, and Task 2 completed successfully in both append and update modes.

Spark printed the following non-blocking warnings:

```
WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
WARN MicroBatchExecution: Disabling AQE since AQE is not supported in stateful workloads.
```

These did not require a code change: Spark used built-in Java classes and automatically disabled adaptive query execution for streaming. The final results were produced successfully.
