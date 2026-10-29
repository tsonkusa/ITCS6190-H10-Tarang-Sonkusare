# Hands-on L10: Ride-Sharing Analytics with Spark Structured Streaming

**ITCS 6190/8190 - Cloud Computing for Data Analysis - Fall 2026**

Until now every Spark job you wrote read a complete dataset. This week the data arrives
while the job runs: ride events from a ride-sharing platform, one file per minute, and you
process them with **Structured Streaming** on the Docker cluster from L5. You write two
streaming queries: a stateless projection to a file sink, and a windowed aggregation on
event time with a watermark, run in append and in update mode. Then you look at what the
watermark did to the late events, and at what a checkpoint contains.

**Worth 1 point.** Submit the repository link on Canvas by 11:59 pm on the day of the class.
Work individually.

---

## The data

`datagen.py` writes twelve files, `rides_00.jsonl` to `rides_11.jsonl`, into
`shared-folder/stream/`, from a seed that you choose (your student ID). Each file holds the
events that *arrived* during one minute of October 27, 2026, from 8:00 to 8:11, one JSON
object per line:

```json
{"trip_id": "T00001", "driver_id": "D04", "distance_km": 19.8, "fare_amount": 45.87, "event_time": "2026-10-27 08:00:15"}
```

| column        | type      | notes                                                   |
| ------------- | --------- | ------------------------------------------------------- |
| `trip_id`     | string    | unique                                                  |
| `driver_id`   | string    | `D01` to `D20`; a few drivers are much busier           |
| `distance_km` | double    | 1 to 30                                                 |
| `fare_amount` | double    | 5 to 80                                                 |
| `event_time`  | timestamp | when the ride ended, **not** when the event arrived     |

Most events have an `event_time` in the minute of their file. Some are **late**: their
`event_time` is one to two minutes earlier (a phone that reconnected). A few are **very
late**: eight to ten minutes earlier (a batch upload). About 290 events in all. The files
get modification times one minute apart, in order, because Spark's file source reads new
files oldest first; generate them where Spark reads them and do not copy them around.

With `maxFilesPerTrigger = 1` and the `availableNow` trigger, each file becomes one
micro-batch, in order, and the query stops after the last one: a reproducible stream.

---

## The tasks

`main.py` sets up the SparkSession, the schema and the streaming source (`read_rides()`),
and runs one task per invocation:

```
spark-submit main.py <task> <shared directory> [append|update]
```


Each task is a function that returns a started `StreamingQuery`; `main.py` waits for it and
prints a summary of the micro-batches (input rows, watermark, state rows), the same numbers
the Structured Streaming tab of the Spark UI shows. The docstring of each function states
exactly what to produce.

**Task 1. Ingest and parse (stateless).** Add `fare_per_km = fare_amount / distance_km`
rounded to 2 decimals and write the rows, as they arrive, to a CSV **file sink** under
`shared-folder/output/task1`, output mode append, with a checkpoint. No state is involved:
every row is final the moment it arrives.

**Task 2. Trips and fares per window (event time and watermark).** Per 2-minute tumbling
window of `event_time`, with a **2-minute watermark**: `window_start, window_end, trips,
total_fare`. In **append** mode the result goes to a CSV file sink under
`shared-folder/output/task2`; in **update** mode (third argument) it goes to the console.
After the stream, `main.py` computes the same windows in batch over all the files, so you
can see what the watermark dropped and which windows were never written.

The queries use the DataFrame API. No RDD operations, no `collect()` loops, no pandas.

---

## What is in this repository

| Path                     | What it is                                                                          |
| ------------------------ | ----------------------------------------------------------------------------------- |
| `docker-compose.yml`     | the cluster from Hands-on L5: one Spark master and two workers, `apache/spark:4.2.0` |
| `docker-compose.codespaces.yml`, `conf/spark-defaults.conf` | the same cluster, for GitHub Codespaces (see step 3) |
| `datagen.py`             | generates the twelve event files from a seed (plain Python, no packages needed)     |
| `main.py`                | the streaming queries: source and setup are given, the two tasks are yours          |
| `REPORT.md`              | the report template you fill in                                                     |
| `shared-folder/stream/`  | where `datagen.py` writes the event files (the streaming source)                    |
| `shared-folder/output/`  | where the results land (`task1/`, `task2/`)                                         |
| `shared-folder/checkpoint/` | one checkpoint folder per task (not committed)                                   |

`shared-folder/` is mounted into every container at `/opt/spark/work-dir/shared`, so the
files you generate on your machine are visible to the master and the workers, and the
results the workers write appear on your disk.

---

## Prerequisites

- **Docker Desktop**, running.
- **Python 3** on your machine, only to run `datagen.py` (it uses the standard library). If
  you do not have it, step 2 shows how to run it inside the container instead.

---

## Steps

### 1. Make your own copy of this repository

On the repository page, click the green **Use this template** button, then
**Create a new repository**. Name it `ITCS6190-H10-<your-name>` and set the visibility to
**Public**. Do not fork and do not clone this repository directly. Clone *your* repository
and work in it.

### 2. Generate your data

```bash
python3 datagen.py <your student ID>
```

Twelve files appear under `shared-folder/stream/`. Open one of the last ones and find an
event whose `event_time` is several minutes before the others. Write your seed in
`REPORT.md`; the grader regenerates your data from it.

Without Python on your machine, start the cluster first (step 3) and run the generator in
the master container, which has Python 3:

```bash
docker cp datagen.py spark-master:/opt/spark/work-dir/shared/
docker exec spark-master python3 /opt/spark/work-dir/shared/datagen.py <your student ID> \
  /opt/spark/work-dir/shared/stream
```

### 3. Start the cluster

```bash
docker compose up -d
```

Open <http://localhost:8080> and check that the master lists two workers in state ALIVE.

**In a GitHub Codespace**, start the cluster with the other compose file instead, as in
Hands-on L5, L6 and L9:

```bash
docker compose -f docker-compose.codespaces.yml up -d
```

Inside a Codespace, traffic between containers on a Compose network can be dropped, so the
workers never register with the master and every job waits forever with `Initial job has not
accepted any resources`. That file puts the whole cluster on one network to avoid it.
Everything else is the same, except that `docker compose down` in step 7 also needs the
`-f docker-compose.codespaces.yml` flag.

### 4. Run `main.py` as it is

```bash
docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task1 /opt/spark/work-dir/shared
```

It reports `task1: not implemented yet`. The same command with `task2` runs the other
task.

### 5. Fill in the tasks, one at a time

Edit `main.py` on your machine. After each change, repeat the `docker cp` and the
`spark-submit` from step 4 for the task you are working on.

Two things are different from the batch jobs of the previous hands-ons:

- **A query remembers what it processed.** The checkpoint folder of a task records which
  files were consumed. To run a task again on the same files, delete its checkpoint first:
  `rm -rf shared-folder/checkpoint/task1` (or `task2`). Otherwise the query starts, finds
  nothing new, and stops after an empty batch. Task 2 in update mode deletes its own
  checkpoint, because that run is only for looking at the console.
- **You cannot `show()` or `count()` a streaming DataFrame.** Spark answers with
  `Queries with streaming sources must be executed with writeStream.start()`. If you want to
  look at the data while developing, read the folder in batch with `spark.read`, as the
  end of `main.py` does for task 2, or send the stream to the console sink.

While a task runs, the Spark UI at <http://localhost:4040> has a **Structured Streaming**
tab with the query, its input rate, processing rate and batch durations. The run takes
about a minute for twelve batches; open the tab early.

Useful commands to look at the outputs on your machine:

```bash
cat shared-folder/output/task1/*.csv | grep -vc trip_id               # rows written by task 1
cat shared-folder/output/task2/*.csv | grep -v window_start | sort      # the windows written
ls shared-folder/checkpoint/task2                                       # what a checkpoint holds
```

Task 2 writes one CSV file per micro-batch and partition, most of them empty (a header
only): a file sink produces many small files, which is one of the things the lecture
mentioned about file sinks. The `_spark_metadata` folder next to them is the sink's own log
of committed files.

### 6. Run task 2 in both modes and answer the questions

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task2 /opt/spark/work-dir/shared update
```

The first run writes the windows to files; the second prints every batch to the console.
Keep both outputs: the three questions in `REPORT.md` are about them, about the batch table
printed at the end, and about the checkpoint folder.

### 7. Stop the cluster

```bash
docker compose down
```

---

## What to submit

Commit and push:

- `main.py` with the two tasks implemented;
- the outputs under `shared-folder/output/task1` and `task2` (the `part-*.csv`
  files; `.gitignore` leaves out `_SUCCESS`, `_spark_metadata` and the `.crc` files);
- `REPORT.md` filled in, with your seed, the summaries, the results and the answers to the
  three questions.

Do not commit the generated `.jsonl` files or the checkpoints (`.gitignore` takes care of
both) and do not modify `datagen.py`, the compose files or `read_rides()`.

Submit the link to your repository on Canvas.

---

## Grading (1 point)

| Criterion | Points |
| --------- | ------ |
| Repository created from the template, public, with the required files and nothing else | 0.15 |
| Task results: row count and columns of task 1, the windows of task 2, correct for your seed | 0.35 |
| Streaming done right: schema and source untouched, file sink with checkpoint and availableNow in task 1, `withWatermark` before the window aggregation in task 2, append to files and update to the console, one checkpoint per task | 0.20 |
| Questions 1 to 3 answered with the numbers from your own run (append versus update, dropped and missing windows, checkpoint) | 0.20 |
| Results pasted and discussed, problems reported with their error message and fix | 0.10 |

---

## Tips

- The output mode is checked when the query starts. If Spark refuses a combination, read
  the message: it says what the query is and which modes it allows.
- `withWatermark` has to be called on the column that `window()` uses, **before** the
  `groupBy`; placed after the aggregation it has no effect.
- `window.start` and `window.end` are the fields of the `window` struct column; select them
  with `col("window.start")`.
- If a run ends with an exception, the checkpoint keeps the batches that were committed.
  Delete the checkpoint of that task before running it again from the start.
- Container names (`spark-master`) and ports are the same as in Hands-on L5, L6 and L9. If
  `docker compose up` complains that a port is in use, stop the other cluster first.
