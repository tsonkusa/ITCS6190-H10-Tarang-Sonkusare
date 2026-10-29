"""Hands-on L10: Ride-Sharing Analytics with Spark Structured Streaming.

Usage (on the Docker cluster from Hands-on L5):
    spark-submit main.py <task> <shared directory> [append|update]

    <task>              task1 or task2
    <shared directory>  the folder mounted in the containers, /opt/spark/work-dir/shared;
                        events are read from <shared>/stream, results are written to
                        <shared>/output/<task>, checkpoints to <shared>/checkpoint/<task>
    append|update       output mode for task2 (default append)

Each task is a streaming query over the files that datagen.py wrote. With one file per
micro-batch and the availableNow trigger, the query processes the twelve files in order,
one batch each, and stops. Spark records what it processed in the checkpoint folder, so to
run a task again on the same data, delete <shared>/checkpoint/<task> first (otherwise the
query finds nothing new and stops at once).

Fill in the parts marked TODO. Each task is a function that returns a StreamingQuery
(already started); main() waits for it and prints a summary of its micro-batches.
"""
import shutil, sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as ssum, round as sround, window

if len(sys.argv) < 3 or sys.argv[1] not in ("task1", "task2"):
    print(__doc__)
    sys.exit(2)
task, shared = sys.argv[1], sys.argv[2].rstrip("/")
mode = sys.argv[3] if len(sys.argv) > 3 else "append"
stream_dir = f"{shared}/stream"
out_dir = f"{shared}/output/{task}"
ckpt_dir = f"{shared}/checkpoint/{task}"

spark = (SparkSession.builder.appName(f"RideSharing-{task}")
         # 200 shuffle partitions (the default) means 200 state stores per aggregation in
         # every micro-batch; far too many for a stream this small (slide "Performance")
         .config("spark.sql.shuffle.partitions", 2)
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Events as written by datagen.py: one JSON object per line. The schema is mandatory for a
# streaming file source (slide "Step 1: Define the Input Source").
schema = ("trip_id STRING, driver_id STRING, distance_km DOUBLE, fare_amount DOUBLE, "
          "event_time TIMESTAMP")


# ---------------------------------------------------------------- Step 0: the source
def read_rides():
    """The streaming DataFrame of ride events: every file that appears in stream_dir, one
    file per micro-batch (maxFilesPerTrigger = 1) so that each minute of data is one batch.
    """
    return (spark.readStream.schema(schema)
            .option("maxFilesPerTrigger", 1)
            .json(stream_dir))


# ---------------------------------------------------------------- Task 1: ingest and parse
def task1_ingest():
    """Parse the events and write them, as they arrive, to a CSV file sink.

    Add a column fare_per_km = fare_amount / distance_km rounded to 2 decimals, keep the
    columns trip_id, driver_id, distance_km, fare_amount, fare_per_km, event_time, and
    write them with writeStream to out_dir in CSV with a header, output mode append,
    checkpoint in ckpt_dir, trigger availableNow. Return the started query.
    Add .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") so that the timestamps in the CSV
    files stay readable.
    """
    rides = read_rides()
    # TODO
    return None


# ---------------------------------------------------------------- Task 2: windows and watermark
def task2_fare_per_window(mode):
    """Trips and total fare per 2-minute tumbling window of event_time, with a 2-minute
    watermark. Columns: window_start, window_end, trips, total_fare (2 decimals).

    mode == "append": CSV file sink in out_dir (a window is written once, when the
    watermark passes its end). mode == "update": console sink (every change is printed).
    Checkpoint in ckpt_dir, trigger availableNow. Return the started query. withWatermark
    comes before groupBy and names the column used in window(); same timestampFormat
    option as in task 1.
    """
    rides = read_rides()
    # TODO
    return None


# ---------------------------------------------------------------- main
if task == "task1":
    query = task1_ingest()
else:
    if mode == "update":
        shutil.rmtree(ckpt_dir, ignore_errors=True)   # the console run is a look, not a result
    query = task2_fare_per_window(mode)

if query is None:
    print(f"\n=== {task}: not implemented yet ===")
    spark.stop(); sys.exit(0)

query.awaitTermination()

# What happened, batch by batch: the same numbers the Structured Streaming tab shows.
print(f"\n=== {task}: micro-batch summary ===")
print(f"{'batch':>5} {'input rows':>10} {'watermark':>20} {'state rows':>10}")
for p in query.recentProgress:
    wm = p.get("eventTime", {}).get("watermark", "-")[:19].replace("T", " ")
    wm = "-" if wm.startswith("1970") else wm        # no watermark yet in the first batch
    ops = p.get("stateOperators", [])
    state = ops[0]["numRowsTotal"] if ops else "-"
    print(f"{p['batchId']:>5} {p['numInputRows']:>10} {wm:>20} {state:>10}")

if task == "task2":
    # The same aggregation as a batch job over all the files at once: no watermark, no late
    # rows, every window. Compare it with what the stream produced (report question 2).
    print("\n=== task2: the same windows computed in batch over all files ===")
    (spark.read.schema(schema).json(stream_dir)
     .groupBy(window(col("event_time"), "2 minutes"))
     .agg(count("*").alias("trips"), sround(ssum("fare_amount"), 2).alias("total_fare"))
     .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"),
             "trips", "total_fare")
     .orderBy("window_start").show(truncate=False))
if mode == "append" or task != "task2":
    print(f"Results under {out_dir}. To run {task} again on the same files, delete {ckpt_dir} first.")
spark.stop()
