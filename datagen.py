"""Hands-on L10: data generator (plain Python, standard library only).

    python3 datagen.py <seed> [output directory]

Writes twelve files of ride events, rides_00.jsonl to rides_11.jsonl, one per minute of a
morning on October 27, 2026. Each file holds the events that *arrived* during that minute,
one JSON object per line. Most events carry an event_time inside that minute; a few are
late: their event_time is one to two minutes earlier (a phone that reconnected), and a few
are very late, eight to ten minutes earlier (a batch upload). The late rows are what make
Task 3 interesting.

The seed makes the data yours and reproducible: the grader regenerates it from the seed you
write in your report. Default output directory: shared-folder/stream (next to this file).

The files get modification times one minute apart, in order, because Spark's file source
reads new files oldest first. Do not copy the files around (copying resets the times):
generate them where Spark will read them.
"""
import json, os, random, sys, time
from datetime import datetime, timedelta

if len(sys.argv) < 2:
    print(__doc__); sys.exit(2)
seed = int(sys.argv[1])
out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "shared-folder", "stream")
os.makedirs(out, exist_ok=True)
rnd = random.Random(seed)

N_FILES, N_DRIVERS = 12, 20
base = datetime(2026, 10, 27, 8, 0, 0)
# a few drivers are much busier than the others, so Task 2 has a clear top
weights = [rnd.choice([1, 1, 2, 2, 3, 5]) for _ in range(N_DRIVERS)]
drivers = [f"D{i:02d}" for i in range(1, N_DRIVERS + 1)]
trip = 0
total = late = very_late = 0
for k in range(N_FILES):
    minute = base + timedelta(minutes=k)
    n = rnd.randint(20, 30)
    rows = []
    for _ in range(n):
        trip += 1
        arrival = minute + timedelta(seconds=rnd.randint(0, 59))
        r = rnd.random()
        if k >= 2 and r < 0.08:            # late by 1 to 2 minutes
            event = arrival - timedelta(seconds=rnd.randint(60, 120)); late += 1
        elif k >= 9 and r < 0.14:          # very late, by 8 to 10 minutes
            event = arrival - timedelta(seconds=rnd.randint(480, 600)); very_late += 1
        else:
            event = arrival - timedelta(seconds=rnd.randint(0, 20))
        rows.append({
            "trip_id": f"T{trip:05d}",
            "driver_id": rnd.choices(drivers, weights=weights, k=1)[0],
            "distance_km": round(rnd.uniform(1, 30), 1),
            "fare_amount": round(rnd.uniform(5, 80), 2),
            "event_time": event.strftime("%Y-%m-%d %H:%M:%S"),
        })
        total += 1
    path = os.path.join(out, f"rides_{k:02d}.jsonl")
    with open(path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
# modification times one minute apart, oldest file first, all in the past
now = time.time()
for k in range(N_FILES):
    t = now - 60 * (N_FILES - k)
    os.utime(os.path.join(out, f"rides_{k:02d}.jsonl"), (t, t))
print(f"seed {seed}: {N_FILES} files, {total} events ({late} late by 1-2 minutes, "
      f"{very_late} late by 8-10 minutes) written to {out}")
