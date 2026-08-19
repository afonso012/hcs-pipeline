"""Every metric this system emits, defined once (spec §14.1).

One module rather than definitions scattered where they are incremented, for a reason that
only shows up later: a Prometheus metric registered twice under the same name raises at
import time, and a metric registered in two places under *slightly* different label sets
produces two series that look like one. Collecting them here makes both impossible.

The names come from §14.1 verbatim. They are what the panels of §14.2 query, so renaming
one silently empties a panel rather than breaking a build — which is the argument for
writing them down in the spec in the first place.

**Gauges declare a `multiprocess_mode`, and that is not decoration.** Celery's prefork pool
runs tasks in a forked child while the metrics endpoint lives in the parent, so a counter
incremented in the child is invisible to the parent's registry. `prometheus_client` solves
it by having every process write to a shared directory and having the exporter aggregate
the files — but a gauge has no obvious aggregation, so it has to be told which one it
means. Discovered the hard way: 480 units processed and every worker metric read zero.

Some metrics here have no producer yet. `inference_*` waits for Phase 7's Ray Serve, and
`plate_z_prime` for Phase 6's science layer. They are declared anyway so the dashboards
that reference them show an honest empty panel instead of a query error, and so that the
shape of what is missing stays visible.
"""

from typing import Final

from prometheus_client import Counter, Gauge, Histogram

# --- ingestion (§10) -------------------------------------------------------------------

INGESTION_REJECTED: Final = Counter(
    "ingestion_rejected_total",
    "Submissions refused before any work was queued.",
    ["reason"],
)

# --- the queue (§10, §14.1) ------------------------------------------------------------

QUEUE_DEPTH: Final = Gauge(
    "celery_queue_depth",
    "Tasks waiting on the broker, read from Redis at scrape time.",
    ["queue"],
    # One queue, read fresh on each scrape: the newest reading is the only correct one, and
    # summing across API replicas would multiply the depth by the replica count.
    multiprocess_mode="mostrecent",
)

TASK_DURATION: Final = Histogram(
    "celery_task_duration_seconds",
    "Wall time of one unit, from claim to terminal state.",
    ["task", "status"],
    # A field takes under a second; a whole-slide level takes minutes. One bucket set has
    # to span both, so it is logarithmic rather than the client default.
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800),
)

TASK_RETRIES: Final = Counter(
    "celery_task_retries_total",
    "Retries, split by whether the cause was transient or permanent (§16.3).",
    ["task", "error_class"],
)

DLQ: Final = Counter("celery_dlq_total", "Units that exhausted their retries.")

RECONCILED: Final = Counter(
    "celery_reconciled_total",
    "Units re-enqueued by the §16.4 sweep after a worker died holding them.",
)

# --- the worker: the metrics that prove Objective B (§14.2 panels 2 and 3) --------------

WORKER_RSS: Final = Gauge(
    "worker_memory_rss_bytes",
    "Resident set size of this worker process.",
    ["worker_id", "phase"],  # phase: init | steady
    # `livemax` across the pool: the parent and its child are one worker as far as a memory
    # limit is concerned, and the limit is what panel 3 is about. `all` would show two
    # series for one container and `liveall` would keep the dead child's last reading.
    multiprocess_mode="livemax",
)

UNIT_BYTES: Final = Histogram(
    "unit_bytes_processed",
    "Input bytes per work unit, which is the x-axis of the anchor panel.",
    ["kind"],
    # From a 2.5 MB HCS channel to a slide of several gigabytes. Panel 2 is only convincing
    # if both rails land on the same axis, so the range has to cover four orders of
    # magnitude.
    buckets=(
        1e6,
        5e6,
        1e7,
        5e7,
        1e8,
        5e8,
        1e9,
        5e9,
        1e10,
    ),
)

CHUNKS_PROCESSED: Final = Counter("chunks_processed_total", "Blocks segmented, across both rails.")

CELLS_DETECTED: Final = Counter("cells_detected_total", "Objects measured and written.")

PARQUET_BYTES_PER_ROW: Final = Histogram(
    "cell_parquet_bytes_per_row",
    "Serialised size divided by row count — the number ADR-007 rests on.",
    buckets=(50, 75, 100, 125, 150, 200, 300, 500),
)

# --- quality (§8.2, §14.2 panel 6) -----------------------------------------------------

QC_REJECTED: Final = Counter(
    "qc_rejected_total",
    "Units declined before segmentation because there was nothing to segment.",
    ["reason"],
)

# --- declared, not yet produced --------------------------------------------------------

# Phase 7, when inference batching exists. Panel 5 reads these; until then it is honestly
# empty rather than absent, which is the difference between "not built" and "broken".
INFERENCE_BATCH_SIZE: Final = Histogram(
    "inference_batch_size", "Images per inference batch.", buckets=(1, 2, 4, 8, 16, 32, 64)
)
INFERENCE_WAIT: Final = Histogram(
    "inference_wait_time_seconds", "How long a request waited to be batched."
)

# Phase 6, when §9.3 computes it. A Z' that falls over weeks is an assay problem rather
# than a software one, and it is only visible if it was being recorded all along.
PLATE_Z_PRIME: Final = Gauge(
    "plate_z_prime",
    "Z' factor per plate and readout.",
    ["plate", "readout"],
    multiprocess_mode="mostrecent",
)
