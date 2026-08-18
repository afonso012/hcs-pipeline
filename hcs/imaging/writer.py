"""Writing one unit's cells to Parquet (spec §6.3, §16.2).

13 200 fields at roughly a thousand cells each is 13.2 million rows per run, with a wide
row. That does not belong in Postgres on this machine, and `pycytominer` wants DataFrames
anyway — so cell metrics go to object storage, one file per `(run_id, work_unit_id)`, and
Postgres keeps only the aggregates of §6.4.

**Idempotency is by file.** A reprocessed unit writes to a temporary key and then replaces
the final one in a single operation. The alternative — deleting rows and re-inserting them
— needs a transaction spanning Postgres and S3, which does not exist. Here a second
attempt that finds fewer cells leaves no orphans from the first, because the file is
replaced whole rather than edited.
"""

import uuid
from io import BytesIO
from typing import Any, Final

import numpy as np
import pandas as pd

from hcs.imaging.measure import FEATURE_PREFIX, MORPHOLOGY_COLUMNS
from hcs.metrics import PARQUET_BYTES_PER_ROW

PREFIX: Final = "cell_metrics"

# float32 throughout (§6.3). A centroid is a coordinate on an image at most tens of
# thousands of pixels wide and an area is a pixel count; float64 would double the file for
# digits that describe nothing. Identifiers stay strings so a reader needs no schema.
_FLOAT_COLUMNS: Final = ("centroid_y", "centroid_x", *MORPHOLOGY_COLUMNS)

_IDENTIFIERS: Final = ("work_unit_id", "run_id", "label")


def object_key(run_id: uuid.UUID, work_unit_id: uuid.UUID) -> str:
    """The one path this unit's cells ever occupy.

    Hive-partitioned by run so reading a whole run is a directory scan, and named by unit
    so a rerun overwrites its own file and nobody else's.
    """
    return f"{PREFIX}/run_id={run_id}/work_unit_id={work_unit_id}.parquet"


def write_cell_metrics(
    s3: Any, bucket: str, cells: pd.DataFrame, run_id: uuid.UUID, work_unit_id: uuid.UUID
) -> str:
    """Write one unit's cells and return the key they landed on.

    A unit that found no cells still writes a file. An empty file and a missing file mean
    different things — "processed, and the field was blank" against "never processed" —
    and the reconciliation sweep of §16.4 has to be able to tell them apart.
    """
    key = object_key(run_id, work_unit_id)
    body = serialise_cell_metrics(cells, run_id, work_unit_id)
    if len(cells):
        # §14.1, and the number ADR-007 turns on. Measured here rather than estimated
        # because it is the only place both the byte count and the row count are in scope.
        PARQUET_BYTES_PER_ROW.observe(len(body) / len(cells))

    # Write beside the destination, then replace it. An interrupted upload leaves a stray
    # temporary object rather than a truncated file that reads as a short unit.
    staging = f"{key}.{uuid.uuid4().hex}.tmp"
    s3.put_object(Bucket=bucket, Key=staging, Body=body)
    s3.copy_object(Bucket=bucket, Key=key, CopySource={"Bucket": bucket, "Key": staging})
    s3.delete_object(Bucket=bucket, Key=staging)
    return key


def serialise_cell_metrics(
    cells: pd.DataFrame, run_id: uuid.UUID, work_unit_id: uuid.UUID
) -> bytes:
    """The bytes `write_cell_metrics` would upload, without uploading them.

    Public because §21.3 has to measure Parquet bytes per row — the number ADR-007 rests
    on, since 13.2 million rows times an unknown size is not a plan — and measuring it
    through the writer would fill the results bucket with a benchmark.
    """
    return _serialise(_conform(cells, run_id, work_unit_id))


def _conform(cells: pd.DataFrame, run_id: uuid.UUID, work_unit_id: uuid.UUID) -> pd.DataFrame:
    """Put the frame into the §6.3 schema: identifiers first, then the fixed columns.

    The order is fixed rather than whatever `measure_field` happened to produce, so two
    runs of the same code write comparable files and a golden-batch diff points at a
    number instead of at a permutation.
    """
    features = sorted(name for name in cells.columns if name.startswith(FEATURE_PREFIX))
    typed = cells.astype({name: "float32" for name in (*_FLOAT_COLUMNS, *features)})

    return typed.assign(
        work_unit_id=str(work_unit_id),
        run_id=str(run_id),
        label=cells["label"].astype(np.int32),
        touches_border=cells["touches_border"].astype(bool),
    )[[*_IDENTIFIERS, *_FLOAT_COLUMNS, "touches_border", *features]]


def _serialise(frame: pd.DataFrame) -> bytes:
    """Parquet bytes, in memory.

    A field's cells are a few thousand rows of numbers, not something worth a temporary
    file on a worker whose memory is the thing being measured.
    """
    buffer = BytesIO()
    frame.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
    return buffer.getvalue()
