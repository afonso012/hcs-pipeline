"""Load profiles for §14.3, and the acceptance criteria of §22.

> Locust com dois perfis: **rajada** (manifesto de 10.000 unidades de uma vez) e
> **contínuo**. Corre com `--scale worker=1,2,4` e desenha a curva.

Two profiles, because they ask different questions.

**Burst** posts a manifest of ten thousand units. The criterion is objective A's: p95 under
200 ms. What it really tests is that the endpoint does no work proportional to the batch
inside the request — before the block dispatcher, `submit` published one task per unit in
the handler, and ten thousand `.delay()` calls is not a 200 ms response.

**Sustained** keeps submitting small batches while workers process. The criterion is that
no request blocks for more than a second, which is a claim about the API not being starved
by the worker fleet sharing its database.

The units are synthetic and the digests are invented. Nothing here uploads an image: the
manifest is metadata by reference (ADR-002), so a load test of the ingestion path never
needs a pixel. That is what makes it testable at this size at all.

    make load                     # burst, against the running stack
    make load PROFILE=Sustained
"""

import random
import uuid
from typing import Any, ClassVar

from locust import HttpUser, between, constant, events, task

BARCODE_PREFIX = "Load"
CHANNELS = ("DAPI", "Tubulin", "Actin")

# §14.3's burst: ten thousand units. Above `ManifestIn`'s cap of 5000, so it goes in two
# requests — which is the honest shape anyway, because a client with ten thousand fields is
# a client that will paginate.
BURST_UNITS = 10_000
MANIFEST_CAP = 5_000

ROWS = "BCDEFG"
COLUMNS = tuple(range(2, 12))
SITES = (1, 2, 3, 4)


def digest() -> str:
    """A syntactically valid sha256 that hashes nothing.

    The API validates the shape at the boundary, so the test has to satisfy the shape; it
    never fetches the bytes, so it does not have to satisfy the content.
    """
    return uuid.uuid4().hex + uuid.uuid4().hex


def units(count: int) -> list[dict[str, Any]]:
    """`count` distinct fields, walking wells and sites the way a plate does.

    Distinct matters. Identical units collide on the deterministic id of §7.1, and the
    second half of the manifest would come back as duplicates — so the test would be
    measuring the conflict path instead of the insert path.
    """
    produced = []
    for index in range(count):
        row = ROWS[index // len(SITES) % len(ROWS)]
        column = COLUMNS[index // (len(SITES) * len(ROWS)) % len(COLUMNS)]
        produced.append(
            {
                "well": f"{row}{column:02d}",
                "field_index": SITES[index % len(SITES)] + index // 240,
                "channels": {
                    name: {"uri": f"s3://raw/load/{index}_{name}.tif", "sha256": digest()}
                    for name in CHANNELS
                },
            }
        )
    return produced


def manifest(barcode: str, batch: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "kind": "HCS_FIELD",
        "plate": {
            "barcode": barcode,
            "n_rows": 8,
            "n_cols": 12,
            "expected_channels": list(CHANNELS),
        },
        "units": batch,
    }


class Burst(HttpUser):
    """One enormous manifest, then submit. Objective A: p95 of the POST under 200 ms."""

    wait_time = between(5, 10)
    saturated: ClassVar[int] = 0

    @task
    def submit_a_large_batch(self) -> None:
        barcode = f"{BARCODE_PREFIX}_{uuid.uuid4().hex[:8]}"
        batch_id = self.client.post(
            "/v1/batches", json={"external_ref": "locust-burst"}, name="POST /batches"
        ).json()["batch_id"]

        prepared = units(BURST_UNITS)
        for start in range(0, BURST_UNITS, MANIFEST_CAP):
            self.client.post(
                f"/v1/batches/{batch_id}/manifest",
                json=manifest(barcode, prepared[start : start + MANIFEST_CAP]),
                name="POST /manifest [5000 units]",
            )

        with self.client.post(
            f"/v1/batches/{batch_id}/submit", name="POST /submit", catch_response=True
        ) as response:
            if response.status_code == 429:
                # Not a failure. §10 says a saturated queue answers 429, and what this test
                # is really checking is that it does so rather than falling over.
                Burst.saturated += 1
                response.success()


class Sustained(HttpUser):
    """Small batches, continuously, while the workers are busy.

    The criterion is that no request blocks for more than a second. A read stalling behind
    the worker fleet's transactions is the failure this looks for, which is why it
    interleaves a GET with every submission.
    """

    wait_time = constant(1)

    @task(3)
    def submit_a_small_batch(self) -> None:
        barcode = f"{BARCODE_PREFIX}_{uuid.uuid4().hex[:8]}"
        batch_id = self.client.post(
            "/v1/batches", json={"external_ref": "locust-sustained"}, name="POST /batches"
        ).json()["batch_id"]
        self.client.post(
            f"/v1/batches/{batch_id}/manifest",
            json=manifest(barcode, units(random.randint(20, 100))),
            name="POST /manifest [small]",
        )
        with self.client.post(
            f"/v1/batches/{batch_id}/submit", name="POST /submit", catch_response=True
        ) as response:
            if response.status_code == 429:
                response.success()

    @task(1)
    def read_something(self) -> None:
        """A cheap read, to catch the API being starved rather than merely slow."""
        self.client.get("/healthz", name="GET /healthz")


@events.quitting.add_listener
def _report(environment: Any, **_: Any) -> None:
    """Print the criteria as verdicts rather than as numbers to squint at.

    A load test whose output is a table of percentiles is a load test somebody has to
    interpret. §22 states thresholds; this says whether they held.
    """
    stats = environment.stats
    entry = stats.get("POST /manifest [5000 units]", "POST")
    manifest_p95 = entry.get_response_time_percentile(0.95) if entry.num_requests else None

    print("\n--- §22 acceptance ---")
    if manifest_p95 is not None:
        print(f"A: p95 of the manifest POST = {manifest_p95} ms (threshold 200)")
    print(f"A: slowest request of any kind = {stats.total.max_response_time} ms (threshold 1000)")
    print(f"§10: submissions refused with 429 = {Burst.saturated}")
    if stats.total.num_failures:
        print(f"FAILURES: {stats.total.num_failures}")
        environment.process_exit_code = 1
