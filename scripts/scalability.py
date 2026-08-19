#!/usr/bin/env python
"""Throughput against worker count (spec §22, objective D-workers).

> Escalabilidade ≥ 3,2x de 1 para 4 workers.

Measured directly rather than inferred from the load test. Locust drives *ingestion* — how
fast the API accepts manifests — and this criterion is about *processing*: how many units
the worker fleet gets through per second. The two are different systems sharing a database,
and reading one off the other is how a number stops meaning what it says.

The same batch, three times, at one, two and four workers. The same batch matters: unit
processing time varies with cell density, so comparing different batches would report the
plates' composition as a scaling property.

**What a sub-linear curve means is a result, not a failure.** §14.3 says so: the knee where
Postgres or I/O becomes the limit is a finding to locate and document. Four workers sharing
one database, one MinIO and one laptop will bend somewhere, and where it bends is worth
knowing before somebody sizes a cluster from a straight line.

    make scale
"""

import argparse
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import httpx
from sqlalchemy import create_engine, text

from hcs.config import get_settings

COMPOSE: Final = ("docker", "compose", "-p", "hcs")
POLL_SECONDS: Final = 3
SETTLE_SECONDS: Final = 20


@dataclass(frozen=True, slots=True)
class Point:
    workers: int
    units: int
    seconds: float

    @property
    def throughput(self) -> float:
        return self.units / self.seconds if self.seconds else 0.0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    engine = create_engine(get_settings().database_url)

    points = []
    for count in args.workers:
        scale(count, args.compose_dir)
        point = run_once(engine, args)
        points.append(point)
        print(
            f"  {count} worker(s): {point.units} unidades em {point.seconds:.1f}s "
            f"= {point.throughput:.2f} u/s",
            file=sys.stderr,
            flush=True,
        )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report(points, args))
    print(f"\nwrote {args.out}", file=sys.stderr)
    return 0


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure throughput against worker count.")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument(
        "--workers", type=lambda v: [int(x) for x in v.split(",")], default=[1, 2, 4]
    )
    parser.add_argument("--api", default="http://localhost:8080")
    parser.add_argument("--compose-dir", type=Path, default=Path.home() / ".cache/hcs-pipeline/src")
    parser.add_argument("--out", type=Path, default=Path("docs/scalability.md"))
    return parser.parse_args(argv)


def scale(count: int, compose_dir: Path) -> None:
    """Resize the worker fleet and let it settle before timing anything.

    The settle matters: a worker that is still importing scipy is a worker that is not
    processing, and counting its start-up against the run would make small fleets look
    better than they are.
    """
    subprocess.run(
        [*COMPOSE, "up", "-d", "--scale", f"worker={count}", "--no-recreate", "worker"],
        cwd=compose_dir,
        capture_output=True,
        check=False,
    )
    time.sleep(SETTLE_SECONDS)


def run_once(engine: Any, args: argparse.Namespace) -> Point:
    with httpx.Client(base_url=args.api, timeout=httpx.Timeout(120.0)) as client:
        response = client.post(f"/v1/batches/{args.batch_id}/submit")
        response.raise_for_status()
        run_id = uuid.UUID(response.json()["run_id"])
        total = int(response.json()["total_units"])

    started = time.perf_counter()
    while True:
        with engine.connect() as connection:
            done = connection.execute(
                text(
                    "SELECT count(*) FROM unit_runs "
                    "WHERE run_id = :run AND status NOT IN ('PENDING','PROCESSING')"
                ),
                {"run": str(run_id)},
            ).scalar_one()
        if done >= total:
            return Point(
                workers=_worker_count(args.compose_dir),
                units=total,
                seconds=time.perf_counter() - started,
            )
        time.sleep(POLL_SECONDS)


def _worker_count(compose_dir: Path) -> int:
    result = subprocess.run(
        [*COMPOSE, "ps", "-q", "worker"],
        cwd=compose_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    return len([line for line in result.stdout.splitlines() if line.strip()])


def report(points: Sequence[Point], args: argparse.Namespace) -> str:
    base = points[0]
    rows = "\n".join(
        f"| {p.workers} | {p.seconds:.1f} | {p.throughput:.2f} | "
        f"{p.throughput / base.throughput:.2f}x | "
        f"{p.throughput / base.throughput / (p.workers / base.workers):.0%} |"
        for p in points
    )
    best = points[-1]
    speedup = best.throughput / base.throughput if base.throughput else 0.0
    target = 3.2
    verdict = (
        f"**Cumpre.** {speedup:.2f}x de {base.workers} para {best.workers} workers, contra "
        f"um alvo de {target}x."
        if speedup >= target
        else f"**Não cumpre.** {speedup:.2f}x de {base.workers} para {best.workers} workers, "
        f"contra um alvo de {target}x."
    )
    return f"""# Escalabilidade — objetivo D-workers (§22)

Gerado em {datetime.now(UTC).isoformat(timespec="seconds")} por `scripts/scalability.py`.
O mesmo lote de {base.units} unidades, uma vez por contagem de workers.

| workers | segundos | unidades/s | speedup | eficiência |
|---:|---:|---:|---:|---:|
{rows}

{verdict}

## Como ler isto

**Eficiência** é o speedup dividido pelo aumento de workers: 100% seria escalar
perfeitamente. O que cai abaixo disso é contenção — Postgres, MinIO, ou a máquina.

**Uma curva sub-linear é um resultado, não uma falha.** O §14.3 diz isso pelo nome: o joelho
onde o Postgres ou o I/O passam a ser o limite encontra-se e documenta-se. Quatro workers a
partilhar uma base de dados, um MinIO e um portátil dobram nalgum sítio, e saber onde vale
mais do que uma reta.

**Medido no processamento, não na ingestão.** O Locust mede a que velocidade a API aceita
manifestos; este critério é sobre a que velocidade a frota os processa. São sistemas
diferentes a partilhar uma base de dados, e ler um pelo outro é como um número deixa de
significar o que diz.
"""


if __name__ == "__main__":
    raise SystemExit(main())
