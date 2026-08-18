#!/usr/bin/env python
"""The Objective B spike (spec §21.3).

It delivers a document, not a "yes, it looks flat". Four things get measured, because each
answers a question the others cannot:

1. **A curve, not a point.** RSS against chunk size *and* concurrency, because the output
   has to be a configuration recommendation rather than a reassurance.
2. **Peak against steady state, separately.** Bad consumption hides in reader start-up,
   not in the loop, and one average over the whole run buries it.
3. **Parquet bytes per row.** ADR-007 is decided in gigabytes, and 13.2 million rows times
   an unknown size is not a plan.
4. **A negative control.** If the configuration cannot be made to fail, the measurement is
   not measuring what it claims to. That one belongs to the WSI rail — loading a whole
   slide with PIL under a 1.5 GB limit — and is skipped until slides are on disk.

Memory is sampled from outside the process being measured. `ru_maxrss` reports only a
high-water mark, so it can neither separate peak from steady state nor ever fall; the
parent polls `ps` instead and keeps the whole series.

    uv run python scripts/spike_memory.py --chunks 256,512,1024 --workers 1,2,4
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from hcs.config import get_settings
from hcs.db import HcsField, UnitRun, UnitStatus
from hcs.imaging.pipeline import ChunkConfig, measure_field
from hcs.imaging.source import load_field, segmentation_channel
from hcs.imaging.writer import serialise_cell_metrics
from hcs.services.units import load_hcs_field
from hcs.storage import s3_client

SAMPLE_INTERVAL = 0.05  # seconds between RSS samples
KIB = 1024

# Samples before this fraction of a run are start-up: importing scipy, building the
# botocore client, filling the first block. Steady state is what the rest looks like.
WARMUP_FRACTION = 0.3

# A chunk is "as fast as the best" if it lands within this of the best measured
# throughput. Set from observed run-to-run spread, which is larger than one might hope.
THROUGHPUT_TOLERANCE = 0.15

# For the ADR-007 projection: the whole dataset at roughly a thousand cells per field.
DATASET_FIELDS = 13_200
CELLS_PER_FIELD = 1_000


@dataclass(frozen=True, slots=True)
class Measurement:
    chunk: int
    workers: int
    peak_mib: float
    steady_mib: float
    total_peak_mib: float
    seconds: float
    fields: int
    cells: int
    parquet_bytes: int

    @property
    def bytes_per_row(self) -> float:
        return self.parquet_bytes / self.cells if self.cells else 0.0

    @property
    def fields_per_second(self) -> float:
        return self.fields / self.seconds if self.seconds else 0.0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.child is not None:
        return child(args)

    units = pick_units(args.fields * max(args.workers))
    if not units:
        print("no HCS fields registered; run `make seed` first", file=sys.stderr)
        return 1
    print(f"{len(units)} field(s) available\n", file=sys.stderr)

    busy = competing_containers()
    if busy:
        print(
            f"WARNING: {busy} unit(s) being processed by the stack; throughput is not comparable",
            file=sys.stderr,
        )

    floor = baseline()
    print(f"baseline (imports only): {floor:.0f} MiB\n", file=sys.stderr)
    results = [
        run_one(chunk, workers, units, args.fields)
        for chunk in args.chunks
        for workers in args.workers
    ]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report(results, args, floor))
    print(f"\nwrote {args.out}", file=sys.stderr)
    return 0


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure RSS against chunk and concurrency.")
    parser.add_argument("--chunks", type=_ints, default=[256, 512, 1024, 4096])
    parser.add_argument("--workers", type=_ints, default=[1, 2, 4])
    parser.add_argument("--fields", type=int, default=6, help="fields processed per worker")
    parser.add_argument("--out", type=Path, default=Path("docs/spike-memory.md"))
    parser.add_argument("--child", type=int, help=argparse.SUPPRESS)  # internal
    return parser.parse_args(argv)


def _ints(value: str) -> list[int]:
    return [int(part) for part in value.split(",") if part]


def pick_units(count: int) -> list[uuid.UUID]:
    """Real registered fields, in a stable order.

    Ordered by id rather than sampled: the spike gets re-run after changes, and comparing
    two runs over different fields would report a difference in cell density as a
    difference in memory.
    """
    engine = create_engine(get_settings().database_url)
    with Session(engine) as session:
        return list(
            session.scalars(
                select(HcsField.work_unit_id).order_by(HcsField.work_unit_id).limit(count)
            )
        )


def run_one(chunk: int, workers: int, units: Sequence[uuid.UUID], per_worker: int) -> Measurement:
    """One point on the curve: `workers` processes, each doing `per_worker` fields."""
    started = time.perf_counter()
    children = [
        _spawn(chunk, [str(u) for u in units[index * per_worker : (index + 1) * per_worker]])
        for index in range(workers)
    ]
    series = _sample_until_done(children)
    elapsed = time.perf_counter() - started

    totals = [json.loads(process.stdout.read()) for process in children if process.stdout]
    for process in children:
        process.wait()

    peaks = [max(values) for values in series.values() if values] or [0.0]
    measurement = Measurement(
        chunk=chunk,
        workers=workers,
        peak_mib=max(peaks),
        steady_mib=_steady(series),
        total_peak_mib=_concurrent_peak(series),
        seconds=elapsed,
        fields=sum(total["fields"] for total in totals),
        cells=sum(total["cells"] for total in totals),
        parquet_bytes=sum(total["parquet_bytes"] for total in totals),
    )
    print(
        f"chunk={chunk:>5} workers={workers}  peak/proc {measurement.peak_mib:6.0f} MiB  "
        f"steady {measurement.steady_mib:6.0f}  all procs {measurement.total_peak_mib:6.0f}  "
        f"{measurement.fields_per_second:4.1f} fields/s",
        file=sys.stderr,
    )
    return measurement


def competing_containers() -> int:
    """Units the stack's own workers are processing right now.

    §21.2 states the experimental condition: everything else stopped. Counting containers
    would be the wrong test — the spike needs Postgres and MinIO up to read anything at
    all, and an idle worker competes for nothing. What competes is a worker segmenting.

    Memory survives a busy machine; an address space is an address space. Throughput does
    not, and a recommendation derived from a contended run picks its chunk out of noise.
    So this warns rather than refuses, and the report says which columns to trust.
    """
    engine = create_engine(get_settings().database_url)
    with Session(engine) as session:
        return (
            session.scalar(
                select(func.count())
                .select_from(UnitRun)
                .where(UnitRun.status == UnitStatus.PROCESSING)
            )
            or 0
        )


def baseline() -> float:
    """RSS of a child that imports the stack and processes nothing.

    Without it the table is unreadable. scipy, scikit-image, pandas and botocore cost
    something the moment they are imported, and that cost is paid once per worker whatever
    the chunk size — so it is the floor every other number sits on, and the difference
    from it is what the chunking actually decides.
    """
    process = _spawn(0, [])
    series = _sample_until_done([process])
    if process.stdout:
        process.stdout.read()
    process.wait()
    return max(series[process.pid], default=0.0)


def _spawn(chunk: int, unit_ids: Sequence[str]) -> subprocess.Popen[str]:
    """Start one worker and hand it its share of the units on stdin."""
    process = subprocess.Popen(
        [sys.executable, __file__, "--child", str(chunk)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    if process.stdin is not None:
        process.stdin.write("\n".join(unit_ids))
        process.stdin.close()
    return process


def _sample_until_done(children: Sequence[subprocess.Popen[str]]) -> dict[int, list[float]]:
    """Poll every child's RSS until they all exit.

    From the parent, because the number that matters is current RSS over time, and a
    process cannot see its own except as a high-water mark that never falls.
    """
    series: dict[int, list[float]] = {process.pid: [] for process in children}
    while any(process.poll() is None for process in children):
        for pid, rss in _rss_of(list(series)).items():
            series[pid].append(rss)
        time.sleep(SAMPLE_INTERVAL)
    return series


def _rss_of(pids: Sequence[int]) -> dict[int, float]:
    """Resident set size in MiB, per pid, via `ps`. Exited pids are simply absent."""
    listed = ",".join(str(pid) for pid in pids)
    output = subprocess.run(
        ["ps", "-o", "pid=,rss=", "-p", listed], capture_output=True, text=True, check=False
    ).stdout
    sizes: dict[int, float] = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 2:
            sizes[int(parts[0])] = int(parts[1]) / KIB
    return sizes


def _steady(series: dict[int, list[float]]) -> float:
    """Median RSS of a process after start-up.

    A median rather than a mean: one allocation spike would drag a mean towards the peak
    and erase the very distinction this measurement exists to draw.
    """
    tails = [values[int(len(values) * WARMUP_FRACTION) :] for values in series.values() if values]
    flat = [value for tail in tails for value in tail]
    return statistics.median(flat) if flat else 0.0


def _concurrent_peak(series: dict[int, list[float]]) -> float:
    """The worst moment across all processes at once — what a container limit sees.

    Summing each process's own peak would overstate it, because those peaks need not
    coincide. Summing per sample index and taking the maximum of that does not.
    """
    length = min((len(values) for values in series.values()), default=0)
    if length == 0:
        return 0.0
    return max(sum(values[index] for values in series.values()) for index in range(length))


def child(args: argparse.Namespace) -> int:
    """One worker: process the fields named on stdin at the given chunk, report totals.

    A fresh process per configuration rather than a thread. The point is to watch one
    isolated address space, and a reused interpreter would carry the previous chunk size's
    allocations into the next measurement.
    """
    unit_ids = [uuid.UUID(line.strip()) for line in sys.stdin if line.strip()]
    settings = get_settings()
    engine = create_engine(settings.database_url)
    s3 = s3_client()
    run_id = uuid.uuid4()

    cells = parquet_bytes = 0
    for unit_id in unit_ids:
        with Session(engine) as session:
            field = load_hcs_field(session, unit_id)
        pixels = load_field(s3, field.channels)
        frame = measure_field(
            segmentation_channel(pixels, settings.segmentation_channel),
            pixels,
            ChunkConfig(chunk=args.child, overlap=settings.chunk_overlap),
        )
        cells += len(frame)
        # Serialised, never uploaded: the size is the measurement, and writing it would
        # fill the results bucket with a benchmark.
        parquet_bytes += len(serialise_cell_metrics(frame, run_id, unit_id))

    print(json.dumps({"fields": len(unit_ids), "cells": cells, "parquet_bytes": parquet_bytes}))
    return 0


def recommend(results: Sequence[Measurement]) -> tuple[int, str]:
    """Turn the table into the configuration recommendation §21.3 asks for.

    The rule: among the chunk sizes whose throughput is within `THROUGHPUT_TOLERANCE` of
    the best measured, take the one with the lowest peak. Memory here is a clean, monotone
    signal and throughput is not — repeated runs move it by more than ten percent — so a
    rule that chased the fastest number would recommend a different chunk each time it ran.

    Compared at the highest concurrency measured, because that is the configuration the
    limits have to survive.
    """
    if not results:
        return 0, "no measurements"

    busiest = max(result.workers for result in results)
    candidates = [result for result in results if result.workers == busiest]
    fastest = max(result.fields_per_second for result in candidates)
    viable = [
        result
        for result in candidates
        if result.fields_per_second >= fastest * (1 - THROUGHPUT_TOLERANCE)
    ]
    best = min(viable, key=lambda result: result.peak_mib)
    return best.chunk, (
        f"a {busiest} processos, dentro de {THROUGHPUT_TOLERANCE:.0%} do melhor débito "
        f"({fastest:.1f} campos/s), o menor pico é {best.peak_mib:.0f} MiB"
    )


def report(results: Sequence[Measurement], args: argparse.Namespace, floor: float) -> str:
    """The document §21.3 asks for."""
    settings = get_settings()
    rows = "\n".join(
        f"| {m.chunk} | {m.workers} | {m.peak_mib:.0f} | {m.peak_mib - floor:.0f} | "
        f"{m.steady_mib:.0f} | {m.total_peak_mib:.0f} | {m.fields_per_second:.1f} | "
        f"{m.bytes_per_row:.0f} |"
        for m in results
    )
    busy = competing_containers()
    contention = (
        f"\n> **O stack processou {busy} unidades durante esta medição.** "
        "As colunas de memória valem na mesma — um espaço de endereçamento é um espaço "
        "de endereçamento — mas o débito e a recomendação que dele sai foram medidos "
        "com a máquina disputada. O "
        "§21.2 manda parar tudo o resto antes de os levar a sério.\n"
        if busy
        else ""
    )
    chunk, why = recommend(results)
    measured = [m.bytes_per_row for m in results if m.cells]
    per_row = statistics.median(measured) if measured else 0.0
    projected = per_row * DATASET_FIELDS * CELLS_PER_FIELD / 1024**3

    return f"""# Spike de memória — Objetivo B (§21.3)

Gerado em {datetime.now(UTC).isoformat(timespec="seconds")} por `scripts/spike_memory.py`.
Carril HCS, `overlap={settings.chunk_overlap}`, canal de segmentação
`{settings.segmentation_channel}`, {args.fields} campos por processo.

Linha de base, só os imports e nenhum trabalho: **{floor:.0f} MiB**.
{contention}

| chunk | procs | pico (MiB) | acima da base | estac. (MiB) | agregado (MiB) | campos/s | B/linha |
|---:|---:|---:|---:|---:|---:|---:|---:|
{rows}

## Como ler isto

**A linha de base é a maior parte do número.** O scipy, o scikit-image, o pandas e o
botocore custam {floor:.0f} MiB no momento em que são importados, e esse custo paga-se uma
vez por worker seja qual for o chunk. A coluna *acima da base* é a única que o chunking
decide; o resto é o preço de entrada.

**Pico por processo** é o que decide o limite de memória de um worker. **Pico agregado** é
a soma no pior instante e é o que um limite de contentor veria se lá corressem vários
processos — não é a soma dos picos individuais, porque os picos não coincidem.

**Estado estacionário** é a mediana depois do arranque. A diferença para o pico é o custo
de inicialização, e é aí que o consumo mau se esconde: é por isso que os dois aparecem
separados em vez de numa média que os fundiria.

## Recomendação

**`CHUNK_SIZE={chunk}`** — {why}.

Não é o chunk mais rápido. A memória acima da base é um sinal limpo e monótono; o débito
não é, e move-se mais de dez por cento entre corridas da mesma configuração. Uma regra que
perseguisse o número mais rápido recomendaria um chunk diferente de cada vez.

O chunk **não** muda a contagem de células. Com o limiar de Otsu calculado uma vez por
imagem (§8.1), a contagem é exatamente invariante — 0,0% de amplitude em campos reais a
256, 512, 1024 e bloco único. Escolher aqui é escolher memória e débito, não resultado.

Continua no `config_hash` (§12), na mesma: identifica a corrida e o custo a que correu.

## Bytes por linha e o ADR-007

Mediana de **{per_row:.0f} bytes por linha** de Parquet. A {DATASET_FIELDS} campos por
~{CELLS_PER_FIELD} células, uma corrida completa dá cerca de **{projected:.1f} GiB**. É o
número em que o ADR-007 se decide, e é a razão de `cell_metrics` não viver em Postgres.

## O controlo negativo

Por fazer. Pertence ao carril WSI — carregar uma lâmina inteira com PIL sob o limite de
1,5 GB e registar o OOMKill — e exige lâminas em disco, que ainda não estão. Sem ele, esta
medição mostra que o consumo é baixo mas não prova que a montagem seria capaz de o detetar
se fosse alto.
"""


if __name__ == "__main__":
    raise SystemExit(main())
