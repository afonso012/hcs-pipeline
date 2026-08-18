#!/usr/bin/env python
"""Kill workers mid-flight and check the invariants hold (spec §13.4).

    #   sem duplicados em (work_unit_id, run_id, label)
    #   processed + failed == total
    #   nenhuma unidade presa em PROCESSING
    #   falhas permanentes com attempts == 1

Everything in §16 — `task_acks_late`, the claim that only accepts PENDING or PROCESSING,
the deterministic unit id, the file-per-unit Parquet write — is a claim about what happens
when a worker dies holding a unit. Until a worker actually dies, it is a claim.

SIGKILL, not SIGTERM. A graceful stop lets Celery finish the task and re-queue politely,
which tests the shutdown path rather than the failure path. The interesting case is the
one where the process stops existing between claiming a unit and marking it.

The recovery this checks is Celery's own: with `task_acks_late`, a task killed before it
acknowledges goes back to the broker and another worker picks it up. What the code has to
guarantee is that picking it up again is *harmless* — that is the idempotency of §16.2, and
the invariant about duplicate labels is where it would show if it were not.

    make chaos
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

TIMEOUT: Final = httpx.Timeout(120.0)
SETTLE_SECONDS: Final = 5
POLL_SECONDS: Final = 5


@dataclass(frozen=True, slots=True)
class Invariant:
    name: str
    detail: str
    held: bool


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    engine = create_engine(get_settings().database_url)

    with httpx.Client(base_url=args.api, timeout=TIMEOUT) as client:
        run_id = submit(client, args.batch_id)
    print(f"run {run_id}", file=sys.stderr)

    time.sleep(args.kill_after)
    killed = kill_workers(args.kill)
    print(f"killed: {', '.join(killed) or '(none)'}", file=sys.stderr)

    restarted = restart(killed)
    wait_for_settle(engine, run_id, args.patience)

    invariants = check(engine, run_id)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report(invariants, run_id, killed, restarted, args))

    for invariant in invariants:
        mark = "held" if invariant.held else "BROKEN"
        print(f"  {mark:<7} {invariant.name}: {invariant.detail}", file=sys.stderr)
    print(f"\nwrote {args.out}", file=sys.stderr)
    return 0 if all(i.held for i in invariants) else 1


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kill workers mid-run and check §16.")
    parser.add_argument("--batch-id", required=True, help="a batch with units already registered")
    parser.add_argument("--kill", type=int, default=1, help="how many workers to SIGKILL")
    parser.add_argument("--kill-after", type=float, default=8.0, help="seconds into the run")
    parser.add_argument("--patience", type=int, default=900, help="seconds to wait for settle")
    parser.add_argument("--api", default="http://localhost:8080")
    parser.add_argument("--out", type=Path, default=Path("docs/chaos.md"))
    return parser.parse_args(argv)


def submit(client: httpx.Client, batch_id: str) -> uuid.UUID:
    response = client.post(f"/v1/batches/{batch_id}/submit")
    response.raise_for_status()
    return uuid.UUID(response.json()["run_id"])


def kill_workers(count: int) -> list[str]:
    """SIGKILL, so the process stops existing rather than winding down.

    A graceful stop exercises Celery's shutdown path, which is not the question. The
    question is what the database looks like when a process holding a claimed unit ceases
    to exist between the claim and the mark.
    """
    names = _worker_containers()[:count]
    for name in names:
        subprocess.run(["docker", "kill", "-s", "SIGKILL", name], capture_output=True, check=False)
    return names


def restart(names: Sequence[str]) -> list[str]:
    """Bring them back, because §16.4's recovery needs somewhere to redeliver to.

    A run whose workers never return does not test recovery, it tests timeouts.
    """
    for name in names:
        subprocess.run(["docker", "start", name], capture_output=True, check=False)
    return list(names)


def _worker_containers() -> list[str]:
    result = subprocess.run(
        ["docker", "ps", "--filter", "name=hcs-worker", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return sorted(line for line in result.stdout.splitlines() if line.strip())


def wait_for_settle(engine: Any, run_id: uuid.UUID, patience: int) -> None:
    """Wait until nothing is in flight, or give up loudly."""
    deadline = time.monotonic() + patience
    while time.monotonic() < deadline:
        with engine.connect() as connection:
            pending = connection.execute(
                text(
                    "SELECT count(*) FROM unit_runs "
                    "WHERE run_id = :run AND status IN ('PENDING','PROCESSING')"
                ),
                {"run": str(run_id)},
            ).scalar_one()
        if pending == 0:
            time.sleep(SETTLE_SECONDS)
            return
        time.sleep(POLL_SECONDS)
    print(f"still {pending} in flight after {patience}s", file=sys.stderr)


def check(engine: Any, run_id: uuid.UUID) -> list[Invariant]:
    """The four invariants of §13.4, each as a query that can only answer one way."""
    with engine.connect() as connection:
        stuck = connection.execute(
            text("SELECT count(*) FROM unit_runs WHERE run_id = :run AND status = 'PROCESSING'"),
            {"run": str(run_id)},
        ).scalar_one()
        totals = connection.execute(
            text(
                "SELECT processed_units, failed_units, total_units, status "
                "FROM runs WHERE id = :run"
            ),
            {"run": str(run_id)},
        ).one()
        retried_permanent = connection.execute(
            text(
                "SELECT count(*) FROM unit_runs "
                "WHERE run_id = :run AND error_class = 'PERMANENT' AND attempts > 1"
            ),
            {"run": str(run_id)},
        ).scalar_one()
        by_status = dict(
            connection.execute(
                text("SELECT status, count(*) FROM unit_runs WHERE run_id = :run GROUP BY 1"),
                {"run": str(run_id)},
            ).all()
        )
        duplicates = connection.execute(
            text(
                "SELECT count(*) FROM ("
                "  SELECT work_unit_id FROM unit_runs WHERE run_id = :run "
                "  GROUP BY work_unit_id HAVING count(*) > 1"
                ") d"
            ),
            {"run": str(run_id)},
        ).scalar_one()

    processed, failed, total, status = totals
    return [
        Invariant(
            "no duplicate rows for (work_unit_id, run_id)",
            f"{duplicates} duplicated unit(s); statuses {by_status}",
            duplicates == 0,
        ),
        Invariant(
            "processed + failed == total",
            f"{processed} + {failed} = {processed + failed} against {total}, run {status}",
            processed + failed == total,
        ),
        Invariant(
            "nothing left stuck in PROCESSING",
            f"{stuck} unit(s) still claimed by a worker that is gone",
            stuck == 0,
        ),
        Invariant(
            "permanent failures were not retried",
            f"{retried_permanent} permanent failure(s) with attempts > 1",
            retried_permanent == 0,
        ),
    ]


def report(
    invariants: Sequence[Invariant],
    run_id: uuid.UUID,
    killed: Sequence[str],
    restarted: Sequence[str],
    args: argparse.Namespace,
) -> str:
    rows = "\n".join(f"| {'✅' if i.held else '❌'} | {i.name} | {i.detail} |" for i in invariants)
    verdict = (
        "**Aguentam.** Matar workers a meio da corrida não deixou unidades presas, não "
        "duplicou linhas, e a contabilidade do run fecha."
        if all(i.held for i in invariants)
        else "**Não aguentam.** Ver a tabela: uma invariante que falha aqui é uma afirmação "
        "do §16 que não se sustenta."
    )
    return f"""# Teste de caos — §13.4

Gerado em {datetime.now(UTC).isoformat(timespec="seconds")} por `scripts/chaos_test.py`.
Run `{run_id}`, {len(killed)} worker(s) mortos com SIGKILL {args.kill_after:.0f}s após o
arranque: {", ".join(killed) or "nenhum"}. Reiniciados: {", ".join(restarted) or "nenhum"}.

| | invariante | observado |
|---|---|---|
{rows}

## Porque é SIGKILL

Uma paragem graciosa deixa o Celery terminar a tarefa e devolvê-la à fila com educação, o
que exercita o caminho de encerramento e não o de falha. O caso que interessa é aquele em
que o processo **deixa de existir** entre reclamar uma unidade e marcá-la.

A recuperação que isto verifica é a do próprio Celery: com `task_acks_late`, uma tarefa
morta antes de confirmar volta ao broker e outro worker apanha-a. O que o código tem de
garantir é que apanhá-la outra vez seja **inofensivo** — é a idempotência do §16.2, e a
invariante das linhas duplicadas é onde se veria se não fosse.

## Veredicto

{verdict}
"""


if __name__ == "__main__":
    raise SystemExit(main())
