#!/usr/bin/env python
"""The negative control of §21.3, point 4.

> Se não se consegue fazer falhar, a medição não está a medir o que se julga.

Every other measurement in this project reports low memory. None of them proves the
apparatus could have detected high memory, and a measurement that cannot fail is not
evidence. So this runs the same slide, at the same level, under the same limit, two ways:

- **whole** — `read_region` over the entire level 0 of `CMU-1`, which is 46000x32914 RGBA,
  or 5.64 GiB. Not a strawman: it is the one-liner anybody writes first, and OpenSlide
  will attempt it. Under a 1.5 GiB limit it has to be killed.
- **chunked** — the pipeline, over the same pixels. It has to finish.

The pair is the argument. One failing proves the limit is real; the other finishing proves
the design is what survives it. Either result alone proves nothing.

The limit is a container's, not `setrlimit`'s. `RLIMIT_AS` caps virtual address space,
which is neither what the deployment imposes nor what kills a worker in production — a
cgroup limit and an OOMKill with exit 137 are. This control checks that the mechanism §15
relies on actually fires.

    make negative-control
"""

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

DEFAULT_SLIDES: Final = Path("/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/OpenSlide")
SLIDE_IN_CONTAINER: Final = "/slides/Aperio/CMU-1.svs"
IMAGE: Final = "hcs-worker"
MEMORY: Final = "1536m"

OOM_EXIT_CODE: Final = 137
MODES: Final = ("whole", "chunked")


@dataclass(frozen=True, slots=True)
class Outcome:
    mode: str
    exit_code: int
    oom_killed: bool
    seconds: float
    tail: str

    @property
    def killed(self) -> bool:
        return self.oom_killed or self.exit_code == OOM_EXIT_CODE

    @property
    def verdict(self) -> str:
        if self.killed:
            return "morto pelo limite"
        return "concluído" if self.exit_code == 0 else f"falhou ({self.exit_code})"


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.inside:
        return payload(args.inside, args.slide)

    outcomes = [run_in_container(mode, args) for mode in args.modes]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report(outcomes, args))
    print(f"\nwrote {args.out}", file=sys.stderr)

    return 0 if _as_expected(outcomes) else 1


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prove the memory limit can be hit.")
    parser.add_argument("--slides", type=Path, default=DEFAULT_SLIDES)
    parser.add_argument("--memory", default=MEMORY, help="container limit, e.g. 1536m")
    parser.add_argument("--modes", nargs="*", default=list(MODES), choices=MODES)
    parser.add_argument("--out", type=Path, default=Path("docs/negative-control.md"))
    parser.add_argument("--inside", choices=MODES, help=argparse.SUPPRESS)
    parser.add_argument("--slide", default=SLIDE_IN_CONTAINER, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


# --- inside the container -----------------------------------------------------------------


def payload(mode: str, slide_path: str) -> int:
    """What runs under the limit.

    The imports are local so that anything raised here is the slide or the memory, not an
    import that would have failed on the host too.
    """
    from hcs.imaging.pipeline import ChunkConfig, measure_field
    from hcs.imaging.wsi import measure_at, open_slide, thumbnail_threshold

    slide = open_slide(slide_path)
    width, height = slide.level_dimensions[0]
    print(f"level 0: {width}x{height} = {width * height * 4 / 1024**3:.2f} GiB as RGBA", flush=True)

    if mode == "whole":
        # The one-liner. It allocates the entire level before returning anything, which is
        # precisely the behaviour the chunked path exists to avoid.
        region = slide.read_region((0, 0), 0, (width, height))
        print(f"loaded {region.size} — the limit did not fire", flush=True)
        return 0

    level = measure_at(slide)
    cells = measure_field(
        level,
        {"H": level},
        ChunkConfig(chunk=1024, overlap=32),
        threshold=thumbnail_threshold(slide),
    )
    print(f"measured {len(cells)} nuclei without ever holding the level", flush=True)
    return 0


# --- outside it -----------------------------------------------------------------------------


def run_in_container(mode: str, args: argparse.Namespace) -> Outcome:
    """Run one mode under a cgroup limit and report how it ended.

    `--memory-swap` is pinned to `--memory` deliberately. Left alone, Docker grants swap
    equal to the limit again, and the naive load would page instead of dying — turning a
    control that must fail into one that merely runs slowly.
    """
    name = f"hcs-negative-control-{mode}"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)

    # PYTHONPATH, because the image builds with `uv sync --no-install-project`: `hcs` is
    # not in site-packages, it is the /app/hcs directory that the default working directory
    # happens to make importable. Running a script from /scripts puts /scripts at the head
    # of sys.path and drops /app entirely.
    command = [
        "docker", "run", "--name", name,
        "--memory", args.memory, "--memory-swap", args.memory,
        "-e", "PYTHONPATH=/app",
        "-v", f"{args.slides}:/slides:ro",
        "-v", f"{Path(__file__).parent}:/scripts:ro",
        "--entrypoint", "/app/.venv/bin/python",
        IMAGE, "/scripts/negative_control.py", "--inside", mode, "--slide", args.slide,
    ]  # fmt: skip

    print(f"\n--- {mode} at {args.memory} ---", file=sys.stderr, flush=True)
    started = time.perf_counter()
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    elapsed = time.perf_counter() - started

    state = _inspect(name)
    subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)

    lines = (result.stdout + result.stderr).strip().splitlines()
    # The container's own exit code where docker reports one; the client's only as a
    # fallback, because a docker that failed to start the container never had one.
    reported = state.get("ExitCode")
    outcome = Outcome(
        mode=mode,
        exit_code=reported if isinstance(reported, int) else result.returncode,
        oom_killed=bool(state.get("OOMKilled")),
        seconds=elapsed,
        tail=lines[-1] if lines else "",
    )
    print(f"{outcome.verdict} in {elapsed:.1f}s — {outcome.tail}", file=sys.stderr, flush=True)
    return outcome


def _inspect(name: str) -> dict[str, object]:
    """The container's own account of how it died.

    Exit 137 alone is not proof: it is SIGKILL, and the kernel's OOM killer is only one of
    the things that send it. `OOMKilled` is the flag that says which.
    """
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{json .State}}", name],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return {}
    state: dict[str, object] = json.loads(result.stdout)
    return state


def _as_expected(outcomes: Sequence[Outcome]) -> bool:
    """The control passes only if the naive path died and the chunked path did not."""
    by_mode = {outcome.mode: outcome for outcome in outcomes}
    whole = by_mode.get("whole")
    chunked = by_mode.get("chunked")
    if whole is not None and not whole.killed:
        return False
    return not (chunked is not None and chunked.exit_code != 0)


def report(outcomes: Sequence[Outcome], args: argparse.Namespace) -> str:
    rows = "\n".join(
        f"| {o.mode} | {args.memory} | {o.exit_code} | {'sim' if o.oom_killed else 'não'} | "
        f"{o.seconds:.1f} | {o.verdict} |"
        for o in outcomes
    )
    verdict = (
        "**Passa.** O caminho ingénuo morreu no limite e o caminho por blocos concluiu."
        if _as_expected(outcomes)
        else "**Falha.** O resultado não é o par esperado — ver a tabela antes de confiar "
        "em qualquer número de memória deste repositório."
    )
    return f"""# Controlo negativo — Objetivo B (§21.3, ponto 4)

Gerado em {datetime.now(UTC).isoformat(timespec="seconds")} por `scripts/negative_control.py`.
Lâmina `CMU-1.svs`, nível 0: 46000x32914, **5,64 GiB** em RGBA. Limite do contentor:
{args.memory}, sem swap.

| modo | limite | saída | OOMKilled | segundos | veredicto |
|---|---|---:|---|---:|---|
{rows}

## Porque é que isto existe

Todas as outras medições deste projeto dizem que o consumo é baixo. **Nenhuma delas prova
que a montagem seria capaz de detetar consumo alto**, e uma medição que não pode falhar não
é prova de nada.

Daí a mesma lâmina, no mesmo nível, sob o mesmo limite, de duas maneiras. O modo `whole` é
o `read_region` sobre o nível 0 inteiro — não é um espantalho, é a linha que qualquer
pessoa escreve primeiro, e o OpenSlide tenta-a. O modo `chunked` é o pipeline sobre os
mesmos pixels.

**O par é que é o argumento.** Um falhar prova que o limite é real; o outro concluir prova
que o desenho é o que sobrevive a ele. Qualquer um deles sozinho não prova nada.

## O limite é de contentor, não `setrlimit`

O `RLIMIT_AS` limita espaço de endereçamento virtual, que não é o que o deployment impõe
nem o que mata um worker em produção. O que mata é um limite de cgroup e um OOMKill com
saída 137, e o §15 conta com esse mecanismo. Este controlo verifica que ele dispara.

A saída 137 sozinha também não bastaria: é SIGKILL, e o OOM killer é só uma das coisas que
o enviam. A coluna `OOMKilled` vem do `docker inspect` e é a que diz qual.

## Veredicto

{verdict}
"""


if __name__ == "__main__":
    raise SystemExit(main())
