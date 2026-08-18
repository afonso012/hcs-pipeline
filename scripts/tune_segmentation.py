#!/usr/bin/env python
"""Tune the classic segmenter against BBBC039's validation split (spec §13.5).

**The rule this script exists to make checkable: tuning happens on validation, once.**

The test split measured F1@0.50 = 0.845 with 10% over-segmentation. The temptation is to
adjust a constant, re-measure on test, and repeat until the number looks good — which
produces a figure that describes this dataset's noise rather than the method. BBBC039
publishes a validation split precisely so that the choosing and the reporting happen on
different images.

So: sweep here, pick the best on validation, and measure test exactly once afterwards with
`make accuracy`. Anything else is choosing a parameter by looking at the answer, which
§13.2 forbids by name for the golden batch and which is no more defensible here.

The sweep is deliberately small. `min_distance` seeds the watershed and is the suspect —
the golden batch's mutation test showed it was the most sensitive constant, and seeding
twice inside one nucleus is exactly what splits it in two. `sigma` is included because
smoothing merges close peaks, so the two interact.

    make tune
"""

import argparse
import itertools
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import numpy as np
import tifffile

from hcs.imaging.pipeline import global_threshold
from hcs.imaging.qc import check_field
from hcs.imaging.segment import SegmentationConfig, segment_chunk
from hcs.science.accuracy import aggregate, mean_f1, score_image
from scripts.accuracy_bbbc039 import DEFAULT_ROOT, decode_mask, read_split

# Upward mostly, because the measured failure is over-segmentation: too many seeds. The
# lower values are kept so the sweep can say the current setting was not simply too high.
MIN_DISTANCE: Final = (6, 8, 10, 12, 15, 20)
SIGMA: Final = (1.0, 1.5, 2.0)


@dataclass(frozen=True, slots=True)
class Trial:
    min_distance: int
    sigma: float
    mean_f1: float
    f1_at_50: float
    predicted: int
    annotated: int

    @property
    def ratio(self) -> float:
        return self.predicted / self.annotated if self.annotated else 0.0


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    names = read_split(args.root, "validation")
    if not names:
        print(f"no validation split under {args.root}", file=sys.stderr)
        return 1

    pairs = load(args.root, names)
    print(f"{len(pairs)} validation field(s)", file=sys.stderr)

    trials = []
    for distance, sigma in itertools.product(args.min_distance, args.sigma):
        trial = evaluate(pairs, SegmentationConfig(sigma=sigma, min_distance=distance))
        trials.append(trial)
        print(
            f"  min_distance={distance:>3} sigma={sigma:<4} "
            f"meanF1={trial.mean_f1:.3f} F1@50={trial.f1_at_50:.3f} "
            f"previstos/anotados={trial.ratio:.1%}",
            file=sys.stderr,
            flush=True,
        )

    best = max(trials, key=lambda t: t.mean_f1)
    current = next(
        (t for t in trials if t.min_distance == 10 and t.sigma == 1.0),
        None,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report(trials, best, current, len(pairs)))

    print(f"\nbest on validation: min_distance={best.min_distance} sigma={best.sigma}")
    print(f"wrote {args.out}", file=sys.stderr)
    return 0


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep the segmenter on validation.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument(
        "--min-distance", type=lambda v: [int(x) for x in v.split(",")], default=list(MIN_DISTANCE)
    )
    parser.add_argument(
        "--sigma", type=lambda v: [float(x) for x in v.split(",")], default=list(SIGMA)
    )
    parser.add_argument("--out", type=Path, default=Path("docs/tuning-bbbc039.md"))
    return parser.parse_args(argv)


def load(root: Path, names: Sequence[str]) -> list[tuple[np.ndarray, np.ndarray]]:
    """Read every pair once, because the sweep visits them repeatedly."""
    pairs = []
    for name in names:
        image = root / f"{name}.tif"
        mask = root / "masks" / f"{name}.png"
        if image.exists() and mask.exists():
            pairs.append((tifffile.imread(image), decode_mask(mask)))
    return pairs


def evaluate(pairs: Sequence[tuple[np.ndarray, np.ndarray]], cfg: SegmentationConfig) -> Trial:
    scores = []
    predicted = annotated = 0
    for image, truth in pairs:
        level = global_threshold(image, cfg)
        labels = (
            segment_chunk(image, cfg, level)
            if check_field(image, level).accepted
            else np.zeros(image.shape, dtype=np.int32)
        )
        predicted += int(len(np.unique(labels)) - 1)
        annotated += int(len(np.unique(truth)) - 1)
        scores.append(score_image(labels, truth))

    pooled = aggregate(scores)
    return Trial(
        min_distance=cfg.min_distance,
        sigma=cfg.sigma,
        mean_f1=mean_f1(pooled),
        f1_at_50=pooled[0.5].f1 if 0.5 in pooled else 0.0,
        predicted=predicted,
        annotated=annotated,
    )


def report(trials: Sequence[Trial], best: Trial, current: Trial | None, fields: int) -> str:
    rows = "\n".join(
        f"| {t.min_distance} | {t.sigma} | {t.mean_f1:.3f} | {t.f1_at_50:.3f} | {t.ratio:.1%} |"
        + ("  ← escolhido" if t is best else "")
        for t in sorted(trials, key=lambda t: -t.mean_f1)
    )
    delta = (
        f"{best.mean_f1 - current.mean_f1:+.3f} de F1 médio contra a configuração atual "
        f"(min_distance=10, sigma=1.0)"
        if current
        else "sem configuração atual na varredura"
    )
    return f"""# Afinação da segmentação — split de validação (§13.5)

Gerado em {datetime.now(UTC).isoformat(timespec="seconds")} por `scripts/tune_segmentation.py`.
{fields} campos do split **validation**, {len(trials)} configurações.

**Escolhido: `min_distance={best.min_distance}`, `sigma={best.sigma}`** — {delta}.

| min_distance | sigma | F1 médio 0,50:0,95 | F1@0,50 | previstos/anotados |
|---:|---:|---:|---:|---:|
{rows}

## A regra, e porque está escrita aqui

A escolha acima é feita sobre **validation**. O número que se cita é medido sobre **test**,
uma vez, depois desta escolha estar fechada. O BBBC039 publica os dois splits exatamente
para isto.

A alternativa — ajustar, medir no test, repetir — produz um número que descreve o ruído
deste conjunto e não o método. É a mesma proibição que o §13.2 escreve para o lote dourado,
e não é mais defensável aqui do que lá.

**Uma passagem.** Esta varredura corre uma vez e o resultado aceita-se. Iterar até o número
ficar bonito é afinar a olhar para a resposta com passos extra.

## O que muda a jusante

`min_distance` desloca as contagens de células, e as contagens de células são o que o lote
dourado do §13.2 guarda a 2%. Mudar esta constante obriga a **recongelar a referência no
mesmo commit** — é para isso que a regra do §13.2 existe, e o diff da referência passa a ser
a documentação da mudança.
"""


if __name__ == "__main__":
    raise SystemExit(main())
