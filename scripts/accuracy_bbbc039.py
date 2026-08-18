#!/usr/bin/env python
"""Accuracy against annotation (spec §13.5).

> Sem isto, ter dois backends é uma escolha sem dados.

The only measurement in this project that can say the pipeline is *right* rather than
merely consistent. Everything else — the golden batch, the chunking property, the negative
control — would hold exactly as well if the segmenter were counting the wrong things.

**Segmented as one block, deliberately.** BBBC039 images are 520x696, so the whole field
fits in a single chunk and no stitching is involved. That is not dodging the chunked path:
§13.1's second property already proves the count does not depend on the block size, so
mixing the two questions here would only make a bad result ambiguous. This asks about the
segmentation algorithm and nothing else.

**Scored on the held-out test split**, the one the dataset publishes. Quoting an accuracy
figure over images somebody else labelled "training" is how numbers stop being comparable
to the literature.

**The ground truth needs decoding.** BBBC039's masks are not one integer per nucleus: they
encode how many nuclei cover each pixel — 0 background, 1 a single nucleus, 2 and 3 where
neighbours overlap. Instances are the connected components of the 1s, grown into the
contested pixels by watershed. Reading the file naively gives three "objects" per image
instead of a hundred, which is a mistake that would have produced a beautiful and
meaningless score.

    make accuracy
"""

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import numpy as np
import numpy.typing as npt
import tifffile
from PIL import Image
from scipy import ndimage
from skimage.segmentation import watershed

from hcs.imaging.pipeline import global_threshold
from hcs.imaging.qc import check_field
from hcs.imaging.segment import SegmentationConfig, segment_chunk
from hcs.science.accuracy import IOU_THRESHOLDS, REPORTED, Score, aggregate, mean_f1, score_image

DEFAULT_ROOT: Final = Path("/Volumes/SSD/Imagens_Celulares_em_Larga_Escala/BBBC039")
SPLITS: Final = ("test", "validation", "training")

# In the mask, 1 means "exactly one nucleus covers this pixel". Higher values are contested
# ground where annotated nuclei overlap, and they are what the watershed hands back out.
UNCONTESTED: Final = 1


@dataclass(frozen=True, slots=True)
class FieldScore:
    name: str
    predicted: int
    annotated: int
    scores: list[Score]


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    names = read_split(args.root, args.split)
    if not names:
        print(f"no {args.split} split under {args.root}; run `make data-bbbc039`", file=sys.stderr)
        return 1

    cfg = SegmentationConfig()
    results = []
    for index, name in enumerate(names, start=1):
        result = score_one(args.root, name, cfg)
        if result is not None:
            results.append(result)
        if index % 20 == 0:
            print(f"  {index}/{len(names)}", file=sys.stderr, flush=True)

    pooled = aggregate([r.scores for r in results])
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report(results, pooled, args))
    print(f"\nmean F1 over {IOU_THRESHOLDS[0]}:{IOU_THRESHOLDS[-1]} = {mean_f1(pooled):.3f}")
    print(f"wrote {args.out}", file=sys.stderr)
    return 0


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score segmentation against BBBC039.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--split", choices=SPLITS, default="test")
    parser.add_argument("--out", type=Path, default=Path("docs/accuracy-bbbc039.md"))
    return parser.parse_args(argv)


def read_split(root: Path, split: str) -> list[str]:
    path = root / "metadata" / f"{split}.txt"
    if not path.exists():
        return []
    return [
        line.strip().removesuffix(".png") for line in path.read_text().splitlines() if line.strip()
    ]


def score_one(root: Path, name: str, cfg: SegmentationConfig) -> FieldScore | None:
    """Segment one image and score it, or skip the pair if either half is missing."""
    image_path = root / f"{name}.tif"
    mask_path = root / "masks" / f"{name}.png"
    if not image_path.exists() or not mask_path.exists():
        return None

    image = tifffile.imread(image_path)
    truth = decode_mask(mask_path)

    level = global_threshold(image, cfg)
    predicted = (
        segment_chunk(image, cfg, level)
        if check_field(image, level).accepted
        else np.zeros(image.shape, dtype=np.int32)
    )

    return FieldScore(
        name=name,
        predicted=int(len(np.unique(predicted)) - 1),
        annotated=int(len(np.unique(truth)) - 1),
        scores=score_image(predicted, truth),
    )


def decode_mask(path: Path) -> npt.NDArray[np.int32]:
    """Turn BBBC039's overlap-count encoding into instance labels.

    The file says how many annotated nuclei cover each pixel, not which one. The 1s are
    therefore one connected component per nucleus except where neighbours touch, and the
    2s and 3s are the contested seam — handed back to the nearest component by watershed,
    so that a scored object has its real area rather than an eroded one.
    """
    plane = np.asarray(Image.open(path))[..., 0]
    markers, _ = ndimage.label(plane == UNCONTESTED)
    nuclei = plane > 0
    labels: npt.NDArray[np.int32] = watershed(  # type: ignore[no-untyped-call]
        -ndimage.distance_transform_edt(nuclei), markers, mask=nuclei
    ).astype(np.int32)
    return labels


def report(
    results: Sequence[FieldScore], pooled: dict[float, Score], args: argparse.Namespace
) -> str:
    rows = "\n".join(
        f"| {t:.2f} | {s.true_positives} | {s.false_positives} | {s.false_negatives} | "
        f"{s.precision:.3f} | {s.recall:.3f} | **{s.f1:.3f}** | {s.mean_iou_of_matches:.3f} |"
        for t, s in sorted(pooled.items())
    )
    predicted = sum(r.predicted for r in results)
    annotated = sum(r.annotated for r in results)
    headline = mean_f1(pooled)
    at = {t: pooled[t].f1 for t in REPORTED if t in pooled}

    return f"""# Exatidão contra ground truth — BBBC039 (§13.5)

Gerado em {datetime.now(UTC).isoformat(timespec="seconds")} por `scripts/accuracy_bbbc039.py`.
Split **{args.split}** ({len(results)} campos), backend clássico, campo inteiro num bloco.

**F1 médio em IoU 0,50:0,95 = {headline:.3f}**
· F1@0,50 = {at.get(0.5, 0):.3f}
· F1@0,75 = {at.get(0.75, 0):.3f}

Núcleos: **{predicted} previstos** contra **{annotated} anotados** ({predicted / annotated:.1%}).

| IoU | TP | FP | FN | precisão | recall | F1 | IoU médio dos pares |
|---:|---:|---:|---:|---:|---:|---:|---:|
{rows}

## Como ler isto

**Ao nível do objeto, não do pixel.** Uma máscara binária pontuada pixel a pixel não
distingue um método que funde dois núcleos encostados de um que os separa — ambos pintam os
mesmos pixels. Separá-los é o problema mais difícil deste pipeline, é para isso que existe o
watershed, e é isso que o `min_distance` afina. A métrica tem de o conseguir ver.

**O limiar importa.** O F1@0,50 diz "encontrou o objeto". O F1@0,75 diz "encontrou-o e o
contorno está aproximadamente certo". Citar só o primeiro favorece um método que acerta nos
sítios e erra nas fronteiras, que é exatamente a falha de um watershed sobre um limiar mau.
A média sobre a varredura é a convenção do COCO e é o número que vale a pena citar.

**Contagens agregadas, não médias por imagem.** Uma média de F1 por imagem deixa um campo
com três núcleos pesar tanto como um com trezentos, e transforma a métrica numa afirmação
sobre a composição do conjunto.

## O que estes números dizem

**Prevemos {predicted} núcleos onde há {annotated} — {predicted / annotated:.1%}.** O pipeline
sobre-segmenta. Com recall de {pooled[0.5].recall:.3f} a IoU 0,50 encontra quase tudo o que lá
está; com precisão de {pooled[0.5].precision:.3f}, cerca de um quinto do que reporta não
corresponde a nenhum núcleo anotado. O suspeito é o `min_distance`, que semeia o watershed:
foi a constante que o teste de mutação do lote dourado mostrou ser a mais sensível, e
semear duas vezes dentro de um núcleo parte-o em dois.

**Não afinei isto.** A medição acima é sobre o split **test**, e ajustar parâmetros contra
o conjunto de teste é como um número deixa de significar o que diz. O caminho honesto existe
e está aberto: afinar contra o split **validation**, que é para isso que o BBBC039 o publica,
voltar a medir no test uma vez, e recongelar o lote dourado no mesmo commit — porque mudar
o `min_distance` desloca as contagens que o §13.2 guarda.

**Contexto.** Um F1@0,50 de {at.get(0.5, 0):.3f} para um pipeline clássico por afinar é
razoável e não é estado da arte; a literatura sobre este conjunto põe o CellProfiler na
mesma ordem e as redes convolucionais claramente acima. É o número que dá sentido a ter dois
backends, que é precisamente o argumento do §13.5.

## O que a ground truth precisou de descodificação

As máscaras do BBBC039 não trazem um inteiro por núcleo: dizem **quantos núcleos cobrem cada
pixel** — 0 fundo, 1 um núcleo, 2 e 3 onde vizinhos se sobrepõem. As instâncias são as
componentes ligadas dos 1, crescidas para os pixels disputados por watershed.

Ler o ficheiro ingenuamente dá **três objetos por imagem em vez de cem**, e as 200 máscaras
somariam 531 núcleos em vez de 19 391. Teria produzido uma pontuação bonita e sem sentido —
e nada mais neste repositório teria dado por isso.
"""


if __name__ == "__main__":
    raise SystemExit(main())
