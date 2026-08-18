"""Quality control before the pixels are worth segmenting (spec §8.2).

> QC primeiro, antes de gastar computação. [...] Intensidade mediana perto do fundo →
> poço vazio.

One check so far, and it earns its place by having already been needed. Otsu is defined to
split whatever it is given into two classes; it cannot answer "there is nothing here". Give
it a field where every cell died and it splits the background noise down the middle and
reports half the image as foreground.

Measured on `Week9_39282 F03 s3`, mitoxantrone at 10 µM: maximum 1264 against a median of
864, so the field holds no nuclei at all. Otsu returned 857 — *below* the median — marking
52% of the pixels as foreground and yielding **2166 objects**. Its three sibling fields, with
a handful of survivors each, gave 4, 2 and 2. Real fields on this dataset put 5-9% of their
pixels above the threshold.

That single field was enough to destroy the golden batch's dose-response: it turned the
top dose from the lowest point on the curve into the highest, and the IC50 fitted to it
came out at 6.5e9 µM.

The same shape of error appeared once before, one level down, and was fixed by taking the
threshold from the whole image instead of from a block (§8.1). This is the residue: when
the whole image is background, there is no wider view left inside the field, and the answer
has to be to decline rather than to threshold harder.
"""

from dataclasses import dataclass
from typing import Final

import numpy as np
import numpy.typing as npt

# Nuclei do not cover a third of a field. In BBBC021 a healthy field puts 5-9% of its
# pixels above the threshold and a sparse one far less; the broken field put 52%. The
# bound is deliberately far above anything real rather than tuned close to it — this is a
# check for "the threshold found no nuclei", not a segmentation parameter, and a value
# near the data would start silently deciding results.
MAX_FOREGROUND_FRACTION: Final = 0.35


@dataclass(frozen=True, slots=True)
class FieldQc:
    """Whether a field is worth segmenting, and the number behind the answer."""

    foreground_fraction: float
    accepted: bool

    @property
    def reason(self) -> str:
        if self.accepted:
            return ""
        return (
            f"no nuclei distinguishable: the threshold selects "
            f"{self.foreground_fraction:.0%} of the field, above the "
            f"{MAX_FOREGROUND_FRACTION:.0%} a field of nuclei ever reaches"
        )


def check_field(image: npt.NDArray[np.floating | np.integer], threshold: float) -> FieldQc:
    """Decide whether a threshold has separated nuclei from background, or split noise.

    Cheap by construction — one comparison over the array it was going to read anyway —
    because §8.2's whole point is that QC runs before the expensive part.
    """
    fraction = float(np.mean(np.asarray(image) > threshold))
    return FieldQc(foreground_fraction=fraction, accepted=fraction <= MAX_FOREGROUND_FRACTION)
