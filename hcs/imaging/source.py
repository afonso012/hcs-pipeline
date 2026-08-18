"""Getting one HCS field's pixels out of storage (spec §8.1).

The manifest recorded an `s3://` URI per channel; this fetches them and decodes the TIFFs.
Nothing here is lazy, and for this rail that is the right answer: a BBBC021 channel is
2.5 MB, stored in strips of three rows with no compression, so a strip spans the full
width and slicing a column range saves no bytes at all. Three channels come to about
7.5 MB, which is not what the memory limits of §21 are defending against.

What the chunking defends against is the segmentation working set — the float64 smoothing,
the distance transform, the label array — and that is where the measured difference
actually was: 8 MiB at chunk 256 against 94 MiB unchunked, on an image of 2.5 MB.
"""

from collections.abc import Mapping
from io import BytesIO
from typing import Any

import numpy.typing as npt
import tifffile

from hcs.storage import read_object


class MissingChannelError(ValueError):
    """The unit does not carry the channel it is supposed to be segmented on."""


def load_field(s3: Any, channels: Mapping[str, str]) -> dict[str, npt.NDArray[Any]]:
    """Fetch and decode every channel of one field, keyed by channel name."""
    return {name: _decode(read_object(s3, uri)) for name, uri in channels.items()}


def segmentation_channel(pixels: Mapping[str, npt.NDArray[Any]], name: str) -> npt.NDArray[Any]:
    """The channel objects are found in, by name (§8.2).

    Which one that is belongs to the run's configuration rather than to this module: it is
    an assay decision, it moves every number downstream, and §12 hashes it into the run so
    two runs that disagree about it cannot be mistaken for one another.
    """
    if name not in pixels:
        raise MissingChannelError(f"segmentation channel {name!r} is not among {sorted(pixels)}")
    return pixels[name]


def _decode(body: bytes) -> npt.NDArray[Any]:
    """Decode one TIFF from bytes already in memory.

    No temporary file. A worker that wrote every channel to disk before reading it back
    would spend its I/O budget twice and turn the RSS curve into a measurement of the page
    cache.
    """
    array: npt.NDArray[Any] = tifffile.imread(BytesIO(body))
    return array
