"""Properties of the deterministic work unit identity (spec §7.1).

The manifest is re-runnable *by construction* rather than by deduplication logic:
`INSERT ... ON CONFLICT (id) DO NOTHING` is idempotent in both `work_units` and the
subtype table only if `id` is a pure function of the unit's logical identity.
Everything here defends that one claim.
"""

import uuid

import pytest
from hypothesis import given
from hypothesis import strategies as st

from hcs.identity import NS_HCS, hcs_field_id, wsi_slide_id

# Frozen expectations. If these change, every id in every database changes with them,
# so a diff here must be a deliberate, migration-bearing decision — never a refactor.
GOLDEN_NAMESPACE = uuid.UUID("93918048-52d0-4a00-ae2b-068533a34aa9")
GOLDEN_FIELD_ID = uuid.UUID("03e3ddec-40fa-53a6-9713-77d9396767ea")
GOLDEN_SLIDE_ID = uuid.UUID("19963cf6-5ba6-5735-84df-cad7c0fdfeaa")

# Hypothesis strategies for the identity components. Barcodes and wells are opaque
# strings from BBBC021_v1_image.csv, so the only excluded character is the delimiter.
barcodes = st.text(min_size=1, max_size=40).filter(lambda s: "|" not in s)
wells = st.text(min_size=1, max_size=8).filter(lambda s: "|" not in s)
field_indices = st.integers(min_value=0, max_value=32767)  # SMALLINT
digests = st.text(alphabet="0123456789abcdef", min_size=64, max_size=64)


class TestFrozenNamespace:
    def test_namespace_never_changes(self):
        assert NS_HCS == GOLDEN_NAMESPACE

    def test_hcs_field_id_matches_frozen_value(self):
        assert hcs_field_id("Week1_22141", "B04", 1) == GOLDEN_FIELD_ID

    def test_wsi_slide_id_matches_frozen_value(self):
        assert wsi_slide_id("a" * 64) == GOLDEN_SLIDE_ID


class TestDeterminism:
    @given(barcode=barcodes, well=wells, field_index=field_indices)
    def test_hcs_field_id_is_a_pure_function(self, barcode, well, field_index):
        assert hcs_field_id(barcode, well, field_index) == hcs_field_id(barcode, well, field_index)

    @given(digest=digests)
    def test_wsi_slide_id_is_a_pure_function(self, digest):
        assert wsi_slide_id(digest) == wsi_slide_id(digest)

    @given(barcode=barcodes, well=wells, field_index=field_indices)
    def test_hcs_field_id_is_a_version_5_uuid(self, barcode, well, field_index):
        assert hcs_field_id(barcode, well, field_index).version == 5

    @given(digest=digests)
    def test_wsi_slide_id_is_a_version_5_uuid(self, digest):
        assert wsi_slide_id(digest).version == 5


@st.composite
def resplit_pairs(draw):
    """Two (barcode, well) pairs whose concatenation is the same string.

    Splitting ``"ABC"`` at 1 and at 2 gives ``("A", "BC")`` and ``("AB", "C")``. These
    are different fields that a separator-less format would collapse onto one id, and
    they are the pairs an independent-draw strategy will essentially never produce.
    """
    text = draw(st.text(min_size=3, max_size=20).filter(lambda s: "|" not in s))
    left = draw(st.integers(min_value=1, max_value=len(text) - 1))
    right = draw(st.integers(min_value=1, max_value=len(text) - 1).filter(lambda k: k != left))
    return (text[:left], text[left:]), (text[:right], text[right:])


class TestInjectivity:
    """Distinct logical identities must produce distinct ids.

    A uuid5 collision is not the risk here; an ambiguous *name string* is. Without a
    reserved delimiter, ("A|B", "C") and ("A", "B|C") would serialise identically and
    two different fields would silently share a primary key.
    """

    @given(pairs=resplit_pairs(), field_index=field_indices)
    def test_the_barcode_well_boundary_is_unambiguous(self, pairs, field_index):
        (barcode_a, well_a), (barcode_b, well_b) = pairs
        assert hcs_field_id(barcode_a, well_a, field_index) != hcs_field_id(
            barcode_b, well_b, field_index
        )

    @given(
        barcode=barcodes,
        digits=st.text(alphabet="0123456789", min_size=1, max_size=4),
        suffix=st.text(alphabet="0123456789", min_size=1, max_size=4),
    )
    def test_the_well_index_boundary_is_unambiguous(self, barcode, digits, suffix):
        """Wells end in digits and field indices are digits, so the seam between them
        is the second place a missing separator would silently merge two fields:
        ``("B0", 41)`` and ``("B04", 1)`` both concatenate to ``B041``."""
        assert hcs_field_id(barcode, digits, int(suffix)) != hcs_field_id(
            barcode, digits + suffix, 0
        )

    @given(
        a=st.tuples(barcodes, wells, field_indices),
        b=st.tuples(barcodes, wells, field_indices),
    )
    def test_distinct_fields_get_distinct_ids(self, a, b):
        if a == b:
            return
        assert hcs_field_id(*a) != hcs_field_id(*b)

    @given(a=digests, b=digests)
    def test_distinct_digests_get_distinct_ids(self, a, b):
        if a == b:
            return
        assert wsi_slide_id(a) != wsi_slide_id(b)

    @given(barcode=barcodes, well=wells, field_index=field_indices, digest=digests)
    def test_kinds_share_no_ids(self, barcode, well, field_index, digest):
        """The kind prefix keeps the two rails in disjoint id spaces."""
        assert hcs_field_id(barcode, well, field_index) != wsi_slide_id(digest)


class TestRejectedInput:
    @pytest.mark.parametrize(
        ("barcode", "well", "field_index"),
        [
            ("Week1|22141", "B04", 1),  # delimiter in barcode
            ("Week1_22141", "B|04", 1),  # delimiter in well
            ("", "B04", 1),  # empty barcode
            ("Week1_22141", "", 1),  # empty well
            ("Week1_22141", "B04", -1),  # negative field index
        ],
    )
    def test_hcs_field_id_rejects_ambiguous_identity(self, barcode, well, field_index):
        with pytest.raises(ValueError):
            hcs_field_id(barcode, well, field_index)

    @pytest.mark.parametrize(
        "digest",
        [
            "",
            "a" * 63,
            "a" * 65,
            "g" * 64,  # not hexadecimal
            "a" * 32,  # md5-length
        ],
    )
    def test_wsi_slide_id_rejects_malformed_digest(self, digest):
        with pytest.raises(ValueError):
            wsi_slide_id(digest)

    def test_digest_case_is_not_part_of_the_identity(self):
        """Hex digest case carries no meaning, so it must not fork the id."""
        assert wsi_slide_id("A" * 64) == wsi_slide_id("a" * 64)
