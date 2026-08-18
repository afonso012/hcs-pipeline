"""Operational scripts: seeding, measurement, tuning and failure injection.

A package rather than loose files because two things import from here — the golden batch
test imports `scripts.golden_batch`, and the tuning sweep reuses the mask decoding from
`scripts.accuracy_bbbc039`. Without an `__init__.py`, mypy sees each of those files under
two module names and refuses to check either.

Excluded from coverage on purpose (§13.6): these are measurement and operations, and a
coverage target on them would reward writing tests for a load generator instead of for the
pipeline it measures.
"""
