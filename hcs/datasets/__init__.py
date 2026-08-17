"""Readers for the public datasets this pipeline runs on.

Kept apart from `hcs.science` on purpose: this layer knows the shape of somebody else's
CSV, and nothing downstream of it should. What comes out is the pipeline's own vocabulary
— plates, wells, fields, channels — so a second dataset means a second reader and no
change anywhere else.
"""
