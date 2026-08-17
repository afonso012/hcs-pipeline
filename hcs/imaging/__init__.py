"""Reading images and turning them into rows (spec §8).

The layer is arranged so that nothing the size of the full image is ever built. A block
comes in, cells come out, and the only thing that grows with the image is the number of
rows — which is why the same code serves a 1024x1280 HCS field and a whole-slide image.
"""
