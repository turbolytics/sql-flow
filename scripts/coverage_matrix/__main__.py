"""Entry point, so `python -m coverage_matrix` runs the generator."""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
