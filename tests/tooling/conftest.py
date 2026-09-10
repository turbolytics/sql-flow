"""Puts scripts/ on the path, so the tests import the generator package.

pytest imports conftest before any test module, which is what makes this the
one place the path is set rather than the top of ten files.

The generator is not an installed package: it lives under scripts/ and the
Makefile runs it with PYTHONPATH set. This is the same arrangement for tests.
"""

import os
import sys

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "scripts"))
