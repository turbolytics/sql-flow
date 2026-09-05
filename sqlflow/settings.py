"""Paths the release-test harness needs.

What remains of the Python package is test tooling for the Go engine's image
tests: this module, kafka.py and fixtures/. The engine itself is gone.
"""

import os

LOG_LEVEL = os.environ.get('SQLFLOW_LOG_LEVEL', 'INFO')

# The repo's dev/ directory, which the image tests mount into the container as
# /tmp/conf so a test can name a shipped example config.
DEV_DIR = os.path.join(
    os.path.dirname(__file__),
    '..',
    'dev',
)

CONF_DIR = os.path.join(DEV_DIR, 'config')
FIXTURES_DIR = os.path.join(DEV_DIR, 'fixtures')
