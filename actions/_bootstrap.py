"""
Bootstrap helper that lets actions import ``salesforce.lib.sf_client``.

Each action does::

    from _bootstrap import sf_client

This adds the pack root (``packs/salesforce``) to ``sys.path`` so the
``lib`` package is importable, and re-exports the ``sf_client`` module
under a stable name.
"""

import os
import sys

_PACK_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PACK_ROOT not in sys.path:
    sys.path.insert(0, _PACK_ROOT)

from lib import sf_client  # noqa: E402,F401
