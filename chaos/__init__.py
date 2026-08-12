"""
Chaos harness for the raft-kv-store engine.

./raft-engine is a read-only dependency: it is cloned, imported from, and
driven externally. Nothing in this package writes to it. Because that repo
keeps its packages under `src/` and expects `pythonpath = src` (see its
pytest.ini), we prepend that directory here so `import raft...` resolves the
same way it does inside the engine's own test suite.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ENGINE_SRC = Path(__file__).resolve().parent.parent / "raft-engine" / "src"

if not _ENGINE_SRC.is_dir():  # pragma: no cover - setup guard
    raise RuntimeError(
        f"raft-engine sources not found at {_ENGINE_SRC}. "
        "Clone it first: git clone https://github.com/abho7/raft-kv-store.git ./raft-engine"
    )

if str(_ENGINE_SRC) not in sys.path:
    sys.path.insert(0, str(_ENGINE_SRC))
