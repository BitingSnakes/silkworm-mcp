from __future__ import annotations

import importlib
import sys
from pathlib import Path

_SRC_PATH = Path(__file__).resolve().parent / "src"
if str(_SRC_PATH) not in sys.path:
    sys.path.insert(0, str(_SRC_PATH))

_server = importlib.import_module("silkworm_mcp.server")

if __name__ == "__main__":
    _server.main()

sys.modules[__name__] = _server
