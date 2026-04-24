"""Make ``dagster_compass_kit`` importable when running pytest without
first installing the package (``pip install -e .``).

Once the package is installed (either by the user or in CI), this is a
no-op — the import succeeds from site-packages and the sys.path insert
is irrelevant.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
