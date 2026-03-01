from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Union


def connect(db_path: Union[str, Path]) -> sqlite3.Connection:
    path = Path(db_path)
    conn = sqlite3.connect(str(path))
    return conn
