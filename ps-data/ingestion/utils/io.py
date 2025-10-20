# ingestion/utils/io.py
"""
I/O helpers:
- write_raw / write_interim: write if content changed (idempotent)
- utc_timestamp: UTC timestamp string
"""

import io
import hashlib
from pathlib import Path
from typing import Union
from datetime import datetime, timezone

import pandas as pd


def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _read_bytes(p: Path) -> bytes:
    try:
        return p.read_bytes()
    except Exception:
        return b""


def _ensure_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def _write_if_changed_bytes(target: Path, data: bytes) -> bool:
    _ensure_dir(target)
    old = _read_bytes(target)
    if _hash_bytes(old) == _hash_bytes(data):
        return False
    target.write_bytes(data)
    return True


def write_raw(obj: Union[pd.DataFrame, str], path: Path) -> bool:
    """
    Write raw file only if content changed.
    - DataFrame -> CSV
    - str       -> UTF-8 text
    Returns True if wrote/updated, False if unchanged.
    """
    if isinstance(obj, pd.DataFrame):
        data = _to_csv_bytes(obj)
        return _write_if_changed_bytes(path, data)
    elif isinstance(obj, str):
        return _write_if_changed_bytes(path, obj.encode("utf-8"))
    else:
        raise TypeError("write_raw expects a pandas.DataFrame or str")


def write_interim(obj: Union[pd.DataFrame, str], path: Path) -> bool:
    """Same behavior as write_raw, but semantically for interim outputs."""
    return write_raw(obj, path)


def utc_timestamp() -> str:
    """UTC timestamp e.g. 20250101T235959Z"""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
