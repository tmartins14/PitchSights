# ingestion/utils/paths.py
from pathlib import Path
import os

def repo_root() -> Path:
    # This file lives at ps-betting/ingestion/utils/paths.py
    # Going up two levels lands at ps-betting/
    return Path(__file__).resolve().parents[2]

def data_dir() -> Path:
    # Allow override via env if needed; default to ps-betting/data
    return Path(os.getenv("PITCHSIGHTS_DATA_DIR", str(repo_root() / "data")))
