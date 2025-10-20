import sys
import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

paths_to_add = [
    os.path.join(ROOT_DIR,"data", "scripts", "api"),
    os.path.join(ROOT_DIR,"data", "scripts", "fbref"),
    os.path.join(ROOT_DIR,"data", "scripts", "data_cleaning"),
    os.path.join(ROOT_DIR, "strategies", "shots_on_target"),
    os.path.join(ROOT_DIR, "models"),
    os.path.join(ROOT_DIR, "ingestion", "utils")
]
for path in paths_to_add:
    if path not in sys.path:
        sys.path.append(path)
