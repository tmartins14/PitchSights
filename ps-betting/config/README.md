# config

Centralized settings for paths, leagues, seasons, secrets (loaded via env), and market parameters.

## Files

- `settings.yaml` — global config (paths, leagues, seasons, rate limits)
- `markets.yaml` — per-market knobs (lines, buffers, min_ev, stakes)
- `logging.yaml` — logging format/level/handlers
- `setup_paths.py` — helper to resolve project-relative paths

## Key ideas

- Keep secrets out of VCS; read from env or a local `.env`.
- All jobs read from `settings.yaml` + `markets.yaml` only.

## Example

```python
from config.setup_paths import PATHS
from common.io import load_yaml
cfg = load_yaml(PATHS.config/'settings.yaml')
```
