from pathlib import Path
from dotenv import load_dotenv, find_dotenv

def load_project_dotenv() -> None:
    """
    Load the nearest .env starting from current working directory,
    which works in scripts, REPL, and heredoc/stdin execution.
    """
    path = find_dotenv(usecwd=True)
    if not path:  # fallback if none found
        path = str(Path.cwd() / ".env")
    load_dotenv(path)
