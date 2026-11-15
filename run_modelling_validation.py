"""Part 5 command-line interface. Train first; evaluate test exactly once; verify read-only."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fdic_pipeline.modelling_validation import cli

if __name__ == "__main__":
    raise SystemExit(cli())
