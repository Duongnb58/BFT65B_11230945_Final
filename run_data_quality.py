"""Part 2 entry point: automated quality audit and point-in-time panel."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fdic_pipeline.panel import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
