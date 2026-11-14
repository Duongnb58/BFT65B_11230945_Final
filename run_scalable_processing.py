"""Part 3 entry point: measured pandas, Polars and PySpark processing."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fdic_pipeline.scalable_processing import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
