"""Entry point for Part 4 feature construction and verification."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fdic_pipeline.feature_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
