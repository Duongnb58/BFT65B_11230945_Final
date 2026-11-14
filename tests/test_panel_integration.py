"""Real Parquet E2E tests. They must not be skipped in Google Colab."""
import json
from datetime import date, datetime
from pathlib import Path
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fdic_pipeline.acquisition import (Settings, arrow_schema, financial_path,
                                       quarter_ends, sha256, spec_for)
from fdic_pipeline.panel import PanelError, build_panel, verify_panel, panel_schema


def _fixture(tmp_path):
    source = tmp_path / "fdic_1984Q1_2026Q2"
    source.mkdir()
    quarters = quarter_ends(date(1984, 3, 31), date(2026, 6, 30))
    manifest = {"spec": spec_for(Settings()), "financials": {}, "tables": {}}
    n = 0
    # Bank 101 disappears after 1984Q1 with a documented failure in 1984Q2;
    # Bank 202 is right-censored at the end, with one quarantined observation.
    for ix, q in enumerate(quarters):
        qdate = datetime.strptime(q, "%Y%m%d").date()
        records = [{"CERT": 202, "REPDTE": qdate, "STNAME": "Virginia",
                    "ASSET": 0.0 if ix == 0 else 300.0,
                    "DEP": 100.0, "LNLSGR": 75.0, "ROA": 0.1}]
        if ix == 0:
            records.append({"CERT": 101, "REPDTE": qdate, "STNAME": "Ohio",
                            "ASSET": 100.0, "DEP": 120.0, "ROA": 0.0})
        file = financial_path(source, q)
        file.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(records, schema=arrow_schema("financials")),
                       file, compression="snappy")
        manifest["financials"][q] = {"complete": True, "rows": len(records),
                                     "api_total": len(records), "sha256": sha256(file)}
        n += len(records)
    failures = [
        {"CERT": 101, "FAILDATE": "19840410", "NAME": "Failed 101"},
        {"CERT": None, "FAILDATE": "19840410", "NAME": "Missing CERT"},
    ]
    institutions = [
        {"CERT": 101, "ENDEFYMD": "19840410", "ACTIVE": "0"},
        {"CERT": 202, "ENDEFYMD": None, "ACTIVE": "1"},
    ]
    for name, rec in [("failures", failures), ("institutions", institutions)]:
        file = source / (name + ".parquet")
        pq.write_table(pa.Table.from_pylist(rec, schema=arrow_schema(name)), file)
        manifest["tables"][name] = {"complete": True, "rows": len(rec),
                                    "sha256": sha256(file), "api_total": len(rec)}
    (source / "manifest.json").write_text(json.dumps(manifest))
    verified = {"success": True, "scope": {"start": quarters[0], "end": quarters[-1]},
                "expected_quarters": 170, "verified_quarters": 170,
                "missing_quarters": [], "invalid_artifacts": [],
                "financial_rows": n, "distinct_financial_certs": 2,
                "tables": {"failures": {"rows": 2, "missing_cert_rows": 1},
                           "institutions": {"rows": 2}}}
    (source / "verification_report.json").write_text(json.dumps(verified))
    return source


def test_full_panel_and_lineage(tmp_path):
    source = _fixture(tmp_path)
    out = tmp_path / "part2" / "run_test"
    result = build_panel(source, out, git_commit="fixture_commit")
    assert result["success"]
    assert result["source_rows"] == 171
    assert result["clean_rows"] == 170
    assert result["quarantined_rows"] == 1
    assert result["exit_counts"]["confirmed_failure"] == 1
    assert result["exit_counts"]["right_censored"] == 1
    assert result["bank_certificates"] == 2
    assert verify_panel(source, out)["success"]
    p = out / "panel/year=1984/quarter=1/part-00000.parquet"
    table = pq.ParquetFile(p).read()
    assert table.schema == panel_schema()
    assert table.num_rows == 1
    assert table.column("CERT").to_pylist() == [101]
    assert table.column("DQ_DEPOSITS_GT_ASSETS").to_pylist() == [True]
    assert table.column("PRIOR_REPORT_COUNT").to_pylist() == [0]
    assert table.column("ASSUMED_AVAILABLE_FROM").to_pylist() == [date(1984, 6, 29)]
    assert "FAILDATE" not in table.column_names
    assert "ACTIVE" not in table.column_names
    assert "TERMINAL_CLASS" not in table.column_names
    assert len(list((out / "panel").rglob("*.parquet"))) == 170
    assert (out / "figures/field_missingness.png").stat().st_size > 0
    with pytest.raises(PanelError, match="Output already exists"):
        build_panel(source, out)


def test_verification_rejects_modified_evidence(tmp_path):
    source = _fixture(tmp_path)
    out = tmp_path / "result"
    build_panel(source, out)
    p = out / "quality_rules.csv"
    p.write_text(p.read_text() + "tampered\n")
    with pytest.raises(PanelError, match="Audit evidence hash mismatch"):
        verify_panel(source, out)


def test_refuse_modified_acquisition(tmp_path):
    source = _fixture(tmp_path)
    out = tmp_path / "result"
    build_panel(source, out)
    p = source / "verification_report.json"
    p.write_text(p.read_text() + "\n")
    with pytest.raises(PanelError, match="not derived from"):
        verify_panel(source, out)
