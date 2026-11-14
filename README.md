# A Big-Data Pipeline for Bank Failure Early Warning

**BFT65B — Big Data Analytics in Finance | Part 1: Acquisition & Storage (15 points); Part 2: Data Quality & Panel (20 points)**

This repository implements the completed Part 1 acquisition/storage workflow and the Part 2 automated data-quality audit and clean bank–quarter panel. Part 2 reads only the verified Part 1 acquisition output and preserves its hashes; it does not repeat FDIC API retrieval or the storage benchmark. All derived data, event-reconciliation tables and figures remain outside GitHub on Google Drive.

The source files do not contain downloaded FDIC records, benchmark output files or claims about results that have not been measured in the current execution.

## 1. Data and field selection

The examination dataset has three source tables:

| Source | Scope | Selected fields |
|---|---|---|
| Financials | All quarterly reporting dates, 1984Q1–2026Q2 (170 quarters) | 33 fields: certificate, reporting date, geographic descriptor, balance-sheet measures, loans, credit quality, earnings and regulatory ratios |
| Failures | Complete FDIC failure-event table | `CERT`, `NAME`, `CITYST`, `FAILDATE`, `FAILYR`, `COST`, `QBFASSET`, `QBFDEP`, `RESTYPE` |
| Institutions | Complete FDIC institution table | `CERT`, `NAME`, `STNAME`, `BKCLASS`, `ACTIVE`, `ESTYMD`, `ENDEFYMD`, `CHANGEC1`, `INSDATE` |

The 33 Financials fields are defined in `src/fdic_pipeline/acquisition.py` and initially follow the instructor's `fdic_download_starter.py`. They cover: size and funding (`ASSET`, `DEP`, `DEPDOM`, `EQ`, `BRO`), liquidity and securities (`CHBAL`, `SC`), credit exposures (`LNLSGR`, `LNLSNET`, `LNRE`, `LNCI`, `LNCON`, `LNATRES`), loan and asset quality (`NCLNLS`, `P3ASSET`, `P9ASSET`, `NAASSET`, `NTLNLS`, `ORE`), earnings (`NETINC`, `INTINC`, `EINTEXP`, `NONII`, `NONIX`, `ELNATR`, `ROA`, `ROE`, `NIMY`) and regulatory capital ratios (`RBC1AAJ`, `RBCRWAJ`). This selection captures multiple dimensions relevant to later monitoring without defining predictors or risk labels at the acquisition stage. Consult the FDIC dictionary before interpreting any financial code; some fields may be historically unavailable.

Sources: [FDIC BankFind Suite API](https://api.fdic.gov/banks/docs/), [FDIC Financials dictionary](https://api.fdic.gov/banks/docs/risview_properties.yaml), [FDIC Failures dictionary](https://api.fdic.gov/banks/docs/failure_properties.yaml), [FDIC Institutions dictionary](https://api.fdic.gov/banks/docs/institution_properties.yaml), and the BFT65B examination sheet and starter code.

## 2. Execution sequence

The execution order is Part 1 Notebook 01 → Part 1 Notebook 02 → Part 2 Notebook 03. Part 2 depends on the verified acquisition dataset, not on the performance benchmark results. The original two Part 1 notebooks remain unchanged:

| Notebook | Sections | Dependency | Produced evidence |
|---|---|---|---|
| [`notebooks/01_acquisition_colab.ipynb`](notebooks/01_acquisition_colab.ipynb) | 1. GitHub and Python; 2. tests; 3. Drive; 4. FDIC retrieval; 5. dataset verification; 6. handoff | New Drive folder and working FDIC API | Quarter-partitioned Financials, complete reference tables, `manifest.json`, `verification_report.json`, logs |
| [`notebooks/02_storage_colab.ipynb`](notebooks/02_storage_colab.ipynb) | 1. setup and storage tests; 2. source contract; 3. identical format pair; 4. size; 5. timed reads; 6. audit; 7. presentation; 8. end-to-end contract | Verified dataset produced by Notebook 01 | `preparation.json`, raw trials, summaries, charts, provenance and the report evidence package |

**Run Notebook 01 before Notebook 02.** Notebook 02 uses the exact output path of Notebook 01 and derives its expected observation count from the current verification report, not from earlier experiments. It generates one benchmark run ID and passes that same identifier explicitly to the analysis stage. Separate Colab sessions require package installation and Drive mounting again; this is runtime initialization, not re-acquisition.

Colab notebooks:
- `https://colab.research.google.com/github/Duongnb58/BFT65B_11230945_Final/blob/main/notebooks/01_acquisition_colab.ipynb`
- `https://colab.research.google.com/github/Duongnb58/BFT65B_11230945_Final/blob/main/notebooks/02_storage_colab.ipynb`

### Drive layout

All data and evidence stay under `MyDrive/BFT65B_11230945_Final/`:

```text
BFT65B_11230945_Final/
├── fdic_1984Q1_2026Q2/
│   ├── financials/year=YYYY/quarter=Q/part-00000.parquet
│   ├── failures.parquet
│   ├── institutions.parquet
│   ├── manifest.json
│   ├── verification_report.json
│   └── logs/
├── storage_benchmark/run_YYYYMMDDTHHMMSSZ/
│   ├── preparation.json
│   ├── benchmark_verification.json
│   ├── environment.json
│   ├── trials.csv
│   ├── summary.csv
│   ├── comparisons.csv
│   ├── storage_sizes.csv
│   ├── storage_sizes.png
│   ├── read_time_medians.png
│   ├── peak_rss_medians.png
│   └── storage_benchmark.log
└── storage_analysis/run_YYYYMMDDTHHMMSSZ/
    ├── analysis_validation.json
    ├── source_file_hashes.json
    ├── report_storage_table.csv
    ├── report_performance_table.csv
    ├── part1_storage_report_draft.md
    └── figures/*.png
```

The two Part 1 `run_...` directories use the **same identifier** and are connected by stored source hashes. Benchmark preparation additionally creates local `/content/bft65b_11230945_storage/` files; these are temporary and are excluded from the repository.

## 3. Acquisition method and quality gate

HTTP requests to the public FDIC API use deterministic page parameters, bounded retries for transient failures, pagination-total checks and per-quarter reporting-date filters. Each Financials quarter is a separately committed Parquet file. The program checks required keys, duplicate `(CERT, REPDTE)` pairs, source row counts, explicit Arrow types and file hashes. Valid quarters are reused on an **interrupted run of this new study**; invalid or missing units are downloaded again. The complete reference tables are fetched without date filtering.

Some historical Failures records have a missing `CERT`. These source events are retained, represented as nullable `int64`, and counted in the verification report. They cannot be matched by certificate without additional research, and no event is discarded solely for missing `CERT`. Financials and Institutions still require non-null, unique keys as appropriate.

Acquisition is complete only when `verification_report.json` states `success: true`, `expected_quarters = verified_quarters = 170`, `missing_quarters = []` and `invalid_artifacts = []`. **The financial row count is measured, not fixed at a value from a previous run.** Each artifact records SHA-256 and the download time in UTC. A successful structural audit is not an economic/accounting data-quality audit; that is Part 2.

## 4. Storage architecture

- **Format:** Apache Parquet, column-oriented, Snappy compression, dictionary encoding where suitable.
- **Partitioning:** Hive-style `financials/year=YYYY/quarter=Q`, one Parquet file per quarter-end, 170 reporting quarters.
- **Schema:** `CERT: int64`, `REPDTE: date32`, descriptive fields: UTF-8 strings, financial quantities and ratios: nullable `float64`. The same schema applies to every Financials partition.
- **Source preservation:** No imputation, clipping, ratio engineering, failure labelling or merger classification is performed here.
- **Reference tables:** `failures.parquet` and `institutions.parquet`, with their own typed schemas.

Partitioning supports date-based selective access and independent recovery of quarters; it is not assumed to accelerate every query. Repeated Parquet metadata may increase partitioned-file size relative to a flat Parquet file.

## 5. Benchmark design

The controlled comparison uses the Financials dataset only. All 170 verified source partitions are hashed and copied to the local Colab filesystem. A **single flat CSV (uncompressed)** and **single flat Parquet (Snappy)** are created from the same typed Arrow batches. Equality of Arrow tables, schemas, values, nulls and row counts is required. Three representations are counted for file size: flat CSV, flat Parquet, and the original partitioned Parquet layout. Read timings compare **flat CSV vs flat Parquet**, not the quarter-partitioned directory.

Three reader workloads are executed with PyArrow for both formats: (i) full scan, (ii) projection onto six columns, and (iii) projection plus `REPDTE >= 2020-01-01`. Each format/workload pair is measured in a fresh Python process with **one warmup and five timed repetitions**. Seed `11230945` fixes the randomized task order. The output row count, sum of certificates and sum of assets must match between formats in every paired trial. Elapsed time uses `time.perf_counter`; peak process RSS uses Linux `resource.ru_maxrss`. The report compares medians, not single trials. RSS represents the peak process footprint, including runtime overhead.

`trials.csv` stores individual executions, `summary.csv` stores medians and ranges, `comparisons.csv` stores CSV/Parquet ratios, and `environment.json` stores runtime metadata and the Git commit SHA when available. An independent analysis module audits all source tables before producing report-ready tables and figures.

The benchmark is a one-machine, one-engine measurement. CPU load, filesystem cache and Colab resource variation are not fully controlled. The date-filtered Parquet reader can use file-level statistics, but the experiment does not isolate row-group pruning. These measurements do **not** establish the effect of partitioning on read latency or the threshold for deploying Spark; the latter requires Part 3 experiments.

## 6. Reproduction and tests

Use **Google Colab CPU runtime**. Open and run Notebook 01 in order, then Notebook 02. Neither GPU nor paid infrastructure is required. New package versions should not be substituted for the pinned versions without documenting the change. The source API may revise historical observations; the manifest and download timestamps identify the observed vintage, but not a transactionally consistent FDIC snapshot.

From a terminal with Python installed:

```bash
python -m pip install -r requirements.txt
python -m pytest -q -rs tests

# Examples after mounting the new Drive root in Colab:
python run_acquisition.py --start 1984-03-31 --end 2026-06-30 --out /content/drive/MyDrive/BFT65B_11230945_Final/fdic_1984Q1_2026Q2
python run_storage_benchmark.py prepare --source-dir /content/drive/MyDrive/BFT65B_11230945_Final/fdic_1984Q1_2026Q2 --work-dir /content/bft65b_11230945_storage
python run_storage_benchmark.py benchmark --work-dir /content/bft65b_11230945_storage --results-dir /content/drive/MyDrive/BFT65B_11230945_Final/storage_benchmark/run_YYYYMMDDTHHMMSSZ --repetitions 5 --warmups 1 --seed 11230945
python run_storage_analysis.py --benchmark-dir /content/drive/MyDrive/BFT65B_11230945_Final/storage_benchmark/run_YYYYMMDDTHHMMSSZ --output-dir /content/drive/MyDrive/BFT65B_11230945_Final/storage_analysis/run_YYYYMMDDTHHMMSSZ
```

Replace `run_YYYYMMDDTHHMMSSZ` with **one identical concrete run ID** for both result directories. PyArrow tests must **not** be skipped in the Colab execution. The notebook stages enforce that condition. Record the raw Colab outputs and results in Drive. Before submitting, save the executed notebooks with their small summary outputs into GitHub; never commit raw data or benchmark CSV/PNG/JSON results. Images and detailed evidence can be presented in the report or appendix.

## 7. Report evidence and academic integrity

The final report must be in English, at most 20 pages plus a 10-page appendix. Page 1 must state author, student ID, GitHub URL, data acquisition method and date, actual row counts, and eventual development/test windows with their failure counts. Development/test periods are determined in later parts; they are **not** inferred by Part 1.

The report must distinguish measured outcomes from assumptions and must cite FDIC, the course assignment, the instructor's starter, the Apache Arrow documentation and any AI assistance or third-party code used. This repository was prepared with assistance from OpenAI ChatGPT, with source methods adapted from the instructor's starter; the researcher is responsible for executing, reviewing and validating all results. Do not present results from an earlier repository or Drive as though they had been measured in this study.


## 8. Part 2 – Data Quality & Clean Bank–Quarter Panel (20 points)

### 8.1 Reproduction and exact dependency

Run [`notebooks/03_data_quality_panel_colab.ipynb`](notebooks/03_data_quality_panel_colab.ipynb) after Part 1 Notebook 01 has produced `fdic_1984Q1_2026Q2/manifest.json` and `verification_report.json`. The notebook reads the **same** Drive root and never calls the FDIC API or repeats storage measurements. Parts 1 and 2 share `requirements.txt`: `requests`, `pyarrow`, `matplotlib` and `pytest`, each pinned to a concrete version. The source schema comes directly from `src/fdic_pipeline/acquisition.py`.

Notebook: <https://colab.research.google.com/github/Duongnb58/BFT65B_11230945_Final/blob/main/notebooks/03_data_quality_panel_colab.ipynb>

| Section | Input / analysis | Evidence / acceptance |
|---|---|---|
| 1. Source and tests | GitHub commit, pinned environment, deterministic quality tests and a real Parquet integration test | No failed or skipped tests |
| 2. Part 1 contract | Acquisition manifest, verification report | 170 quarters, observed row counts, field selection, hashes |
| 3. Quality policy | Current-report checks; explicit 90-day availability *assumption* | Run ID and policy recorded |
| 4. Panel generation | Verified 170 Financials partitions and both full reference files | Clean panel + detailed audit + verified manifest |
| 5. Quality findings | Null rates by field/year, quarantined rows, financial anomaly flags | Tables and images based on the observed FDIC extract |
| 6. Exit review | CERT-linked dated failures; dated institution endings; financial reporting history | Disjoint terminal classes; no terminal information in predictors |
| 7. Independent verification | Manifest, each output partition and audit-file checksums | `Part 2 evidence: PASSED` |

Command-line equivalent after mounting Drive and cloning the repository:

```bash
python -m pytest -q -rs tests/test_data_quality.py tests/test_panel_integration.py
python run_data_quality.py run \
  --source-dir /content/drive/MyDrive/BFT65B_11230945_Final/fdic_1984Q1_2026Q2 \
  --output-dir /content/drive/MyDrive/BFT65B_11230945_Final/data_quality_panel/run_YYYYMMDDTHHMMSSZ \
  --availability-lag-days 90
python run_data_quality.py verify \
  --source-dir /content/drive/MyDrive/BFT65B_11230945_Final/fdic_1984Q1_2026Q2 \
  --output-dir /content/drive/MyDrive/BFT65B_11230945_Final/data_quality_panel/run_YYYYMMDDTHHMMSSZ
```

Replace the run ID with the same unique UTC identifier in both commands. The **run** command refuses to overwrite an existing output directory. If processing is interrupted before validation, inspect/remove the incomplete Part 2 output directory before starting a new run; source acquisition files remain unchanged.

### 8.2 Quality rules and scientific interpretation

- **Missingness:** full field-level and field-year audit, no imputation or global clipping. Indicators such as ROA/ROE can be absent in early periods or for particular institutions; missingness is documented before later training-only transformations.
- **Core bank-quarter validity:** `(CERT, REPDTE)` must be unique and reporting dates must match the correct quarter. Rows with null/nonpositive `ASSET` are recorded in `quarantined_rows.csv` rather than silently corrected; all other reported amounts and ratios are preserved.
- **Accounting plausibility screens:** negative `DEP` or `LNLSGR`, deposits or gross loans above `ASSET`, `NCLNLS` above nonnegative `LNLSGR`, and `|ROA|`/`|ROE|` above 100 percentage points. These are **review flags**, not claims of data-entry errors or exclusion criteria. Negative equity is not automatically treated as invalid.
- **Bank history:** `PRIOR_REPORT_COUNT` and `REPORT_GAP_QUARTERS` use earlier reports for the same CERT. Interior reporting gaps are counted but never interpolated. Later reports do not retrospectively change earlier-row features.
- **Exit review:** `terminal_exits.csv` combines the last observed financial reporting quarter with dated failure events and dated institution terminations. It distinguishes confirmed failures, documented other exits, right-censoring, unresolved disappearance, and inconsistent/ambiguous event histories. A bank disappearing from Financials is **not** automatically considered a failure or a merger. Failure records without `CERT` are retained in the linkage audit and never matched by fabricated identifiers. The nine selected Institutions fields do **not** provide a complete dated merger history; `CHANGEC1` or the current `ACTIVE` flag is not treated as historical status for all quarters.
- **Point in time:** the clean panel retains current-quarter FDIC observations, contemporaneous quality flags and historical reporting counts/gaps. It does **not** contain future failure events, current-snapshot institution status or retrospectively known exit types. `ASSUMED_AVAILABLE_FROM = REPDTE + 90 days` is an *explicit publication-lag assumption*, not an FDIC-recorded release date. A strict historical-vintage or release-time guarantee requires additional source metadata and remains a limitation. Part 4 must also respect the examination's label maturity limit of 2025Q2 and use a time-ordered evaluation.

### 8.3 Produced Drive artifacts and next-stage interface

```text
MyDrive/BFT65B_11230945_Final/
├── fdic_1984Q1_2026Q2/                   # original verified Part 1 source; unchanged
└── data_quality_panel/run_YYYYMMDDTHHMMSSZ/
    ├── panel/year=YYYY/quarter=Q/part-00000.parquet  # 170 typed partitions
    ├── quality_variables.csv             # field-level missingness, observed extrema
    ├── quality_by_year.csv               # year-by-variable missingness
    ├── quality_rules.csv                 # current-quarter screening counts
    ├── coverage_by_quarter.csv           # coverage and quarantine counts
    ├── quarantined_rows.csv              # explicitly excluded bank-quarter keys
    ├── anomaly_examples.csv              # bounded review samples by rule
    ├── failure_linkage.csv               # source event linkage (separate from predictors)
    ├── terminal_exits.csv                # retrospective classifications (separate)
    ├── quality_report.md                 # autogenerated data-quality evidence
    ├── figures/*.png
    ├── part2_manifest.json               # source hashes, output partition hashes and policy
    └── part2_validation.json             # independent verification verdict
```

**Part 3 input:** the verified `panel/` directory, with the exact run ID and manifest. `terminal_exits.csv` is not a contemporaneous predictor table and must not be joined to model features as an as-of characteristic. Part 4 must define future failure labels separately, using dated event evidence, chronology and outcome maturity requirements.

### 8.4 Evidence and limitations

The automated report is a **data-quality evidence file**, not the final research report. It documents the rules, exclusions, source lineage, temporal restrictions, empirical counts and three descriptive figures. The Part 2 evaluation is based on direct FDIC observations; no row counts, missing-value rates, exit frequencies or performance claims are fabricated in source code.

The FDIC BankFind public endpoints can revise historical observations. Therefore a fully point-in-time historical vintage is not established merely by avoiding future columns; a research model must explicitly state the publication-lag assumption and the limits of retrospective public data. For merger-specific conclusions, additional dated institution history events are needed. Sources: [FDIC BankFind Suite API](https://api.fdic.gov/banks/docs/), [FDIC BankFind bulk/history definitions](https://banks.data.fdic.gov/bankfind-suite/bulkData), and the BFT65B final examination sheet. Code development and documentation used OpenAI ChatGPT assistance; the operator must validate the Colab outputs and cite that use in the submitted report.
