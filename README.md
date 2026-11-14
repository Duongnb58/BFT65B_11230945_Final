# A Big-Data Pipeline for Bank Failure Early Warning

**BFT65B — Big Data Analytics in Finance | Part 1: Acquisition & Storage (15 points)**

This repository contains the executable Part 1 pipeline. It acquires FDIC Financials, Failures and Institutions records; verifies the source scope and file integrity; stores typed Snappy-compressed Parquet; constructs a logically equivalent uncompressed CSV / flat Parquet comparison; measures three read workloads; and audits the recorded results. Part 2 will use these verified inputs for bank-quarter data-quality analysis and point-in-time panel construction.

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

Two notebooks cover Part 1 without repeating acquisition or benchmark work:

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

The two `run_...` directories use the **same identifier** and are connected by stored source hashes. Benchmark preparation additionally creates local `/content/bft65b_11230945_storage/` files; these are temporary and are excluded from the repository.

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
