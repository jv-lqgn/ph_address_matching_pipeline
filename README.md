# 🇵🇭 Philippine Address Matching Pipeline

A locally-run batch pipeline that standardizes and validates unstructured Philippine addresses. It takes raw address text as input and outputs structured, validated records with **barangay**, **city/municipality**, **province**, and **region** fields resolved.

---

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [Data Setup](#data-setup)
4. [Prerequisites](#prerequisites)
5. [Installation](#installation)
6. [Running the Pipeline](#running-the-pipeline)
7. [Inputs](#inputs)
8. [Outputs](#outputs)
9. [Match Outcome Reference](#match-outcome-reference)
10. [QA Process](#qa-process)
11. [Utility Scripts](#utility-scripts)
12. [Project Structure](#project-structure)
13. [Re-syncing Dependencies](#re-syncing-dependencies)
14. [Troubleshooting](#troubleshooting)

---

## Overview

Real-world Philippine address data is often messy — inconsistent abbreviations, misspellings, missing components, or non-standard formats. This pipeline resolves those issues using hierarchical fuzzy matching and alias normalization, producing clean, structured address records suitable for analytics, reporting, or database ingestion.

The pipeline is built around a Jupyter Notebook workflow (recommended) with an optional `main.py` script entry point. It is designed to run entirely **locally** — no external APIs or cloud services required.

---

## Features

- Hierarchical fuzzy matching across barangay → city → province → region
- Alias/abbreviation normalization via a configurable alias rules CSV
- Batch processing with configurable input paths
- Categorized output: matched results are separated from unmatched ones, with reason-based subfolders
- Utility scripts for combining outputs and slicing large files into test batches

---

## Data Setup

> ⚠️ **The `data/` folder is not included in this repository.**

You must manually download it from the shared OneDrive link and place it under `address_matching/data/` before running the pipeline. The folder should contain the following subdirectories:

```
address_matching/data/
├── input/
├── mapping/
├── output/
└── utils/
```

📁 **OneDrive Link:** `[insert OneDrive link here]`

---

## Prerequisites

- Python 3.11 or newer
- Git
- [`uv`](https://github.com/astral-sh/uv) (Python package manager)

Install `uv` if you don't have it:

```bash
pip install uv
```

---

## Installation

1. Clone the repository:

```bash
git clone https://github.com/jv-lqgn/ph_address_matching_pipeline.git
cd ph_address_matching_pipeline
```

2. Sync dependencies using `uv`:

```bash
uv sync
```

3. Activate the virtual environment:

```bash
# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate
```

---

## Running the Pipeline

### Notebook Workflow (Recommended)

1. Open the notebook:
   ```
   address_matching/notebooks/optimized_address_pipeline.ipynb
   ```
2. Ensure the selected kernel points to `.venv`.
3. Configure your input file paths in **Cell 2** (`input_paths`).
4. Run all cells from top to bottom.

### Script Workflow (Optional)

```bash
python main.py
```

---

## Inputs

The pipeline expects the following input files to be present:

| File | Description |
|------|-------------|
| `address_matching/data/utils/ph_address_alias_extended_v3.csv` | Alias and abbreviation normalization rules |
| `address_matching/data/mapping/dim_location_20260316_v2.csv` | Canonical location mapping (barangay → city → province → region) |
| Batch input files | Configured in notebook Cell 2 via `input_paths` |

---

## Outputs

Processed records are written to:

```
address_matching/data/output/
├── matched/
│   └── hierarchical_match/           # Highest-confidence matches
└── unmatched/
    ├── province_barangay_inferred_city/
    ├── no_location_detected/
    ├── city_barangay_not_connected/
    └── ambiguous_city_for_barangay/
```

Each subfolder corresponds to a specific match outcome or failure reason, making it easy to audit and reprocess records as needed.

---

## Match Outcome Reference

| Outcome | Folder | Description |
|---------|--------|-------------|
| ✅ Hierarchical match | `matched/hierarchical_match` | Full match across all levels |
| ⚠️ Province + barangay (city inferred) | `unmatched/province_barangay_inferred_city` | City could not be confirmed directly |
| ❌ No location detected | `unmatched/no_location_detected` | No recognizable location tokens found |
| ❌ City–barangay mismatch | `unmatched/city_barangay_not_connected` | Barangay does not belong to resolved city |
| ❌ Ambiguous city | `unmatched/ambiguous_city_for_barangay` | Multiple cities match the given barangay |

---

## QA Process

After the pipeline runs, two QA steps are applied to ensure data accuracy and consistency.

### 1. Manual QA (Post-Pipeline Validation)

A manual review is conducted on all pipeline output to catch what automated matching cannot resolve.

**Process:**

- Review `order_deliveraddress` (the raw address field)
- Cross-check entries using:
  - `dim_location` (reference directory)
  - Google Search (for unclear or incomplete addresses)

**Actions Taken:**

- Populate missing fields: barangay, city/municipality, province, region
- Correct incorrectly mapped entries
- Standardize inconsistent formats

**Data Cleaning Rules Applied:**

- Remove rows with insufficient or missing information
- Remove rows where a single entry contains multiple cities
- Ensure each row corresponds to a single, valid location hierarchy

---

### 2. Duplicate Code Validation and Correction

The second QA step ensures that location codes are unique and correctly assigned across all geographic levels.

**Identified Issue (example for illustration — not limited to this):**

Duplicate region codes assigned to different regions:

| `region_code` | `region_name` |
|---------------|---------------|
| 13 | National Capital Region (NCR) |
| 13 | Cordillera Administrative Region (CAR) |

**Correction Applied** (validated against `dim_location`):

| `region_code` | `region_name` |
|---------------|---------------|
| 13 | National Capital Region (NCR) |
| 14 | Cordillera Administrative Region (CAR) |

**Scope of Validation:**

- Barangay level
- City/Municipality level
- Province level
- Region level

**Objectives:**

- Ensure uniqueness of location codes
- Maintain hierarchical consistency
- Align all values with the `dim_location` reference table

---

### QA Validation Checklist

Before finalizing output, confirm that:

- [ ] All location fields are correctly populated
- [ ] No duplicate or conflicting codes exist
- [ ] Address-to-location mapping is accurate and consistent

---

### How to Perform Manual QA

1. Open the pipeline output file and review the `order_deliveraddress` column.
2. For unclear entries, search using Google to identify the correct location.
3. Match results against `dim_location` to confirm the correct hierarchy.
4. Populate any missing barangay, city, province, or region fields.
5. Remove invalid or ambiguous records that cannot be resolved.

### How to Validate Location Codes

1. Extract location codes from the processed output file.
2. Identify duplicate entries by scanning for repeated codes (use **Ctrl + F** in Excel, or run a deduplication query).
3. Cross-check each identified code against `dim_location` to verify correctness.
4. Update incorrect codes based on the reference table.
5. Ensure each location code correctly corresponds to its geographic entity.

---

## Utility Scripts

### Combine Matched Outputs

Concatenates all files under `matched/hierarchical_match/` into a single file — the most refined, highest-confidence output:

```bash
python address_matching/scripts/combine_hierarchical_match.py
```

### Slice a File into Batches

Chops a given file into 1,000-row batches, useful for sample testing or incremental processing. Output is written to `address_matching/data/sample/`:

```bash
python address_matching/scripts/xlsx_slicer.py "<file_name>"
```

---

## Project Structure

```
ph_address_matching_pipeline/
├── address_matching/
│   ├── notebooks/
│   │   └── optimized_address_pipeline.ipynb
│   ├── scripts/
│   │   ├── combine_hierarchical_match.py
│   │   └── xlsx_slicer.py
│   └── data/                  # ⚠️ Not included — download from OneDrive
│       ├── input/
│       ├── utils/
│       ├── mapping/
│       ├── output/
│       └── sample/
├── main.py
├── pyproject.toml
├── requirements.txt
└── uv.lock
```

---

## Re-syncing Dependencies

If dependencies fall out of sync or you pull new changes:

```bash
uv sync
```

---

## Troubleshooting

| Issue | Resolution |
|-------|------------|
| Incomplete or ambiguous addresses | Cross-reference with `dim_location`; validate using Google Search; apply manual correction or remove the entry |
| Multiple location values in a single row | Split or remove the row; each row must map to exactly one location hierarchy |
| Duplicate or incorrect location codes | Identify via dedup scan, then correct against `dim_location` |
| Kernel not found in notebook | Ensure `.venv` is activated and selected as the notebook kernel |
| Missing data files | Download the `data/` folder from the shared OneDrive link and place it under `address_matching/data/` |
