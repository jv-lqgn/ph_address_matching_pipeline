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
7. [Pipeline Stages](#pipeline-stages)
8. [Inputs](#inputs)
9. [Outputs](#outputs)
10. [Match Tier Reference](#match-tier-reference)
11. [Post-Pipeline Validation](#post-pipeline-validation)
12. [QA Process](#qa-process)
13. [Utility Scripts](#utility-scripts)
14. [Project Structure](#project-structure)
15. [Re-syncing Dependencies](#re-syncing-dependencies)
16. [Troubleshooting](#troubleshooting)

---

## Overview

Real-world Philippine address data is often messy — inconsistent abbreviations, misspellings, missing components, or non-standard formats. This pipeline resolves those issues through a multi-stage process: noise removal, alias normalization, city detection, barangay fuzzy matching, confidence scoring, and export.

The pipeline is built around a Jupyter Notebook workflow (recommended) and is designed to run entirely **locally** — no external APIs or cloud services required.

---

## Features

- Multi-stage address parsing: junk stripping → alias normalization → city detection → barangay fuzzy matching → confidence scoring
- Right-to-left city detection with exact and fuzzy matching, plus barangay-based city fallback
- Alias/abbreviation normalization via a configurable alias rules CSV (e.g. `BRGY` → `Barangay`, `A.C` → `Angeles City`)
- Post-pipeline validation that back-checks extracted fields against the original raw address to nullify hallucinated values
- Single-file timestamped Excel export sorted by confidence score
- Match tiers: `valid`, `partial`, `invalid`

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

1. Open the notebook:
   ```
   address_matching/notebooks/ph_address_pipeline.ipynb
   ```
2. Ensure the selected kernel points to `.venv`.
3. In the **Config** cell, update the paths to point to your input file and reference data:
   ```python
   DIM_LOC_PATH = "../data/mapping/dim_location_20260421.csv"
   ALIAS_PATH   = "../data/utils/ph_address_alias_extended_v6.csv"
   INPUT_PATH   = "../data/input/your_input_file.xlsx"
   INPUT_COL    = "order_deliveraddress"
   ```
4. Optionally tune the fuzzy thresholds in the same Config cell (defaults shown):
   ```python
   CITY_FUZZY_THRESHOLD  = 85   # city name match sensitivity
   BGY_FUZZY_THRESHOLD   = 70   # barangay match sensitivity
   CONFIDENCE_VALID      = 75   # minimum score for "valid" tier
   CONFIDENCE_PARTIAL    = 45   # minimum score for "partial" tier
   ```
5. Run all cells from top to bottom.

---

## Pipeline Stages

The pipeline processes each raw address through six sequential stages.

### Stage 0 — Load Reference Data

Loads and preprocesses two reference files:

- `dim_location` — 42,000+ barangay rows with city, province, and region hierarchy
- `alias_map` — abbreviation/shorthand rules (e.g. `BRGY` → `Barangay`, `A.C` → `Angeles City`)

Several lookup structures are precomputed at this stage for performance: a city mega-regex, city subset dictionaries, and barangay exact-match and fuzzy-match lists.

---

### Stage 1 — Junk Token Stripping

Removes noise from the raw address that would hurt fuzzy matching:

- Stray punctuation (backticks, tildes, pipes)
- Phone numbers
- Landmark phrases (`near`, `beside`, `in front of`, `across`, etc.)
- Lot / Block / Unit / Floor / Room designations
- Building, subdivision, and compound names (stripped as a whole phrase including prefix words)
- Street-level designations (`Street`, `Avenue`, `Road`, `Blvd`, etc.) along with the name words preceding them

Meaningful abbreviations (e.g. `A.C` for Angeles City) are protected before stripping and restored afterward.

---

### Stage 2 — Alias Normalization

Expands shorthand and regional abbreviations to their canonical forms using the alias rules CSV. Examples:

| Raw | Normalized |
|-----|------------|
| `BRGY` | `Barangay` |
| `A.C` | `Angeles City` |
| `QC` | `Quezon City` |

After normalization, a **useless filter** short-circuits further processing for addresses that are too short, contain no recognizable location tokens, or are otherwise unresolvable — marking them as `invalid` immediately to avoid expensive fuzzy scans.

---

### Stage 3 — City Detection

Attempts to identify the city/municipality from the normalized address using two strategies:

**3a — Direct city detection** scans the address right-to-left using a precompiled mega-regex of all 1,405 known cities (sorted longest-first to avoid partial matches). It scores each candidate by match quality and token position, applies a province-role penalty when a city name is also a province name (e.g. `Bulacan`), and returns the top 3 candidates.

**3b — Barangay-based fallback** is used only when direct city detection yields nothing. It attempts to infer the city by finding a matching barangay name in the address, then looking up which city that barangay belongs to.

---

### Stage 4 — Barangay Fuzzy Matching

For each city candidate from Stage 3, the pipeline:

1. Looks up the precomputed city subset from `dim_location` (O(1) dictionary lookup)
2. Fuzzy-matches the cleaned address against all barangay names in that city using `rapidfuzz` `partial_ratio`
3. Selects the city candidate with the highest barangay match score

A **composite confidence score** is then computed:

```
composite = (0.7 × barangay_match_score) + (0.3 × city_presence_score)
```

---

### Stage 5 — Confidence Scoring and Tier Assignment

Each address is assigned a **match tier** based on its composite score:

| Tier | Condition |
|------|-----------|
| `valid` | Barangay score ≥ 70 **and** composite score ≥ 75 |
| `partial` | Composite score ≥ 45 (city resolved, barangay uncertain) |
| `invalid` | Composite score < 45, or no city detected, or useless address |

---

### Stage 6 — Export

All results are combined into a single timestamped Excel file sorted by confidence score (highest first):

```
address_matching/data/output/address_parsed_YYYYMMDD_HHMMSS.xlsx
```

The output file includes these columns for every address:

| Column | Description |
|--------|-------------|
| `order_deliveraddress` | Original raw address |
| `normalized_address` | Address after junk stripping and alias normalization |
| `barangay_code` / `barangay_name` | Matched barangay |
| `city_code` / `city_name` | Matched city/municipality |
| `province_code` / `province_name` | Inferred province |
| `region_code` / `region_name` | Inferred region |
| `bgy_match_score` | Barangay fuzzy match score (0–100) |
| `city_match_score` | City presence score (0–100) |
| `confidence_score` | Composite confidence score (0–100) |
| `match_tier` | `valid` / `partial` / `invalid` |
| `match_reason` | Reason code for the assigned tier |

---

## Post-Pipeline Validation

After the main pipeline runs, a **validation pass** is applied to back-check each extracted field against the original raw address (and its normalized form). This prevents hallucinated values from passing through undetected.

### What it checks

Each row is checked for whether `barangay_name`, `city_name`, and `province_name` actually appear in the raw or normalized address string, using both exact word-boundary matching and fuzzy fallback.

### Nullification rules

| Field | Nullified when... |
|-------|-------------------|
| `barangay_name` / `barangay_code` | Barangay cannot be found in raw or normalized address, **and** the pipeline's own `bgy_match_score` is below 70 or the tier is not `valid` |
| `city_name` / `city_code` | City cannot be found in the address **and** the pipeline's `city_match_score` is also below 60 |
| `province_name` | Never nullified on its own (it is always inferred from city). Cleared only if `city_name` is nullified |

### Accuracy scoring

A `match_accuracy_score` (0–100) is added to each row based on how many fields were verified:

| Field | Points |
|-------|--------|
| Barangay verified | 3 pts |
| City verified | 2 pts |
| Province verified | 1 pt |
| **Max total** | **6 pts → normalized to 100** |

Score bands:

| Band | Score | Meaning |
|------|-------|---------|
| High | ≥ 83.4 | All 2–3 fields verified |
| Mid | 50–83.3 | 1–2 fields verified |
| Low | < 50 | 0–1 fields verified |

---

## QA Process

After automated pipeline processing and validation, two manual QA steps are applied.

### 1. Manual QA (Post-Pipeline Validation)

A manual review is conducted on pipeline output to resolve cases that automation cannot handle.

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

Ensures that location codes are unique and correctly assigned across all geographic levels.

**Example of an identified issue:**

| `region_code` | `region_name` |
|---------------|---------------|
| 13 | National Capital Region (NCR) |
| 13 | Cordillera Administrative Region (CAR) |

**Corrected (validated against `dim_location`):**

| `region_code` | `region_name` |
|---------------|---------------|
| 13 | National Capital Region (NCR) |
| 14 | Cordillera Administrative Region (CAR) |

**Scope of Validation:** barangay, city/municipality, province, and region levels.

**Objectives:** ensure code uniqueness, maintain hierarchical consistency, and align all values with the `dim_location` reference table.

---

### QA Validation Checklist

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

If using the legacy segregated export (separate valid/partial/invalid files), this concatenates all files under `matched/hierarchical_match/` into a single file:

```bash
python address_matching/scripts/combine_hierarchical_match.py
```

### Slice a File into Batches

Chops a given file into 1,000-row batches, useful for sample testing. Output is written to `address_matching/data/sample/`:

```bash
python address_matching/scripts/xlsx_slicer.py "<file_name>"
```

---

## Project Structure

```
ph_address_matching_pipeline/
├── address_matching/
│   ├── notebooks/
│   │   └── ph_address_pipeline.ipynb
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
| Multiple location values in a single row | Remove the row; each row must map to exactly one location hierarchy |
| Duplicate or incorrect location codes | Identify via dedup scan, then correct against `dim_location` |
| Kernel not found in notebook | Ensure `.venv` is activated and selected as the notebook kernel |
| Missing data files | Download the `data/` folder from the shared OneDrive link and place it under `address_matching/data/` |
| Too many `invalid` results | Lower `CONFIDENCE_PARTIAL` or `BGY_FUZZY_THRESHOLD` in the Config cell and re-run |
| Too many false positives in `valid` | Raise `BGY_FUZZY_THRESHOLD` or `CONFIDENCE_VALID` in the Config cell |
