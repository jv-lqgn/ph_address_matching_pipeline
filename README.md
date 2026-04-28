Philippine Address Matching Pipeline
---
A locally-run batch pipeline that standardizes and validates unstructured Philippine addresses. It takes raw address text as input and outputs structured, validated records with barangay, city/municipality, province, and region fields resolved.

---
Overview
---
Real-world Philippine address data is often messy — inconsistent abbreviations, misspellings, missing components, or non-standard formats. This pipeline resolves those issues using hierarchical fuzzy matching and alias normalization, producing clean, structured address records suitable for analytics, reporting, or database ingestion.
The pipeline is built around a Jupyter Notebook workflow (recommended) with an optional `main.py` script entry point. It is designed to run entirely locally — no external APIs or cloud services required.

---
Features
---
Hierarchical fuzzy matching across barangay → city → province → region
Alias/abbreviation normalization via a configurable alias rules CSV
Batch processing with configurable input paths
Categorized output: matched results are separated from unmatched ones, with reason-based subfolders
Utility scripts for combining outputs and slicing large files into test batches

---
Note: The data/ folder is not included in this repository. You must manually download it from the shared OneDrive link and place it under address_matching/data/ before running the pipeline. The folder should contain the input/, mapping/, output/, and utils/ subdirectories.
---
Prerequisites
Python 3.11 or newer
Git
`uv` (Python package manager)
Install `uv` if you don't have it:
```bash
pip install uv
```
---
Installation
Clone the repository:
```bash
git clone https://github.com/jv-lqgn/ph_address_matching_pipeline.git
cd ph_address_matching_pipeline
```
Sync dependencies using `uv`:
```bash
uv sync
```
Activate the virtual environment:
```bash
# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate
```
---
Running the Pipeline
Notebook Workflow (Recommended)
Open the notebook:
```
   address_matching/notebooks/optimized_address_pipeline.ipynb
   ```
Ensure the selected kernel points to `.venv`.
Configure your input file paths in Cell 2 (`input_paths`).
Run all cells from top to bottom.
Script Workflow (Optional)
```bash
python main.py
```
---
Inputs
The pipeline expects the following input files to be present:
File	Description
`address_matching/data/utils/ph_address_alias_extended_v3.csv`	Alias and abbreviation normalization rules
`address_matching/data/mapping/dim_location_20260316_v2.csv`	Canonical location mapping (barangay → city → province → region)
Batch input files	Configured in notebook Cell 2 via `input_paths`
---
Outputs
Processed records are written to:
```
address_matching/data/output/
├── matched/
│   └── hierarchical_match/       # Highest-confidence matches
└── unmatched/
    ├── province_barangay_inferred_city/
    ├── no_location_detected/
    ├── city_barangay_not_connected/
    └── ambiguous_city_for_barangay/
```
Each subfolder corresponds to a specific match outcome or failure reason, making it easy to audit and reprocess records as needed.
---
Utility Scripts
Combine matched outputs
Concatenates all files under `matched/hierarchical_match/` into a single file — the most refined, highest-confidence output:
```bash
python address_matching/scripts/combine_hierarchical_match.py
```
Slice a file into batches
Chops a given file into 1,000-row batches, useful for sample testing or incremental processing. Output is written to `address_matching/data/sample/`:
```bash
python address_matching/scripts/xlsx_slicer.py "<file_name>"
```
---
Project Structure
```
ph_address_matching_pipeline/
├── address_matching/
│   ├── notebooks/
│   │   └── optimized_address_pipeline.ipynb
│   ├── scripts/
│   │   ├── combine_hierarchical_match.py
│   │   └── xlsx_slicer.py
│   └── data/
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
Re-syncing Dependencies
If dependencies fall out of sync or you pull new changes:
```bash
uv sync
```
---
Match Outcome Reference
Outcome	Folder	Description
✅ Hierarchical match	`matched/hierarchical_match`	Full match across all levels
⚠️ Province + barangay (city inferred)	`unmatched/province_barangay_inferred_city`	City could not be confirmed directly
❌ No location detected	`unmatched/no_location_detected`	No recognizable location tokens found
❌ City–barangay mismatch	`unmatched/city_barangay_not_connected`	Barangay does not belong to resolved city
❌ Ambiguous city	`unmatched/ambiguous_city_for_barangay`	Multiple cities match the given barangay


