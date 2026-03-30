# Philippine Address Matching Pipeline

Batch pipeline for matching Philippine addresses into standardized barangay, city, province, and region using hierarchical fuzzy matching.

## Prerequisites

- Python 3.11 or newer
- Git
- uv

Install `uv` if needed:

```powershell
pip install uv
```

## Installation

1. Clone the repository:

```powershell
git clone https://github.com/hirajya/de_work---ms.git
cd de_work---ms
```

2. Sync dependencies:

```powershell
uv sync
```

3. Activate the virtual environment:

```powershell
.venv\Scripts\Activate.ps1
```

## Run

### Notebook workflow (recommended)

1. Open the notebook:

- `address_unmatched/notebooks/optimized_address_pipeline.ipynb`

2. Ensure the selected kernel uses `.venv`.
3. Run cells from top to bottom.

### Script workflow (optional)

```powershell
python main.py
```

## Inputs

Common input files used by the pipeline:

- Alias rules: `address_unmatched/data/utils/ph_address_alias_extended_v3.csv`
- Location mapping: `address_unmatched/data/mapping/dim_location_20260316_v2.csv`
- Batch inputs: configured in notebook Cell 2 (`input_paths`)

## Outputs

Generated outputs are written under:

- `address_unmatched/data/output/matched`
- `address_unmatched/data/output/unmatched`

Reason-based subfolders include (depending on run):

- `hierarchical_match`
- `province_barangay_inferred_city`
- `no_location_detected`
- `city_barangay_not_connected`
- `ambiguous_city_for_barangay`

## Helpful Commands

```powershell
# re-sync dependencies
uv sync

# run utility scripts
python address_unmatched/scripts/combine_hierarchical_match.py
python address_unmatched/scripts/xlsx_slicer.py
```

## Scripts
address_unmatched/scripts/

combine_hierarchical_match.py is concatenation of all files under address_unmatched/data/output/matched/hierarchical_match 
~ our most refined data 

xlsx_slicer.py chops the given file to many batches 1k rows, for sample testing
usage: python xlsx_slicer.py "{file_name}"
output is generated to address_unmatched/data/sample/