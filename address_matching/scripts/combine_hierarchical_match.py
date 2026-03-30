from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


TARGET_COLUMNS_TO_REMOVE = {
    "match_reason",
    "match_status",
    "source file",
    "source_file",
}

SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".csv"}


def list_input_files(input_dir: Path) -> list[Path]:
    files = [
        path
        for path in sorted(input_dir.iterdir())
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    ]

    if not files:
        raise FileNotFoundError(
            f"No supported files found in {input_dir}. "
            "Expected at least one .xlsx, .xls, or .csv file."
        )

    return files


def read_table(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path)
    return pd.read_excel(file_path)


def drop_requested_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize once so we can remove variants like "source file" and "source_file".
    normalized_to_original = {
        col.strip().lower().replace("_", " "): col for col in df.columns
    }

    columns_to_drop: list[str] = []
    for target in TARGET_COLUMNS_TO_REMOVE:
        normalized_target = target.strip().lower().replace("_", " ")
        original_name = normalized_to_original.get(normalized_target)
        if original_name is not None:
            columns_to_drop.append(original_name)

    if not columns_to_drop:
        return df

    return df.drop(columns=columns_to_drop)


def combine_files(input_dir: Path) -> pd.DataFrame:
    dataframes: list[pd.DataFrame] = []

    for file_path in list_input_files(input_dir):
        df = read_table(file_path)
        dataframes.append(df)

    combined = pd.concat(dataframes, ignore_index=True)
    combined = drop_requested_columns(combined)
    return combined


def default_input_dir() -> Path:
    project_root = Path(__file__).resolve().parents[1]
    return project_root / "data" / "output" / "matched" / "hierarchical_match"


def default_output_path(input_dir: Path) -> Path:
    return input_dir / "combined_hierarchical_match.xlsx"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Combine hierarchical match files (.xlsx/.xls/.csv), remove "
            "match_reason, match_status, and source file columns, then write a single file."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=default_input_dir(),
        help="Folder containing hierarchical match files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Output file path. Extension controls format: .xlsx or .csv. "
            "Default: <input-dir>/combined_hierarchical_match.xlsx"
        ),
    )
    return parser.parse_args()


def write_output(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    suffix = output_path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(output_path, index=False)
        return

    if suffix in {".xlsx", ".xls"}:
        df.to_excel(output_path, index=False)
        return

    raise ValueError("Output extension must be .xlsx, .xls, or .csv")


def main() -> None:
    args = parse_args()

    input_dir = args.input_dir.resolve()
    if not input_dir.exists() or not input_dir.is_dir():
        raise NotADirectoryError(f"Input directory does not exist: {input_dir}")

    output_path = (args.output or default_output_path(input_dir)).resolve()

    combined_df = combine_files(input_dir)
    write_output(combined_df, output_path)

    print(f"Input folder: {input_dir}")
    print(f"Rows written: {len(combined_df)}")
    print(f"Columns written: {len(combined_df.columns)}")
    print(f"Output file: {output_path}")


if __name__ == "__main__":
    main()
