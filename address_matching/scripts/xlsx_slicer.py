from __future__ import annotations

import argparse
import math
from pathlib import Path

import polars as pl

def read_table(path: Path) -> pl.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path)
    if suffix in {".xlsx", ".xls"}:
        return pl.read_excel(path)
    raise ValueError(f"Unsupported file type: {suffix}")


def resolve_excel_path(excel_name: str, project_root: Path) -> Path:
	candidate = Path(excel_name)

	if candidate.is_file():
		return candidate.resolve()

	data_candidate = project_root / "data" / excel_name
	if data_candidate.is_file():
		return data_candidate.resolve()

	raise FileNotFoundError(
		f"Excel file not found: {excel_name}. "
		"Pass a full path or a file located under address_unmatched/data/."
	)


def split_excel_to_chunks(excel_path: Path, sample_dir: Path, chunk_size: int = 10000) -> list[Path]:
	if chunk_size <= 0:
		raise ValueError("chunk_size must be greater than 0")

	df = pl.read_excel(excel_path)
	total_rows = df.height

	output_dir = sample_dir / excel_path.stem
	output_dir.mkdir(parents=True, exist_ok=True)

	if total_rows == 0:
		empty_file = output_dir / f"{excel_path.stem}_part_001.xlsx"
		df.write_excel(empty_file)
		return [empty_file]

	num_parts = math.ceil(total_rows / chunk_size)
	output_files: list[Path] = []

	for i in range(num_parts):
		start = i * chunk_size
		chunk_df = df.slice(start, chunk_size)

		output_file = output_dir / f"{excel_path.stem}_part_{i + 1:03d}.xlsx"
		chunk_df.write_excel(output_file)
		output_files.append(output_file)

	return output_files


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Split an Excel file into smaller Excel files"
	)
	parser.add_argument(
		"excel_name",
		help=(
			"Excel filename or path. If only filename is given, "
			"the script also checks address_unmatched/data/."
		),
	)
	parser.add_argument(
		"--chunk-size",
		type=int,
		default=1000,
		help="Rows per output file (default: 1000).",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	project_root = Path(__file__).resolve().parents[1]
	sample_dir = project_root / "data" / "sample"

	excel_path = resolve_excel_path(args.excel_name, project_root)
	output_files = split_excel_to_chunks(excel_path, sample_dir, chunk_size=args.chunk_size)

	print(f"Input file: {excel_path}")
	print(f"Total chunks created: {len(output_files)}")
	print(f"Output folder: {output_files[0].parent if output_files else sample_dir}")

	for path in output_files:
		print(f"- {path.name}")


if __name__ == "__main__":
	main()
