from future import annotations

import argparse
import json
from pathlib import Path

from parquet_analyzer.analyzer import analyze_parquet, export_analysis


def build_parser() -> argparse.ArgumentParser:
    """Create CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Analyze Parquet file metadata at row-group and column levels."
    )
    parser.add_argument("file_path", help="Path to the Parquet file")
    parser.add_argument(
        "--export",
        help="Path to export results. For CSV, file suffix is used as a prefix.",
        default=None,
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Export format",
    )
    parser.add_argument(
        "--print-columns",
        action="store_true",
        help="Print column-level analysis to stdout",
    )
    return parser


def main() -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    file_path = Path(args.file_path)

    row_groups_df, columns_df, summary = analyze_parquet(file_path)

    print("Summary:")
    print(json.dumps(summary, indent=2, default=str))

    print("\nRow group analysis:")
    print(row_groups_df.to_string(index=False))

    if args.print_columns:
        print("\nColumn analysis:")
        print(columns_df.to_string(index=False))

    if args.export:
        export_analysis(
            row_groups_df=row_groups_df,
            columns_df=columns_df,
            summary=summary,
            output_path=args.export,
            output_format=args.format,
        )
        print(f"\nResults exported to: {args.export}")


if name == "__main__":
    main()