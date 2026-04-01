from future import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


def _safe_ratio(numerator: float | int | None, denominator: float | int | None) -> float | None:
    """Return numerator / denominator when both values are valid and denominator is not zero."""
    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator)


def _safe_percentage(numerator: float | int | None, denominator: float | int | None) -> float | None:
    """Return percentage numerator / denominator * 100 when possible."""
    ratio = _safe_ratio(numerator, denominator)
    return ratio * 100 if ratio is not None else None


def _mean_ignore_none(values: list[float | None]) -> float | None:
    """Calculate the mean of non-None values."""
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def _get_column_name(parquet_file: pq.ParquetFile, column_index: int) -> str:
    """Return a column name using Arrow schema when available."""
    return parquet_file.schema_arrow.names[column_index]


def _get_column_type(parquet_file: pq.ParquetFile, column_index: int) -> str:
    """Return a readable column type."""
    field = parquet_file.schema_arrow.field(column_index)
    return str(field.type)


def analyze_parquet(file_path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """
    Analyze Parquet file metadata at row-group and column levels.

    Returns:
        row_groups_df: One row per row group.
        columns_df: One row per column per row group.
        summary: File-level summary dictionary.
    """
    file_path = Path(file_path)
    parquet_file = pq.ParquetFile(file_path)
    metadata = parquet_file.metadata

    num_row_groups = parquet_file.num_row_groups
    total_rows = metadata.num_rows
    metadata_serialized_size = metadata.serialized_size

    row_groups_records: list[dict[str, Any]] = []
    columns_records: list[dict[str, Any]] = []

    total_compressed_size_across_row_groups = 0

    for row_group_index in range(num_row_groups):
        row_group = metadata.row_group(row_group_index)
        num_rows = row_group.num_rows
        total_size_bytes = row_group.total_byte_size

        null_percentages: list[float | None] = []
        distinct_percentages: list[float | None] = []

        row_group_compressed_sum = 0
        row_group_uncompressed_sum = 0

        for column_index in range(row_group.num_columns):
            column_meta = row_group.column(column_index)
            column_stats = column_meta.statistics

            column_name = _get_column_name(parquet_file, column_index)
            column_type = _get_column_type(parquet_file, column_index)

            compressed_size = column_meta.total_compressed_size
            uncompressed_size = column_meta.total_uncompressed_size

            row_group_compressed_sum += compressed_size or 0
            row_group_uncompressed_sum += uncompressed_size or 0

            null_count = None
            distinct_count = None
            min_value = None
            max_value = None
            has_stats = column_stats is not None

            if has_stats:
                null_count = getattr(column_stats, "null_count", None)
                distinct_count = getattr(column_stats, "distinct_count", None)
                min_value = getattr(column_stats, "min", None)
                max_value = getattr(column_stats, "max", None)

            null_percentage = _safe_percentage(null_count, num_rows)
            distinct_percentage = _safe_percentage(distinct_count, num_rows)
            compression_ratio = _safe_ratio(uncompressed_size, compressed_size)
            compression_saving_ratio = (
                1 - _safe_ratio(compressed_size, uncompressed_size)
                if _safe_ratio(compressed_size, uncompressed_size) is not None
                else None
            )
			column_memory_share = _safe_percentage(compressed_size, total_size_bytes)

            null_percentages.append(null_percentage)
            distinct_percentages.append(distinct_percentage)

            columns_records.append(
                {
                    "row_group": row_group_index,
                    "column_name": column_name,
                    "column_type": column_type,
                    "compressed_size_bytes": compressed_size,
                    "uncompressed_size_bytes": uncompressed_size,
                    "null_count": null_count,
                    "distinct_count": distinct_count,
                    "null_percentage": null_percentage,
                    "distinct_percentage": distinct_percentage,
                    "compression_ratio": compression_ratio,
                    "compression_saving_ratio": compression_saving_ratio,
                    "memory_share_in_row_group_pct": column_memory_share,
                    "has_statistics": has_stats,
                    "min_value": min_value,
                    "max_value": max_value,
                }
            )

        total_compressed_size_across_row_groups += row_group_compressed_sum

        row_groups_records.append(
            {
                "row_group": row_group_index,
                "num_rows": num_rows,
                "total_size_bytes": total_size_bytes,
                "avg_row_size_bytes": _safe_ratio(total_size_bytes, num_rows),
                "row_group_density_bytes_per_row": _safe_ratio(total_size_bytes, num_rows),
                "avg_null_percentage": _mean_ignore_none(null_percentages),
                "avg_distinct_percentage": _mean_ignore_none(distinct_percentages),
                "compressed_size_sum_bytes": row_group_compressed_sum,
                "uncompressed_size_sum_bytes": row_group_uncompressed_sum,
                "row_group_compression_ratio": _safe_ratio(row_group_uncompressed_sum, row_group_compressed_sum),
                "columns_with_statistics": sum(
                    1
                    for column_index in range(row_group.num_columns)
                    if row_group.column(column_index).statistics is not None
                ),
                "num_columns": row_group.num_columns,
            }
        )

    row_groups_df = pd.DataFrame(row_groups_records)
    columns_df = pd.DataFrame(columns_records)

    summary = {
        "file_path": str(file_path),
        "num_row_groups": num_row_groups,
        "num_columns": metadata.row_group(0).num_columns if num_row_groups > 0 else 0,
        "total_rows": total_rows,
        "metadata_serialized_size_bytes": metadata_serialized_size,
        "total_compressed_size_bytes": total_compressed_size_across_row_groups,
        "avg_row_group_size_bytes": _safe_ratio(
            sum(record["total_size_bytes"] for record in row_groups_records),
            num_row_groups,
        ),
        "avg_rows_per_row_group": _safe_ratio(total_rows, num_row_groups),
    }

    return row_groups_df, columns_df, summary


def export_analysis(
    row_groups_df: pd.DataFrame,
    columns_df: pd.DataFrame,
    summary: dict[str, Any],
    output_path: str | Path,
    output_format: str = "json",
	) -> None:
    """Export analysis results to JSON or CSV."""
    output_path = Path(output_path)
    output_format = output_format.lower()

    if output_format == "json":
        payload = {
            "summary": summary,
            "row_groups": row_groups_df.to_dict(orient="records"),
            "columns": columns_df.to_dict(orient="records"),
        }
        output_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return

    if output_format == "csv":
        if output_path.suffix:
            base = output_path.with_suffix("")
        else:
            base = output_path

        row_groups_df.to_csv(f"{base}_row_groups.csv", index=False)
        columns_df.to_csv(f"{base}_columns.csv", index=False)

        summary_df = pd.DataFrame([summary])
        summary_df.to_csv(f"{base}_summary.csv", index=False)
        return

    raise ValueError("Unsupported output format. Use 'json' or 'csv'.")