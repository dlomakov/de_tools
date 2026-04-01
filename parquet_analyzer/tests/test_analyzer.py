from future import annotations

from pathlib import Path

import pandas as pd

from parquet_analyzer.analyzer import analyze_parquet, export_analysis


def test_analyze_parquet_returns_expected_structures(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "value": ["a", "b", None, "d"],
        }
    )

    file_path = tmp_path / "sample.parquet"
    df.to_parquet(file_path, engine="pyarrow")

    row_groups_df, columns_df, summary = analyze_parquet(file_path)

    assert not row_groups_df.empty
    assert not columns_df.empty
    assert summary["total_rows"] == 4
    assert summary["num_row_groups"] >= 1
    assert "column_name" in columns_df.columns


def test_export_json_creates_file(tmp_path: Path) -> None:
    df = pd.DataFrame({"id": [1, 2], "value": ["x", "y"]})
    file_path = tmp_path / "sample.parquet"
    df.to_parquet(file_path, engine="pyarrow")

    row_groups_df, columns_df, summary = analyze_parquet(file_path)

    output_path = tmp_path / "result.json"
    export_analysis(row_groups_df, columns_df, summary, output_path, "json")

    assert output_path.exists()
    assert output_path.read_text(encoding="utf-8")


def test_export_csv_creates_files(tmp_path: Path) -> None:
    df = pd.DataFrame({"id": [1, 2], "value": ["x", "y"]})
    file_path = tmp_path / "sample.parquet"
    df.to_parquet(file_path, engine="pyarrow")

    row_groups_df, columns_df, summary = analyze_parquet(file_path)

    output_base = tmp_path / "report.csv"
    export_analysis(row_groups_df, columns_df, summary, output_base, "csv")

    assert (tmp_path / "report_row_groups.csv").exists()
    assert (tmp_path / "report_columns.csv").exists()
    assert (tmp_path / "report_summary.csv").exists()