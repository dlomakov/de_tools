import pyarrow.parquet as pq
import pandas as pd
import numpy as np

def analyze_parquet_row_groups(file_path):
    # Чтение метаинформации Parquet-файла
    parquet_file = pq.ParquetFile(file_path)
    
    # Получение общего количества row groups
    num_row_groups = parquet_file.num_row_groups
    total_rows = parquet_file.metadata.num_rows
    total_size = parquet_file.metadata.serialized_size

    # Список для хранения информации о row groups
    row_groups_analysis = []

    # Анализ каждого row group
    for i in range(num_row_groups):
        row_group = parquet_file.metadata.row_group(i)
        num_rows = row_group.num_rows
        total_size_bytes = row_group.total_byte_size
        avg_row_size = total_size_bytes / num_rows if num_rows > 0 else None

        # Метрики для всех столбцов row group
        columns_analysis = []
        for j in range(row_group.num_columns):
            col_meta = row_group.column(j)
            col_stats = col_meta.statistics if col_meta.statistics else None
            col_name = parquet_file.schema.column(j).name
            col_type = parquet_file.schema.column(j).physical_type
            col_size = col_meta.total_compressed_size
            col_uncompressed_size = col_meta.total_uncompressed_size if col_meta.total_uncompressed_size else None
            null_count = col_stats.null_count if col_stats else None
            distinct_count = col_stats.distinct_count if col_stats else None

            # Расчет метрик по столбцу
            null_percentage = (null_count / num_rows * 100) if null_count is not None and num_rows > 0 else None
            distinct_percentage = (distinct_count / num_rows * 100) if distinct_count is not None and num_rows > 0 else None
            compression_efficiency = (col_size / col_uncompressed_size) if col_uncompressed_size else None
            column_memory_share = (col_size / total_size_bytes * 100) if total_size_bytes > 0 else None

            # Добавление метрик столбца
            columns_analysis.append({
                "Column Name": col_name,
                "Column Type": col_type,
                "Column Size (bytes)": col_size,
                "Null Percentage": null_percentage,
                "Distinct Percentage": distinct_percentage,
                "Compression Efficiency": compression_efficiency,
                "Memory Share in Row Group": column_memory_share
            })

        # Итоговые метрики для row group
        total_nulls = sum([col["Null Percentage"] or 0 for col in columns_analysis]) / len(columns_analysis) if columns_analysis else None
        distinct_row_ratio = sum([col["Distinct Percentage"] or 0 for col in columns_analysis]) / len(columns_analysis) if columns_analysis else None

        row_groups_analysis.append({
            "Row Group": i,
            "Num Rows": num_rows,
            "Total Size (bytes)": total_size_bytes,
            "Avg Row Size (bytes)": avg_row_size,
            "Row Group Density": total_size_bytes / num_rows if num_rows > 0 else None,
            "Null Percentage (Avg)": total_nulls,
            "Distinct/Row Ratio (Avg)": distinct_row_ratio,
            "Columns Analysis": pd.DataFrame(columns_analysis),
        })

    # Преобразование в DataFrame
    row_groups_df = pd.DataFrame(row_groups_analysis)

    # Итоговая статистика
    summary = {
        "Total Row Groups": num_row_groups,
        "Total Rows": total_rows,
        "Total Size (bytes)": total_size,
        "Avg Row Group Size (bytes)": total_size / num_row_groups if num_row_groups > 0 else None,
        "Avg Rows per Row Group": total_rows / num_row_groups if num_row_groups > 0 else None,
    }

    return row_groups_df, summary

# Параметры, вызов функции
file_path = "data.parquet"  # Путь к parquet-файлу
row_groups_df, summary = analyze_parquet_row_groups(file_path)

# Вывод информации о row groups в формате DataFrame
print("Row Groups Analysis:")
print(row_groups_df)

# Для анализа колонок в конкретном row group
for idx, row in row_groups_df.iterrows():
print(f"\nRow Group {row['Row Group']} Columns Analysis:")
    print(row["Columns Analysis"])

# Вывод общей статистики
print("\nSummary:")
for key, value in summary.items():
    print(f"{key}: {value}")