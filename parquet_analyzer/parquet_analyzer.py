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
        total_nulls = 0
        total_distinct = 0
        total_columns = row_group.num_columns
        compression_efficiency = 0
        for j in range(total_columns):
            col_meta = row_group.column(j)
            col_stats = col_meta.statistics if col_meta.statistics else None

            # Суммирование статистик
            if col_stats:
                total_nulls += col_stats.null_count or 0
                total_distinct += col_stats.distinct_count or 0
            if col_meta.total_uncompressed_size:
                compression_efficiency += col_meta.total_compressed_size / col_meta.total_uncompressed_size

            # Расчет дополнительных метрик
            null_percentage = (total_nulls / (num_rows * total_columns)) * 100 if num_rows > 0 and total_columns > 0 else None
            distinct_row_ratio = (total_distinct / num_rows) if num_rows > 0 else None
            compression_efficiency = (compression_efficiency / total_columns) if total_columns > 0 else None
            
            # Добавление метрик столбца
            row_group_analysis.append({
                "Row Group": i,
                "Num Rows": num_rows,
                "Total Size (bytes)": total_size_bytes,
                "Avg Row Size (bytes)": avg_row_size,
                "Row Group Density": total_size_bytes / num_rows if num_rows > 0 else None,
                "Null Percentage": null_percentage,
                "Distinct/Row Ratio": distinct_row_ratio,
                "Skewness": num_rows / total_rows if total_rows > 0 else None,
                "Compression Efficiency": compression_efficiency
            })

    # Преобразование в DataFrame
    df_row_groups = pd.DataFrame(row_groups_analysis)

    return df_row_groups
