# de-tools

A collection of utility tools and mini-frameworks for Data Engineering tasks within the Apache Spark and Hadoop ecosystems.
Includes tools for analyzing Hive table structures, Parquet file metadata, monitoring resource usage of applications in YARN, and estimating HDFS directory sizes.

---

## 📦 Contents

### 1. Structure_comparator
Language: Scala
Purpose: Compare the schema structures of two tables in a Hadoop/Spark environment.

Features:
- Launches a SparkSession and compares:
  1. Hive table schemas.
  2. Partitioning fields.
  3. Parquet file schemas in storage.
- Great for ensuring compatibility and identifying discrepancies between table versions.
---

### 2. Parquet_analyzer
Язык: Python (PyArrow, Pandas)  
Назначение: Анализ метаинформации Parquet-файлов.  

Возможности:
- Обрабатывает указанный Parquet-файл и выводит подробный анализ по каждому row group:
  - Количество row group.
  - Общий объём (байты).
  - Средний размер строки (байты).
  - Плотность хранения (density).
  - Количество NULL значений.
  - Кардинальность (уникальность значений).
  - Поколоночный анализ в пределах row group:
    - Эффективность компрессии.
    - Тип данных и имя поля.
    - Размер данных.
    - Доля памяти в пределах row group.

---

### 3. Yarn_api_parser
Язык: Python (Airflow, Requests)  
Назначение: Сбор и загрузка статистики о выполнении приложений из YARN API.  

Возможности:
- Airflow DAG, который:
  1. Запрашивает YARN API через requests.
  2. Парсит данные об application:
     - Время работы.
     - Интегральное потребление ресурсов:
       - vcores-seconds
       - memory-mb-seconds
  3. Записывает данные в PostgreSQL:
     - На stage слой (append-режим, все новые записи).
     - В целевую таблицу (последняя версия по application через `row_number()`).

---

### 4. HDFS_size_estimator
Язык: Python
Назначение: Подсчет кол-ва файлов в HDFS и оценка размера директории.  

Возможности:
- Сканирует указанный путь в HDFS.
- Считает кол-во файлов и подпапок внутри директории.
- Оценивает занятый объем контента, суммарный объем (с учетом фактора репликации).
- Полезно для анализа занятого места.

---

## 🚀 Установка
```bash
git clone https://github.com/dlomakov/de-tools.git
