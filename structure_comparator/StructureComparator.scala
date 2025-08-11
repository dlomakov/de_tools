import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object StructureComparator {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Использование: StructureComparator")
      sys.exit(1)
    }

    val table1 = args(0)
    val table2 = args(1)

    val spark = SparkSession.builder()
      .appName("StructureComparator")
      .enableHiveSupport()
      .getOrCreate()

    compareTables(spark, table1, table2)

    spark.stop()
  }

  def compareTables(spark: SparkSession, table1: String, table2: String): Unit = {
    println(s"Сравнение таблиц: $table1 и $table2")

    // Сравнение схем hive таблиц
    val schema1 = spark.table(table1).schema
    val schema2 = spark.table(table2).schema
    println(s"Сравнение hive таблиц: $table1 и $table2")
    compareSchemas(schema1, schema2)

    // Сравнение полей партицирования
    val partitionCols1 = getPartitionColumns(spark, table1)
    val partitionCols2 = getPartitionColumns(spark, table2)
    println(s"Сравнение полей партицирования: $table1 и $table2")
    comparePartitionColumns(partitionCols1, partitionCols2)

    // Сравнение схем Parquet
    println(s"Сравнение схем parquet-файлов: $table1 и $table2")
    compareParquetSchemas(spark, table1, table2)
  }

  // Сравнение схем таблиц
  def compareSchemas(schema1: StructType, schema2: StructType): Unit = {
    val fields1 = schema1.fields.map(f => f.name.toLowerCase -> f.dataType.typeName).toMap
    val fields2 = schema2.fields.map(f => f.name.toLowerCase -> f.dataType.typeName).toMap

    val onlyInFirst = fields1.keySet.diff(fields2.keySet)
    val onlyInSecond = fields2.keySet.diff(fields1.keySet)

    val commonFields = fields1.keySet.intersect(fields2.keySet)
    val differingTypes = commonFields.filter(field => fields1(field) != fields2(field))

    if (onlyInFirst.nonEmpty) {
      println(s"Поля только в первой таблице:")
      onlyInFirst.foreach(field => println(s"$field: ${fields1(field)}"))
    }

    if (onlyInSecond.nonEmpty) {
      println(s"Поля только во второй таблице:")
      onlyInSecond.foreach(field => println(s"$field: ${fields2(field)}"))
    }

    if (differingTypes.nonEmpty) {
      println("Поля с разными типами данных:")
      differingTypes.foreach { field =>
        println(s"$field: ${fields1(field)} (в первой) <> ${fields2(field)} (во второй)")
      }
    }

    if (onlyInFirst.isEmpty && onlyInSecond.isEmpty && differingTypes.isEmpty) {
      println("Схемы идентичны.")
    }
  }

  // Получение партиционных колонок
  def getPartitionColumns(spark: SparkSession, table: String): Map[String, String] = {
    val describeOutput = spark.sql(s"DESCRIBE FORMATTED $table").collect().map(row => (row.getString(0), row.getString(1)))

    // Поиск блока Partition Information
    val partitionStartIndex = describeOutput.indexWhere(_._1.contains("# Partition Information"))
    val dataStartIndex = describeOutput.indexWhere(_._1.contains("# Detailed Table Information"))

    if (partitionStartIndex == -1  || dataStartIndex == -1  || partitionStartIndex >= dataStartIndex) {
      return Map.empty[String, String] // Нет партиций
    }

    // Извлечение полей партицирования
    val partitionFields = describeOutput.slice(partitionStartIndex + 2, dataStartIndex)
    partitionFields.filter { case (colName, dataType) => colName.nonEmpty && dataType.nonEmpty }
      .map { case (colName, dataType) => colName.trim.toLowerCase -> dataType.trim.toLowerCase }
      .toMap
  }

  // Сравнение полей партицирования
  def comparePartitionColumns(partitions1: Map[String, String], partitions2: Map[String, String]): Unit = {
    val onlyInFirst = partitions1.keySet.diff(partitions2.keySet)
    val onlyInSecond = partitions2.keySet.diff(partitions1.keySet)

    val commonPartitions = partitions1.keySet.intersect(partitions2.keySet)
    val differingTypes = commonPartitions.filter(field => partitions1(field) != partitions2(field))

    if (onlyInFirst.nonEmpty) {
      println("Поля партицирования только в первой таблице:")
      onlyInFirst.foreach(field => println(s"$field: ${partitions1(field)}"))
    }

    if (onlyInSecond.nonEmpty) {
      println("Поля партицирования только во второй таблице:")
      onlyInSecond.foreach(field => println(s"$field: ${partitions2(field)}"))
    }

    if (differingTypes.nonEmpty) {
      println("Поля партицирования с разными типами данных:")
      differingTypes.foreach { field =>
        println(s"$field: ${partitions1(field)} (в первой) <> ${partitions2(field)} (во второй)")
      }
    }

    if (onlyInFirst.isEmpty && onlyInSecond.isEmpty && differingTypes.isEmpty) {
      println("Поля партицирования идентичны.")
    }
  }

  /** Сравнение схем Parquet **/
  def compareParquetSchemas(spark: SparkSession, table1: String, table2: String): Unit = {
    val location1 = getTableLocation(spark, table1)
    val location2 = getTableLocation(spark, table2)

    if (location1.isEmpty || location2.isEmpty) {
      println("Не удалось получить путь к таблице.")
      return
    }

    val parquetSchema1 = spark.read.parquet(location1.get).schema
    val parquetSchema2 = spark.read.parquet(location2.get).schema

    compareSchemas(parquetSchema1, parquetSchema2)
  }

  /** Получение пути до таблицы **/
  def getTableLocation(spark: SparkSession, table: String): Option[String] = {
    try {
      Some(spark.sql(s"DESCRIBE FORMATTED $table")
        .filter("col_name = 'Location'")
        .select("data_type")
        .head()
        .getString(0))
    } catch {
      case _: Exception => None
    }
  }
}