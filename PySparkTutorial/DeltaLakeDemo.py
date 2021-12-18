from delta.tables import *
from pyspark.sql import SparkSession


def load_from_delta_lake_as_table(
    spark_session: SparkSession, delta_path: str
) -> DeltaTable:
    """
    Read data from s3 as delta lake table.
    """
    test_delta = DeltaTable.isDeltaTable(spark_session, delta_path)
    print(test_delta)
    delta_table = DeltaTable.forPath(spark_session, delta_path)
    return delta_table


if __name__ == "__main__":
    file_path = "C:/Users/nitbax/Desktop/TUTORIALS/SparkTutorial/"
    spark = (
        SparkSession.builder.appName("TestDelta")
        .master("local[1]")
        .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.0")
        .config("spark.sql.shuffle.partitions", 5)
        .config("spark.databricks.delta.schema.autoMerge.enabled", "True")
        .config("spark.delta.merge.repartitionBeforeWrite", "True")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .getOrCreate()
    )
    source_path = file_path + "employee.csv"
    delta_parquet_path = file_path + "emp_delta"
    delta_tables = load_from_delta_lake_as_table(spark, delta_parquet_path)
    source_df = (
        spark.read.option("header", "True")
        # .option("partitionColumn", "employeeId")
        # .option("numPartitions", "5")
        .csv(source_path)
    )
    delta_tables.alias("old").merge(
        source_df.alias("new"), "old.employeeId=new.employeeId"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    final_df = (
        spark.read.option("header", "True").format("delta").load(delta_parquet_path)
    )
    final_df.show(truncate=False)
