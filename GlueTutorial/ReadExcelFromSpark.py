import configparser
import os
from typing import List
import glob

from pyspark.sql import SparkSession, DataFrame, functions as fx

config = configparser.RawConfigParser()
config.read("application.properties")
raw_data = config.get("Conf", "raw_data_path")
data_lake = config.get("Conf", "data_lake_path")
output_format = config.get("Conf", "output_format_file")
output = config.get("Conf", "output_path")


def read_file(path: str) -> List[str]:
    files_list = []
    for file_name in os.scandir(path):
        files_list.append(file_name.path)
    return files_list


def convert_excel_to_parquet(file_list: List[str], spark: SparkSession):
    for file in file_list:
        file_name_extension = os.path.splitext(os.path.basename(file))
        file_extension = file_name_extension[1]
        if str(file_extension).upper() == ".XLSX":
            output_file_name = file_name_extension[0]
            excel_df = (
                spark.read.format("com.crealytics.spark.excel")
                .option("useHeader", "true")
                .load(file)
            )
            for col in excel_df.columns:
                excel_df = excel_df.withColumnRenamed(
                    col, "_".join(col.strip().split())
                )
            excel_df.show(truncate=False)
            save_to_parquet(excel_df, output_file_name)


def save_to_parquet(df: DataFrame, output_file_name):
    df.write.mode("overwrite").parquet(data_lake + output_file_name)


def get_final_output(output_form_df: DataFrame, spark: SparkSession) -> DataFrame:
    output_form = output_form_df.columns
    pattern = data_lake + "**/*.parquet"
    for file_name in glob.glob(pattern, recursive=True):
        if os.path.isfile(file_name):
            input_df = spark.read.format("parquet").load(file_name)
            df_schema = input_df.columns
            col_to_select = list(set(output_form) & set(df_schema))
            if not col_to_select:
                continue
            input_df = input_df.select(
                [
                    fx.col(name) if name in col_to_select else fx.lit(None).alias(name)
                    for name in output_form
                ]
            )
            output_form_df = output_form_df.unionByName(input_df)

    return output_form_df


if __name__ == "__main__":
    spark_session = (
        SparkSession.builder.master("local")
        .appName("Glue_POC")
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.11.1")
        .getOrCreate()
    )
    output_format_df = (
        spark_session.read.format("com.crealytics.spark.excel")
        .option("useHeader", "true")
        .load(output_format)
    )
    for col in output_format_df.columns:
        output_format_df = output_format_df.withColumnRenamed(
            col, "_".join(col.strip().split())
        )
    files = read_file(raw_data)
    convert_excel_to_parquet(file_list=files, spark=spark_session)
    final_df = get_final_output(output_format_df, spark_session)
    final_df.show(truncate=False)
    print(final_df.count())
    final_df.coalesce(1).write.mode("overwrite").parquet(output)
    spark_session.read.format("parquet").load(output + "/*.parquet").show()
