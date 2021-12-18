from datetime import datetime, date
from typing import List

import pyspark.sql.functions as fx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, DateType, IntegerType


def calculate(id):
    acct = ACCT.select(
        fx.col("id"),
        fx.col("parent_id"),
    )
    tmp_df = acct.where(fx.col("id") == fx.lit(id)).select(fx.col("parent_id")).head(1)
    if tmp_df:
        tmp_id = tmp_df[0][0]
        if tmp_id is not None:
            return get_parent(tmp_id)
        else:
            return id
    else:
        return id


def get_parent(id):
    return calculate(id)


def get_direct_deposit_status_g_2(
    ACCT: DataFrame,
) -> DataFrame:
    acct = ACCT.select(
        fx.col("id"),
        fx.col("parent_id"),
    )
    acct.show(truncate=False)

    parent_id = get_parent("101")
    print("Parent ID:", parent_id)

    # get_parent_udf = fx.udf(get_parent, IntegerType())
    #
    # spark.udf.register("get_parent_udf", get_parent, StringType())
    # ex = "get_parent_udf(id)"

    acct.select(
        fx.col("id"),
        fx.col("parent_id"),
        get_parent_udf(fx.col("id")).alias("Original Parent")
        # fx.expr(ex).alias("Original Parent")
    ).show(truncate=False)

    # acct.withColumn(
    #     "Original Parent",
    #     fx.expr(ex)
    # ).show(truncate=False)

    return acct


def join_for_parent(
    ACCT: DataFrame,
) -> DataFrame:
    acct = ACCT.select(
        fx.col("id"),
        fx.col("parent_id"),
    )
    acct.show(truncate=False)

    itr_df = acct.select(
        fx.col("id"), fx.coalesce(fx.col("parent_id"), fx.col("id")).alias("parent_id")
    )
    itr_df.show(truncate=False)

    parent_df = ACCT.select(
        fx.col("id").alias("parent_id"),
        fx.col("parent_id").alias("true_parent"),
    )
    print(50 * "-")

    # itr_1 = acct.join(parent_df,"parent_id",how="left").select(
    #     fx.col("parent_id"),
    #     fx.col("id"),
    #     fx.col("true_parent"),
    #     fx.when(fx.col("true_parent").isNotNull(),fx.lit(1)).otherwise(fx.lit(0)).alias("non_nulls_count")
    # )
    # itr_1.show(truncate=False)
    # cleaned_itr_1 = itr_1.select(
    #     fx.col("id"),
    #     fx.coalesce(fx.col("true_parent"),fx.col("parent_id")).alias("parent_id")
    # )
    # cleaned_itr_1.show(truncate=False)
    # print(50 * "-")
    #
    #
    # itr_2 = cleaned_itr_1.join(parent_df, "parent_id", how="left").select(
    #     fx.col("parent_id"),
    #     fx.col("id"),
    #     fx.col("true_parent"),
    #     fx.when(fx.col("true_parent").isNotNull(),fx.lit(1)).otherwise(fx.lit(0)).alias("non_nulls_count")
    # )
    # itr_2.show(truncate=False)
    # cleaned_itr_2 = itr_2.select(
    #     fx.col("id"),
    #     fx.coalesce(fx.col("true_parent"), fx.col("parent_id")).alias("parent_id")
    # )
    # cleaned_itr_2.show(truncate=False)
    # print(50 * "-")
    #
    #
    # itr_3 = cleaned_itr_2.join(parent_df, "parent_id", how="left").select(
    #     fx.col("parent_id"),
    #     fx.col("id"),
    #     fx.col("true_parent"),
    #     fx.when(fx.col("true_parent").isNotNull(),fx.lit(1)).otherwise(fx.lit(0)).alias("non_nulls_count")
    # )
    # itr_3.show(truncate=False)
    # cleaned_itr_3 = itr_3.select(
    #     fx.col("id"),
    #     fx.coalesce(fx.col("true_parent"), fx.col("parent_id")).alias("parent_id")
    # )
    # cleaned_itr_3.show(truncate=False)
    # print(50 * "-")

    # itr_4 = cleaned_itr_3.join(parent_df, "parent_id", how="left").select(
    #     fx.col("parent_id"),
    #     fx.col("id"),
    #     fx.col("true_parent"),
    #     fx.when(fx.col("true_parent").isNotNull(),fx.lit(1)).otherwise(fx.lit(0)).alias("non_nulls_count")
    # )
    # itr_4.show(truncate=False)
    # cleaned_itr_4 = itr_4.select(
    #     fx.col("id"),
    #     fx.coalesce(fx.col("true_parent"), fx.col("parent_id")).alias("parent_id")
    # )
    # cleaned_itr_4.show(truncate=False)
    # print(50 * "-")

    print("Start finding parent ids")

    while True:
        itr_df = itr_df.join(parent_df, "parent_id", how="left").select(
            fx.col("parent_id"),
            fx.col("id"),
            fx.col("true_parent"),
        )
        # itr_df.show(truncate=False)
        # print(itr_df.where(fx.col("true_parent").isNotNull()).head())

        if itr_df.where(fx.col("true_parent").isNotNull()).head() is None:
            # if itr_df.where(fx.col("true_parent").isNotNull()).count() == 0:
            itr_df = itr_df.select(fx.col("id"), fx.col("parent_id"))
            print("Finished")
            itr_df.show(truncate=False)
            break
        else:
            itr_df = itr_df.select(
                fx.col("id"),
                fx.coalesce(fx.col("true_parent"), fx.col("parent_id")).alias(
                    "parent_id"
                ),
            )
            itr_df.show(truncate=False)
            print(50 * "-")

    itr_df.show(truncate=False)


if __name__ == "__main__":
    # load all data
    spark = SparkSession.builder.master("local").getOrCreate()
    # get_parent_udf = fx.udf(get_parent, IntegerType())

    ACCT = spark.read.option("header", True).csv("ACCT.csv")

    # df = get_direct_deposit_status_g_2(
    #     ACCT=ACCT,
    # )

    # join_for_parent(
    #     ACCT=ACCT,
    # )
