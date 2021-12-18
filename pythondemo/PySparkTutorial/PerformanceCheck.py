from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf,
    date_format,
    to_date,
    add_months,
    _create_function,
)
from pyspark.sql.types import IntegerType
import configparser


def main():
    spark = (
        SparkSession.builder.appName("firstSparkApp").master("local[*]").getOrCreate()
    )
    spark.conf.set("spark.sql.caseSensitive", False)
    config = configparser.ConfigParser()
    config.read("application.properties")
    app_prop = config["Conf"]
    bucketName = app_prop["s3BucketName"]
    s3AccessKey = app_prop["s3accessKey"]
    s3SecretKey = app_prop["s3SecretKey"]
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", s3AccessKey)
    hadoop_conf.set("fs.s3a.secret.key", s3SecretKey)
    hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
    spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    folder = "/checkSparkCode/"
    pgm_detl = spark.read.option("header", True).csv(
        "s3a://" + bucketName + folder + "pgm_detl_bk.csv"
    )
    pgm_detl_hst = spark.read.option("header", True).csv(
        "s3a://" + bucketName + folder + "pgm_detl_hst_bk.csv"
    )
    staff = spark.read.option("header", True).csv(
        "s3a://" + bucketName + folder + "staff_bk.csv"
    )
    report_date = "2019-08-01 00:00:00"
    cmBatchDisc = createCM_Batch_Disc(pgm_detl, staff, report_date)
    cmBatchDisc.show()
    cmBatchDiscHst = createCM_Batch_Disc(pgm_detl_hst, staff, report_date)
    rmBatchDisc = createRM_Batch_Disc(pgm_detl, staff, report_date)
    rmBatchDiscHst = createRM_Batch_Disc(pgm_detl_hst, staff, report_date)
    # Creating Parquet files
    cmBatchDisc.coalesce(1).write.option("header", True).mode("overwrite").parquet(
        "s3a://" + bucketName + folder + "cmBatchDiscPy"
    )
    cmBatchDiscHst.coalesce(1).write.option("header", True).mode("overwrite").parquet(
        "s3a://" + bucketName + folder + "cmBatchDiscHstPy"
    )
    rmBatchDisc.coalesce(1).write.option("header", True).mode("overwrite").parquet(
        "s3a://" + bucketName + folder + "rmBatchDiscPy"
    )
    rmBatchDiscHst.coalesce(1).write.option("header", True).mode("overwrite").parquet(
        "s3a://" + bucketName + folder + "rmBatchDiscHstPy"
    )


@udf
def to_upper(s):
    if s is not None:
        return s.upper()


def createCM_Batch_Disc(pgm_detl, staff, report_date):
    s = staff.filter(
        staff["LAST_NAME"].isNotNull()
        & to_upper(staff["LAST_NAME"]).eqNullSafe("BATCH")
    )
    pd_formatted_date = pgm_detl.withColumn(
        "CREATED_ON",
        date_format(to_date(pgm_detl["CREATED_ON"], "dd-MM-yyyy"), "yyyy-MM"),
    ).withColumn(
        "BEG_DATE", date_format(to_date(pgm_detl["BEG_DATE"], "dd-MM-yyyy"), "yyyy-MM")
    )
    lit = _create_function("lit")
    pd_formatted = pd_formatted_date.select(
        pd_formatted_date["STAT_CODE"],
        pd_formatted_date["STAT_RSN_CODE"],
        pd_formatted_date["PGM_ID"],
        pd_formatted_date["CREATED_BY"],
        pd_formatted_date["CREATED_ON"],
        pd_formatted_date["BEG_DATE"],
        lit(report_date).alias("REPORT_DATE"),
    )
    pd1 = pd_formatted.withColumn(
        "REPORT_DATE",
        add_months(
            date_format(to_date(pd_formatted["REPORT_DATE"], "yyyy-MM-dd"), "yyyy-MM"),
            1,
        ),
    )
    pd2 = pd1.withColumn(
        "REPORT_DATE", date_format(to_date(pd1["REPORT_DATE"], "yyyy-MM-dd"), "yyyy-MM")
    )
    pd = pd2.filter(
        (pd2["CREATED_ON"] == pd2["REPORT_DATE"])
        & (pd2["BEG_DATE"] == pd2["REPORT_DATE"])
        & (pd2["STAT_CODE"].eqNullSafe("DS"))
        & (pd2["STAT_RSN_CODE"].isNotNull())
        & (pd2["STAT_RSN_CODE"].isin("01", "02", "03", "SC", "SD", "SB"))
    )
    cmBatch = s.join(pd, s["ID"] == pd["CREATED_BY"], "inner").select(
        s["ID"], pd["PGM_ID"]
    )
    cmBatchShared = (
        cmBatch.withColumn("ID", cmBatch["ID"].cast(IntegerType()))
        .withColumn("PGM_ID", cmBatch["PGM_ID"].cast(IntegerType()))
        .withColumnRenamed("ID", "STAFF_ID")
        .withColumnRenamed("PGM_ID", "PID")
    )
    return cmBatchShared


def createRM_Batch_Disc(pgm_detl, staff, report_date):
    s = staff.filter(
        staff["LAST_NAME"].isNotNull()
        & to_upper(staff["LAST_NAME"]).eqNullSafe("BATCH")
    )
    pd_formatted_date = pgm_detl.withColumn(
        "CREATED_ON",
        date_format(to_date(pgm_detl["CREATED_ON"], "dd-MM-yyyy"), "yyyy-MM"),
    ).withColumn(
        "BEG_DATE", date_format(to_date(pgm_detl["BEG_DATE"], "dd-MM-yyyy"), "yyyy-MM")
    )
    lit = _create_function("lit")
    pd_formatted = pd_formatted_date.select(
        pd_formatted_date["STAT_CODE"],
        pd_formatted_date["STAT_RSN_CODE"],
        pd_formatted_date["PGM_ID"],
        pd_formatted_date["CREATED_BY"],
        pd_formatted_date["CREATED_ON"],
        pd_formatted_date["BEG_DATE"],
        lit(report_date).alias("REPORT_DATE"),
    )
    pd2 = pd_formatted.withColumn(
        "REPORT_DATE",
        date_format(to_date(pd_formatted["REPORT_DATE"], "yyyy-MM-dd"), "yyyy-MM"),
    )
    pd = pd2.filter(
        (pd2["CREATED_ON"] == pd2["REPORT_DATE"])
        & (pd2["BEG_DATE"] == pd2["REPORT_DATE"])
        & (pd2["STAT_CODE"].eqNullSafe("DS"))
        & (pd2["STAT_RSN_CODE"].isNotNull())
        & (pd2["STAT_RSN_CODE"].isin("01", "02", "03", "SC", "SD", "SB"))
    )
    rmBatch = s.join(pd, s["ID"] == pd["CREATED_BY"], "inner").select(
        s["ID"], pd["PGM_ID"]
    )
    rmBatchShared = (
        rmBatch.withColumn("ID", rmBatch["ID"].cast(IntegerType()))
        .withColumn("PGM_ID", rmBatch["PGM_ID"].cast(IntegerType()))
        .withColumnRenamed("ID", "STAFF_ID")
        .withColumnRenamed("PGM_ID", "PID")
    )
    return rmBatchShared


if __name__ == "__main__":
    main()
