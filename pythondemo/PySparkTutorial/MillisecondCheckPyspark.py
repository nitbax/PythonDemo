from pyspark.sql import SparkSession
import pyspark.sql.functions as fx
from pyspark.sql.types import TimestampType

spark = SparkSession.builder.appName("Msc").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.caseSensitive", "False")
pd = (
    spark.read.options(header="True")
    .csv("C:/Code Review/case_sql_task/pd.csv")
    .select(
        fx.col("PGM_ID"),
        fx.col("STAT_CODE"),
        fx.col("BEG_DATE"),
        fx.col("END_DATE"),
        fx.col("CREATED_ON"),
        fx.col("STAT_RSN_CODE"),
        fx.col("CREATED_BY"),
    )
    .alias("pd")
)
pdh = (
    spark.read.options(header="True")
    .csv("C:/Code Review/case_sql_task/pdh.csv")
    .select(
        fx.col("PGM_ID"),
        fx.col("BEG_DATE"),
        fx.col("END_DATE"),
        fx.col("STAT_CODE"),
        fx.col("STAT_RSN_CODE"),
        fx.col("CREATED_ON"),
        fx.col("CREATED_BY"),
        fx.col("UPDATED_ON"),
        fx.col("TS"),
    )
    .alias("pdh")
)
pd_ph = pd.join(pdh, fx.col("pd.PGM_ID") == fx.col("pdh.PGM_ID")).where(
    fx.col("pd.STAT_CODE").eqNullSafe("DS")
)
p1 = pd_ph.where(fx.col("pd.CREATED_ON").__gt__(fx.col("pdh.CREATED_ON"))).select(
    "pd.PGM_ID", "pd.CREATED_ON", "pdh.CREATED_ON", "pd.STAT_CODE"
)
p1.show(truncate=False)
p2 = pd_ph.withColumn(
    "timeStamp", fx.col("pd.CREATED_ON").cast(TimestampType())
).select(fx.col("timeStamp"))
p2.show(truncate=False)
p2.printSchema()
