from pyspark.sql import SparkSession
import pyspark.sql.functions as fx

spark = SparkSession.builder.appName("CheckKite").master("local[*]").getOrCreate()

code_detl = spark.read.option("header", True).csv("C:/Test Data/Report/CODE.csv")
code_detl.show()

pgm = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("C:/Test Data/Report/PGM.csv")
)
issuance = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("C:/Test Data/Report/ISSUE.csv")
)
issuance_detl = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("C:/Test Data/Report/ISSUE_DETL.csv")
)
iss_iss_detl_df = issuance.join(
    issuance_detl, fx.col("issuance.id") == fx.col("issuance_detl.issuance_id")
)
iss_iss_detl_df.explain()
