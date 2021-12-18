from pyspark.sql import SparkSession
import pyspark.sql.functions as fx

spark = SparkSession.builder.appName("Msc").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.caseSensitive", "False")
vib = spark.read.options(header="True").csv("D:/vibgyor.csv")
vib.orderBy(
    fx.when(fx.col("color") == "Violet", 1)
    .when(fx.col("color") == "Indigo", 2)
    .when(fx.col("color") == "Blue", 3)
    .when(fx.col("color") == "Green", 4)
    .when(fx.col("color") == "Yellow", 5)
    .when(fx.col("color") == "Orange", 6)
    .when(fx.col("color") == "Red", 7)
).show()
