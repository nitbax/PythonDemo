import pyspark.sql.functions as fx
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestS3Read").master("local[*]").getOrCreate()
all_df = spark.read.option("header", "True").csv(
    "C:/Users/nitbax/Trainings/all_students.csv"
)
present_df = spark.read.option("header", "True").csv(
    "C:/Users/nitbax/Trainings/students_present.csv"
)
absent_df = all_df.join(present_df, ["roll"], how="left_anti").cache()
absent_df.show()
# absent_df.coalesce(1).write.mode("overwrite").partitionBy("roll").csv("C:/Trainings/students_absent")
absent_df.coalesce(1).write.mode("overwrite").csv(
    "C:/Users/nitbax/Trainings/students_absent"
)
# Select columns on a specific condition
filtered_df = all_df.select(
    [name for name in all_df.columns if name.startswith("ro") | name.endswith("r")]
)
filtered_df.show()
all_df.select(all_df.colRegex("`^.*rol*`")).show()
# Null Safe Join
numbers_df = spark.createDataFrame(
    [("123",), ("456",), (None,), ("",)], ["numbers"]
).alias("num")
letters_df = spark.createDataFrame(
    [("123", "abc"), ("456", "def"), (None, "zzz"), ("", "hhh")],
    ["numbers", "letters"],
).alias("let")
final_df = numbers_df.join(
    letters_df, fx.col("num.numbers").eqNullSafe(fx.col("let.numbers"))
).select(fx.col("let.numbers"), fx.col("letters"))
final_df.show()
