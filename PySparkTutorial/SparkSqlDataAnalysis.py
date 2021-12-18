from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as fx

spark = SparkSession.builder.appName("Data_Analysis").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.caseSensitive", "False")

df = (
    spark.read.options(header="True", inferSchema="True")
    .csv("C:/Users/nitbax/Desktop/TUTORIALS/SparkTutorial/employee_1.csv")
    .alias("df")
)
df.createOrReplaceTempView("employees")
spark.sql("select * from employees").show()
spark.sql("select * from employees where careerCounsellor='Bhuvan'").show()
spark.sql("select * from employees where joining_date>='2014-01-01 00:00:00'").show()
df.createGlobalTempView("global_employees")
# All global tables are stored in global_temp database
spark.sql("select * from global_temp.global_employees").show()
window_spec1 = Window.partitionBy(fx.col("role")).orderBy(fx.col("careerLevel").desc())
df.withColumn("Rank", fx.rank().over(window_spec1)).show()
window_spec2 = (
    Window.partitionBy(fx.col("role"))
    .orderBy(fx.col("careerLevel").desc())
    .rowsBetween(-1, 0)
)
max_id = fx.max(fx.col("employeeId")).over(window_spec2)
df.select("employeeId", "name", "role", "careerLevel", max_id.alias("max_id")).show()
window_spec3 = (
    Window.partitionBy(fx.col("role"))
    .orderBy(fx.col("careerLevel").desc())
    .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)
min_id = fx.min(fx.col("employeeId")).over(window_spec3)
df.select("employeeId", "name", "role", "careerLevel", min_id.alias("min_id")).show()
