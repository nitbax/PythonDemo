from pyspark.sql import SparkSession
import pyspark.sql.functions as fx

spark = SparkSession.builder.master("local").appName("TestIsIn").getOrCreate()
spark.conf.set("spark.sql.caseSensitive", "False")
emp_df = spark.read.options(header="True").csv(
    "C:/Users/nitbax/Desktop/TUTORIALS/SparkTutorial/employee_1.csv"
)
emp_salary_df = (
    spark.read.options(header="True")
    .csv("C:/Users/nitbax/Desktop/TUTORIALS/SparkTutorial/employeeSalary_1.csv")
    .select("employeeId")
)
# IN OR EXISTS Query and Not in query Alias
emp_df.join(emp_salary_df, ["employeeId"]).show()
emp_df.join(emp_salary_df, ["employeeId"], "left_anti").show()

# Filter the data with column containing list of values
"""trends_df = spark.read.options(header="True").csv(
    "C:/Users/nitbax/Desktop/TUTORIALS/SparkTutorial/Trends.csv"
)
l = ["Spices"]
records = trends_df.select(fx.col("trends")).where(
    (fx.concat(*[fx.regexp_extract("trends", str(val), 0) for val in l]) != ""))
records.show()"""
