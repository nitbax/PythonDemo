from pyspark.sql.session import SparkSession
import configparser

config = configparser.ConfigParser()
config.read("application.properties")
app_prop = config["Conf"]
user = app_prop["user"]
pwd = app_prop["password"]
driver = app_prop["driver"]
jdbcUrl = app_prop["jdbcUrl"]
spark = SparkSession.builder.appName("DBCheck").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.caseSensitive", False)
prop = {"user": user, "password": pwd, "driver": driver}
df = spark.read.jdbc(url=jdbcUrl, table="employee", properties=prop)
df.show()
df.printSchema()
code_detl = spark.read.option("header", True).csv("C:/Test Data/Report/CODE_DETL.csv")
code_detl.write.jdbc(
    url="jdbc:mysql://localhost:3306/spark_transformation",
    table="code_detl",
    mode="append",
    properties=prop,
)
# df.write.jdbc(url="jdbc:mysql://localhost:3306/spark_transformation",table="employee",mode="append",properties=prop)
bucketName = app_prop["s3BucketName"]
s3AccessKey = app_prop["s3accessKey"]
s3SecretKey = app_prop["s3SecretKey"]
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", s3AccessKey)
hadoop_conf.set("fs.s3a.secret.key", s3SecretKey)
hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
df.write.option("header", True).mode("overwrite").csv(
    "s3a://" + bucketName + "/pyspark/emp"
)
df1 = spark.read.option("header", True).csv("s3a://" + bucketName + "/employee.csv")
df1.show()
df.write.option("header", True).mode("overwrite").parquet(
    "s3a://" + bucketName + "/pyspark/empParquet"
)
df2 = spark.read.option("header", True).parquet(
    "s3a://" + bucketName + "/pyspark/empParquet"
)
df2.show()
df.printSchema()
