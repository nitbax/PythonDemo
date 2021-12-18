from pyspark.sql.session import SparkSession
import pyspark.sql.functions as fx


def main():
    spark = SparkSession.builder.appName("TestS3Read").master("local[*]").getOrCreate()
    bucketName = "test"
    s3AccessKey = "abc"
    s3SecretKey = "abcpwd"
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", s3AccessKey)
    hadoop_conf.set("fs.s3a.secret.key", s3SecretKey)
    hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
    spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    df = spark.read.option("header", True).csv("s3a://" + bucketName + "/employee.csv")
    df.show()
    df2 = df.withColumn(
        "name", fx.when(fx.col("name") == fx.lit("Nitin"), True).otherwise(False)
    )
    df2.show()


if __name__ == "__main__":
    main()
