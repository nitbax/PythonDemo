from pyspark.sql import SparkSession
import pyspark.sql.functions as fx

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()


def word_count_rdd(text_file):
    rdd = spark.sparkContext.textFile(text_file)
    rdd_to_tuple = rdd.map(lambda x: x.split(" ")).reduce(lambda x: x)
    print(len(rdd_to_tuple))
    split_rdd = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))
    print(split_rdd.count())
    print(split_rdd.reduceByKey(lambda x, y: x + y).collect())


def word_count_df(text_file):
    df = spark.read.text(text_file)
    df_with_words_list = df.withColumn(
        "word_list", fx.size(fx.split(fx.col("value"), " "))
    )
    df_with_words_list.show(truncate=False)


if __name__ == "__main__":
    text_file_path = "TestFile.txt"
    # word_count_rdd(text_file_path)
    word_count_df(text_file_path)
