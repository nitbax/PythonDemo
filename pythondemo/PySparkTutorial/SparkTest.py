from pyspark.sql import SparkSession
import pyspark.sql.functions as fx

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()


def word_count_rdd(text_file):
    """Word Count Examples using Spark RDD"""
    rdd = spark.sparkContext.textFile(text_file)
    # Total word count by converting rdd to tuple
    rdd_to_tuple = rdd.map(lambda x: x.split(" ")).reduce(lambda x: x)
    print(len(rdd_to_tuple))
    # Total word count using map and reduce functions
    split_rdd = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1))
    print(split_rdd.count())
    # Word counts by their their name
    print(split_rdd.reduceByKey(lambda x, y: x + y).collect())


def word_count_df(text_file):
    """Word Count Examples using Spark DataFrame"""
    df = spark.read.text(text_file).cache()
    df.show(truncate=False)
    # Total word count in a file
    df_words_total_count = df.withColumn(
        "total_word_count", fx.size(fx.split(fx.col("value"), " "))
    )
    df_words_total_count.show(truncate=False)
    # Total word count in a file per word in order of the frequency of the occurrence
    df_with_words_count = (
        df.withColumn("word", fx.explode(fx.split(fx.col("value"), " ")))
        .groupBy(fx.col("word"))
        .count()
        .sort(fx.col("count"), ascending=False)
    )
    df_with_words_count.show(truncate=False)


if __name__ == "__main__":
    text_file_path = "TestFile.txt"
    # word_count_rdd(text_file_path)
    word_count_df(text_file_path)
