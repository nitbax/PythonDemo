from pyspark.sql import SparkSession
from pyspark.sql.types import Row

spark = SparkSession.builder.appName("RDDOperations").master("local[*]").getOrCreate()
sc = spark.sparkContext
rdd = sc.parallelize([1, "Nitin", "TL", 9])
print(rdd.first())
print(rdd.count())
print(rdd.take(2))
print(rdd.collect())
rdd1 = sc.parallelize(
    [[1, "Nitin", "TL", 9], [2, "Bhuvan", "M", 7], [3, "Deepu", "AM", 8]]
)
print(rdd1.first())
print(rdd1.count())
print(rdd1.take(2))
print(rdd1.collect())
df1 = rdd1.toDF()
df1.show()
rdd2 = sc.parallelize([Row(index=1, name="Nitin", post="TL", level=9)])
print(rdd2.collect())
df2 = rdd2.toDF()
df2.show()
rdd3 = sc.parallelize(
    [
        Row(index=1, name="Bhuvan", post="M", level=7),
        Row(index=2, name="Deepu", post="AM", level=8),
        Row(index=3, name="Nitin", post="TL", level=9),
    ]
)
print(rdd3.collect())
df3 = rdd3.toDF()
df3.show()
df4 = spark.range(4)
df4.show()
list1 = [
    (1, "Bhuvan", "M", 6),
    (2, "Deepu", "AM", 7),
    (3, "Nitin", "TL", 8),
    (4, "Sandeep", "TL", 8),
    (5, "Atul", "TL", 9),
]
df5 = spark.createDataFrame(list1, ["Id", "Name", "Post", "Level"])
df5.show()
