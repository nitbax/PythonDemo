from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("check").getOrCreate()
li = [(1, 2), (3, 4), (5, 6)]
se = {(1, 2), (3, 4), (5, 6)}
sc = spark.sparkContext
rdd1 = sc.parallelize(li)
rdd2 = sc.parallelize(se)
rdd = spark.sparkContext.textFile("F:/Softwares/Python Tutorial/logFile.txt")

looking_for = "Error"
contains = rdd.filter(lambda a: a == looking_for).count() > 0
print(contains)
print(rdd1.collectAsMap())
print(rdd2.collectAsMap())
accum = sc.accumulator(0)
arr = [1, 2, 3, 4]
# sc.parallelize(arr).foreach(lambda x: accum+=x)
# df=spark.read.csv("")
# df.explain()
rddx = sc.parallelize({(1, 2), (3, 4), (3, 6)})
rddy = sc.parallelize({(3, 9)})
print(rddx.join(rddy).collect())
