from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("test").getOrCreate())

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rddCollect = rdd.collect()
print("Number of Partitions: ", str(rdd.getNumPartitions()))
print("Count: ", rdd.count())
print("First: ", rdd.first())
