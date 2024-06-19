import pyspark
from pyspark.sql import SparkSession
from granule_downloader.data_handler import download_cmr_data

import sys
import os


os.environ['HADOOP_HOME'] = "C:/Users/felix/hadoop/hadoop-3.3.6"
sys.path.append("C:/Users/felix/hadoop/hadoop-3.3.6/bin")
# os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-22"
# sys.path.append("C:/Users/felix/hadoop/hadoop-3.4.0/bin")
"""
spark = (SparkSession.builder
    .appName("test")
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.4.1,"
        "org.datasyslab:geotools-wrapper:1.4.0-28.2,"
        "net.postgis:postgis-jdbc:2021.1.0,"
        "net.postgis:postgis-geometry:2021.1.0,"
        "org.postgresql:postgresql:42.5.4,",
    )
    .getOrCreate())

# rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
#rddCollect = rdd.collect()
# print("Number of Partitions: ", str(rdd.getNumPartitions()))

urls = spark.sparkContext.parallelize(['./data/test.geojson', './data/test.geojson2'])
rddCollect = urls.map(download_cmr_data).collect()




p
print(rddCollect)

"""

spark = (SparkSession.builder.appName("test").getOrCreate())

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rddCollect = rdd.collect()
print("Number of Partitions: ", str(rdd.getNumPartitions()))
print("First: ", rdd.count())
