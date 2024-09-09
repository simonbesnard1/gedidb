from pyspark.sql import SparkSession

def create_spark():
    
    spark_session = (SparkSession.builder
             .appName("GEDI Processing")
             .config("spark.executor.instances", "4")  # Number of workers (executors)
             .config("spark.executor.cores", "4")      # Number of cores per executor
             .config("spark.executor.memory", "4g")    # Memory per executor
             .config("spark.driver.memory", "2g")      # Driver memory
             .getOrCreate())
    
    return spark_session
