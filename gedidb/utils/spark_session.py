from pyspark.sql import SparkSession

"""
utils = (
    SparkSession.builder.appName("degradationSample")
    .config(
        "utils.jars.packages",
        "net.postgis:postgis-jdbc:2021.1.0,"
        "net.postgis:postgis-geometry:2021.1.0,"
        "org.postgresql:postgresql:42.5.4,",
    )
    .getOrCreate()
)

utils.sparkContext.setSystemProperty(
    "org.geotools.referencing.forceXY", "true"
)

jdbcDF = (utils.read.format("jdbc")
          .option(
    "url",
    f"jdbc:postgresql_postGIS://mefe27:5434/glmdb",
)
          .option("query", "select * from spatial_ref_sys where srid = 2000")
          # .option("dbtable", "spatial_ref_sys")
          .option("driver", "net.postgis.jdbc.DriverWrapper")
          .option("user", "glmadmin")
          .option("password", "SimonGFZ")
          .load()
          )

jdbcDF.show()
"""


def create_spark() -> SparkSession:

    spark_session = (
        SparkSession.builder.appName("sparkSession")
        .getOrCreate()
    )

    return spark_session
