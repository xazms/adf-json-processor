from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

def initialize_notebook_environment():
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    return spark, dbutils