from pyspark.sql import SparkSession

def initialize_notebook_environment():
    """
    Initialize the Spark session and dbutils for the Databricks environment.

    Returns:
        tuple: (spark, dbutils) where spark is the active Spark session and dbutils provides access to Databricks utilities.
    """
    spark = SparkSession.builder.getOrCreate()

    # Handle dbutils initialization in Databricks notebooks
    dbutils = None
    try:
        # In Databricks, dbutils is already defined, so no import is needed
        dbutils = globals().get('dbutils') or DBUtils(spark)
    except NameError:
        # Catch the case where we're not in Databricks and dbutils is not available
        print("Warning: dbutils not found. This might not be a Databricks environment.")
    
    return spark, dbutils
