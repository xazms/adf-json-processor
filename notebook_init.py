from pyspark.sql import SparkSession

def initialize_notebook_environment():
    """
    Initialize the notebook environment by setting up Spark, 
    and importing necessary modules and configurations.
    """
    # Initialize Spark session (if not already started)
    spark = SparkSession.builder.appName("ADF Notebook Initialization").getOrCreate()
    
    # Additional initialization or configuration steps can be added here
    
    return spark