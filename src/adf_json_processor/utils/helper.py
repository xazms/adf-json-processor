from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Check if a Delta table exists in the Spark catalog.

    Args:
        spark (SparkSession): The active Spark session.
        table_name (str): The name of the Delta table to check.

    Returns:
        bool: True if the Delta table exists, False otherwise.
    """
    return spark._jsparkSession.catalog().tableExists(table_name)

def get_table_version(spark: SparkSession, table_name: str) -> int:
    """
    Get the current version of the Delta table.

    Args:
        spark (SparkSession): The active Spark session.
        table_name (str): The name of the Delta table.

    Returns:
        int: The version number of the Delta table.
    """
    delta_table = DeltaTable.forName(spark, table_name)
    return delta_table.history().select("version").orderBy(F.desc("version")).first()["version"]