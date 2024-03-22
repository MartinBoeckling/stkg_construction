from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from helper.constants import *
from pyspark.sql import SparkSession

builder = SparkSession.\
        builder.\
        master(spark_master).\
        appName('delta_optimization').\
        config('spark.driver.memory', spark_driver_memory).\
        config('spark.executor.memory', spark_executor_memory).\
        config("spark.local.dir", spark_temp_directory).\
        config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").\
        config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").\
        config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
deltaTable = DeltaTable.forPath(spark, kg_output_path)

deltaTable.optimize().executeCompaction()
deltaTable.vacuum(0)