from sedona.spark import SedonaContext
from helper.constants import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from sedona.sql.st_functions import ST_GeoHash, ST_MakeValid, ST_H3CellIDs

def write_sedona_geoparquet():
        builder = SparkSession.\
            builder.\
            master(spark_master).\
            appName('osmparquet').\
            config('spark.driver.memory', spark_driver_memory).\
            config('spark.executor.memory', spark_executor_memory).\
            config("spark.local.dir", spark_temp_directory).\
            config("spark.jars.packages",
            'org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.5.1,'
                    'org.datasyslab:geotools-wrapper:1.5.1-28.2').\
            getOrCreate()

        sedona = SedonaContext.create(builder)
        data = sedona.read.option("recursiveFileLookup", "true").format("geoparquet").load(osm_parquet_path)
        data = data.withColumn("date", concat(lit("20"), regexp_extract(input_file_name(), "\\d+", 0)))
        data = data.withColumns({"date": to_date("date", "yyyyMMdd"),
                                "geohash": ST_GeoHash('geometry', geo_hash_level)}).\
                orderBy('geohash')
        data = data.select(col("osm_id"), col("all_tags"), col("geometry"), col("date"), col("geohash"))
        data = data.na.drop()
        data = data.withColumn("geometry", ST_MakeValid(col("geometry")))
        data.write.mode("overwrite").format("geoparquet").save(geometry_file_path)

if __name__ == "__main__":
        write_sedona_geoparquet()