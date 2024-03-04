from delta import configure_spark_with_delta_pip
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import MapType, StringType
from helper.constants import *
from sedona.spark import SedonaContext
from sedona.sql.st_functions import ST_AsText
import h3_pyspark

# initialize spark context

def osm_tags_transformation(osm_data_path:str, helper_path_directory:str) -> None:
    """_summary_

    Args:
        osm_data_path (str): _description_
        helper_path_directory (str): _description_
    """    ""
    # define configuration of tags transformation
    builder = SparkSession.\
        builder.\
        master(spark_master).\
        appName('kgcreation').\
        config('spark.driver.memory', spark_driver_memory).\
        config('spark.executor.memory', spark_executor_memory).\
        config("spark.local.dir", spark_temp_directory).\
        config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").\
        config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder, extra_packages=sedona_packages).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sedona = SedonaContext.create(spark)
    osm_data = sedona.read.format("geoparquet").load(geometry_file_path)
    h3_grid = sedona.read.format("geoparquet").load(grid_parquet_path)
    """
    Creation of tag data based OSM knowledge graph part. Based on predefined key value pair from the OpenStreetMap tags those are
    exposed into a subclass class construct. The related parts are based on the following script  where the following wiki is queried
    to determine the subclass-class OpenStreetMap tags: https://wiki.openstreetmap.org/wiki/Map_features
    """
    # expose key value pairs for OpenStreetMap
    osm_data_tags = osm_data.select("osm_id", from_json(col("all_tags"),MapType(StringType(), StringType())), "date")
    osm_data_tags = osm_data_tags.select("osm_id", explode("entries"), "date")
    osm_key_value = spark.read.format("parquet").load("helper/osm_main_tags.parquet")
    osm_key_value = osm_key_value.withColumn('Transform', lit(True))
    osm_joined_tags = osm_data_tags.\
        join(osm_key_value, ["key", "value"], how="left").\
        select(osm_data_tags.osm_id,
               osm_data_tags.key,
               osm_data_tags.value,
               osm_data_tags.date,
               ifnull(osm_key_value.Transform, lit(False)).alias("Transform"))

    osm_data_tags = osm_joined_tags.select(col("osm_id").alias("subject"),
                                           lit("rdfs:subclass").alias("predicate"),
                                           col("value").alias("object"),
                                           col("date")).\
                                           where(osm_joined_tags.Transform == True).\
                                    union(osm_joined_tags.select(col("value").alias("subject"),
                                           lit("rdfs:subclass").alias("predicate"),
                                           col("key").alias("object"),
                                           col("date")).\
                                           where(osm_joined_tags.Transform == True)).\
                                    union(osm_joined_tags.select(col("osm_id").alias("subject"),
                                           lit("key").alias("predicate"),
                                           col("value").alias("object"),
                                           col("date")).\
                                           where(osm_joined_tags.Transform == False))
    """
    Creation of geometry based exposure of OpenStreetMap based features using the related geometry to an OpenStreetMap feature.
    """
    osm_data_geometry = osm_data.select("osm_id", "geometry", "date")
    osm_data_geometry = osm_data_geometry.select(col("osm_id").alias("subject"),
                                                   lit("geo:hasGeometry").alias("predicate"),
                                                   concat(lit("geo"), col("osm_id")).alias("object"),
                                                   col("date")).\
                                            unionAll(osm_data_geometry.select(concat(lit("geo"), col("osm_id")).alias("subject"),
                                                   lit("geo:asWKT").alias("predicate"),
                                                   ST_AsText(col("geometry")).alias("object"),
                                                   col("date"))).\
                                            unionAll(osm_data_geometry.select(concat(lit("geo"), col("osm_id")).alias("subject"),
                                                   lit("rdf:type").alias("predicate"),
                                                   lit("geo:Geometry").alias("object"),
                                                   col("date")))

    """
    Repetition of the h3_grid data from the Knowledge Graph
    """
    
    """
    Creation of h3 grid to OpenStreetMap relation based on the different geometries for the respective items
    """
    osm_data.createOrReplaceTempView("osm_data")
    h3_grid.createOrReplaceTempView("h3_grid")
    contains_relation = sedona.sql("""
        SELECT h3_grid.h3_id AS subject, 'geo:sfContains' AS predicate, osm_data.osm_id AS object, osm_data.date AS date
        FROM osm_data, h3_grid
        WHERE ST_Contains(h3_grid.geometry, osm_data.geometry)
    """)
    crosses_relation = sedona.sql("""
        SELECT h3_grid.h3_id AS subject, 'geo:sfCrosses' AS predicate, osm_data.osm_id AS object, osm_data.date AS date
        FROM osm_data, h3_grid
        WHERE ST_Crosses(h3_grid.geometry, osm_data.geometry)
    """)
    equals_relation = sedona.sql("""
        SELECT h3_grid.h3_id AS subject, 'geo:sfEquals' AS predicate, osm_data.osm_id AS object, osm_data.date AS date
        FROM osm_data, h3_grid
        WHERE ST_Equals(h3_grid.geometry, osm_data.geometry)
    """)
    overlaps_relation = sedona.sql("""
        SELECT h3_grid.h3_id AS subject, 'geo:sfOverlaps' AS predicate, osm_data.osm_id AS object, osm_data.date AS date
        FROM osm_data, h3_grid
        WHERE ST_Overlaps(h3_grid.geometry, osm_data.geometry)
    """)
    touches_relation = sedona.sql("""
        SELECT h3_grid.h3_id AS subject, 'geo:sfTouches' AS predicate, osm_data.osm_id AS object, osm_data.date AS date
        FROM osm_data, h3_grid
        WHERE ST_Touches(h3_grid.geometry, osm_data.geometry)
    """)
    within_relation = sedona.sql("""
        SELECT h3_grid.h3_id AS subject, 'geo:sfWithin' AS predicate, osm_data.osm_id AS object, osm_data.date AS date
        FROM osm_data, h3_grid
        WHERE ST_Within(h3_grid.geometry, osm_data.geometry)
    """)
    covers_relation = sedona.sql("""
        SELECT h3_grid.h3_id AS subject, 'geo:ehCovers' AS predicate, osm_data.osm_id AS object, osm_data.date AS date
        FROM osm_data, h3_grid
        WHERE ST_Covers(h3_grid.geometry, osm_data.geometry)
    """)
    coveredby_relation = sedona.sql("""
        SELECT h3_grid.h3_id AS subject, 'geo:ehCoveredBy' AS predicate, osm_data.osm_id AS object, osm_data.date AS date
        FROM osm_data, h3_grid
        WHERE ST_CoveredBy(h3_grid.geometry, osm_data.geometry)
    """)
    intersects_relation = sedona.sql("""
        SELECT h3_grid.h3_id AS subject, 'geo:sfIntersects' AS predicate, osm_data.osm_id AS object, osm_data.date AS date
        FROM osm_data, h3_grid
        WHERE ST_Intersects(h3_grid.geometry, osm_data.geometry)
    """)

    # combine the different datasets together
    osm_kg_data = osm_data_tags.\
        unionAll(osm_data_geometry).\
        unionAll(contains_relation).\
        unionAll(crosses_relation).\
        unionAll(equals_relation).\
        unionAll(overlaps_relation).\
        unionAll(touches_relation).\
        unionAll(within_relation).\
        unionAll(covers_relation).\
        unionAll(coveredby_relation).\
        unionAll(intersects_relation)

    osm_kg_data.write.mode("overwrite").\
        format("delta").\
        save(kg_output_path)

if __name__ == "__main__":
    osm_tags_transformation(osm_parquet_path, helper_path_directory)
