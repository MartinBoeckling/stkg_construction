from delta import configure_spark_with_delta_pip
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import MapType, StringType
from helper.constants import *
from sedona.spark import SedonaContext
from sedona.sql.st_functions import ST_AsText
from sedona.sql.st_predicates import ST_CoveredBy, ST_Covers, ST_Intersects, ST_Contains, ST_Crosses, ST_Equals, ST_Overlaps, ST_Touches, ST_Within
import h3_pyspark

# initialize spark context

def kg_creation() -> None:
    """
    As a computational engine we use Apache Sedona to scale
    the Knowledege Graph creation process. For the Knowledge Graph
    creation we divide our creation in different parts. The first part
    transforms the tag structure of OpenStreetMap into a triple structure.
    Based on the fact whether a tag is marked as a commonly used tag, we
    transform the tag into a subclass relation for the Knowledge Graph.

    Args:
        osm_data_path (str): _description_
        helper_path_directory (str): _description_
    """    ""
    # define configuration of tags transformation
    builder = SparkSession.\
        builder.\
        master(spark_master).\
        appName('kg_creation').\
        config('spark.driver.memory', spark_driver_memory).\
        config('spark.executor.memory', spark_executor_memory).\
        config("spark.worker.cleanup.enabled", "true").\
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
                                           lit("rdf:type").alias("predicate"),
                                           col("value").alias("object"),
                                           col("date")).\
                                           where(osm_joined_tags.Transform == True).\
                                    unionAll(osm_joined_tags.select(col("value").alias("subject"),
                                           lit("rdfs:subClassOf").alias("predicate"),
                                           col("key").alias("object"),
                                           col("date")).\
                                           where(osm_joined_tags.Transform == True)).\
                                    unionAll(osm_joined_tags.select(col("osm_id").alias("subject"),
                                           col("key").alias("predicate"),
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
                                                   col("date")))

    """
    Repetition of the h3_grid data from the Knowledge Graph
    """
    if grid_compaction:
        h3_grid = h3_grid.withColumn('resolution', h3_pyspark.h3_get_resolution(col('h3_id')))
    else:
        h3_grid = h3_grid.withColumn('resolution', lit(grid_level))
        h3_grid_geometry = h3_grid.\
            select(col("h3_id").alias("subject"), lit("geo:hasGeometry").alias("predicate"), concat(lit("geoH3"), "h3_id").alias("object")).\
            unionAll(h3_grid.select(concat(lit("geoH3"), "h3_id").alias("subject"), lit("geo:asWKT").alias("predicate"), ST_AsText(col("geometry")).alias("object")))
        h3_grid.createOrReplaceTempView("h3_grid")
        
        h3_grid_neighbor = h3_grid.alias("leftH3").\
            join(h3_grid.alias("rightH3"), on=expr("ST_INTERSECTS(leftH3.geometry, rightH3.geometry) AND leftH3.h3_id != rightH3.h3_id AND leftH3.resolution = rightH3.resolution")).\
            select(col("leftH3.h3_id").alias("subject"), lit("hcf:isAdjacentTo").alias("predicate"), col("rightH3.h3_id").alias("object"))
        
        if grid_compaction:
            h3_grid_parent = h3_grid.alias("leftH3").\
                join(h3_grid.alias("rightH3"), on=expr("leftH3.h3_id != rightH3.h3_id AND leftH3.resolution < rightH3.resolution AND ST_INTERSECTS(leftH3.geometry, rightH3.geometry)")).\
                select(col("leftH3.h3_id").alias("subject"), lit("isParentCellOf").alias("predicate"), col("rightH3.h3_id").alias("object"))
            
            h3_grid_child = h3_grid_parent.\
                select(col("object").alias("subject"), lit("isChildCellOf").alias("predicate"), col("subject").alias("object"))

            h3_grid_relation = h3_grid_geometry.\
                unionAll(h3_grid_child).\
                unionAll(h3_grid_parent).\
                unionAll(h3_grid_neighbor)
        else:
            h3_grid_relation = h3_grid_geometry.\
                unionAll(h3_grid_neighbor)
        
    unique_date = osm_data.select('date').distinct().toPandas()['date'].astype('str').tolist()
    h3_grid_relation = h3_grid_relation.withColumn("date", explode(lit(unique_date)))
    """
    Creation of h3 grid to OpenStreetMap relation based on the different geometries for the respective items
    """

    intersects_data = osm_data.alias("osm_data").\
        join(h3_grid.alias("h3_grid"), on=expr("ST_Intersects(h3_grid.geometry, osm_data.geometry)")).\
        select(col("h3_grid.h3_id").alias("h3_id"), col("h3_grid.geometry").alias("h3_geometry"), col("osm_data.osm_id").alias("osm_id"), col("osm_data.geometry").alias("osm_geometry"), col("osm_data.date").alias("date"))

    intersects_relation = intersects_data.\
        select(col("h3_id").alias("subject"), lit("geo:sfIntersects").alias("predicate"), col("osm_id").alias("object"), col("date").alias("date"))

    contains_relation = intersects_data.\
        select(col("h3_id").alias("subject"), lit("geo:sfContains").alias("predicate"), col("osm_id").alias("object"), col("date").alias("date")).\
        where(ST_Contains(col("h3_geometry"), col("osm_geometry")))
    
    crosses_relation = intersects_data.\
        select(col("h3_id").alias("subject"), lit("geo:sfCrosses").alias("predicate"), col("osm_id").alias("object"), col("date").alias("date")).\
        where(ST_Crosses(col("h3_geometry"), col("osm_geometry")))

    equals_relation = intersects_data.\
        select(col("h3_id").alias("subject"), lit("geo:sfEquals").alias("predicate"), col("osm_id").alias("object"), col("date").alias("date")).\
        where(ST_Equals(col("h3_geometry"), col("osm_geometry")))

    overlaps_relation = intersects_data.\
        select(col("h3_id").alias("subject"), lit("geo:sfOverlaps").alias("predicate"), col("osm_id").alias("object"), col("date").alias("date")).\
        where(ST_Overlaps(col("h3_geometry"), col("osm_geometry")))

    touches_relation = intersects_data.\
        select(col("h3_id").alias("subject"), lit("geo:sfTouches").alias("predicate"), col("osm_id").alias("object"), col("date").alias("date")).\
        where(ST_Touches(col("h3_geometry"), col("osm_geometry")))

    within_relation = intersects_data.\
        select(col("h3_id").alias("subject"), lit("geo:sfWithin").alias("predicate"), col("osm_id").alias("object"), col("date").alias("date")).\
        where(ST_Within(col("h3_geometry"), col("osm_geometry")))

    covers_relation = intersects_data.\
        select(col("h3_id").alias("subject"), lit("geo:ehCovers").alias("predicate"), col("osm_id").alias("object"), col("date").alias("date")).\
        where(ST_Covers(col("h3_geometry"), col("osm_geometry")))

    coveredby_relation = intersects_data.\
        select(col("h3_id").alias("subject"), lit("geo:ehCoveredBy").alias("predicate"), col("osm_id").alias("object"), col("date").alias("date")).\
        where(ST_CoveredBy(col("h3_geometry"), col("osm_geometry")))

    # combine the different datasets together
    osm_kg_data = osm_data_tags.\
        unionAll(osm_data_geometry).\
        unionAll(intersects_relation).\
        unionAll(contains_relation).\
        unionAll(crosses_relation).\
        unionAll(equals_relation).\
        unionAll(overlaps_relation).\
        unionAll(touches_relation).\
        unionAll(within_relation).\
        unionAll(covers_relation).\
        unionAll(coveredby_relation).\
        unionAll(h3_grid_relation)

    osm_kg_data.write.mode("overwrite").\
        format("delta").\
        save(kg_output_path)

if __name__ == "__main__":
    kg_creation()
