"""
File containing the different constants relevant for this repository
"""
# data gathering variables
helper_path_directory = "/ceph/mboeckli/stkg/helper"
osm_data_path = "/ceph/mboeckli/stkg/raw/"
osm_parquet_path = "/ceph/mboeckli/stkg/bronze/"
osm_start_date = "2010-01-01"
osm_end_date = "2022-12-31"
osm_area = "California, United States"
osm_clipping = False
ogr_temporary = "/ceph/mboeckli/stkg/ogrTemp"

# data preparation variables
cpu_cores = 35
spark_master = f"local[{cpu_cores}]"
spark_temp_directory = "/ceph/mboeckli/sparkTmp"
spark_driver_memory = "200G"
spark_executor_memory = "200G"
kg_output_path = "/ceph/mboeckli/stkg/gold/knowledge_graph"
grid_clipping = False
grid_level = 5
grid_compaction = False
grid_parquet_path = "/ceph/mboeckli/stkg/silver/h3_grid.parquet"
grid_kg_path = "/ceph/mboeckli/stkg_comparison_data/wildfire_data/base_data/grid/h3_grid_kg.parquet"
geo_hash_level = 3
sedona_packages = ['org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.5.1,'
                   'org.datasyslab:geotools-wrapper:1.5.1-28.2']
geometry_file_path = "/ceph/mboeckli/stkg/bronze/osm_geometry"