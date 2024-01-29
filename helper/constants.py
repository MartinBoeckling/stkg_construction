"""
File containing the different constants relevant for this repository
"""
# data gathering variables
helper_path_directory = "/ceph/mboeckli/stkg/helper"
osm_data_path = "/ceph/mboeckli/stkg_comparison_data/wildfire_data/base_data/openstreetmap/california"
osm_parquet_path = "/ceph/mboeckli/stkg_comparison_data/wildfire_data/base_data/openstreetmap/california/parquet"
osm_start_date = "2010-01-01"
osm_end_date = "2022-12-31"
osm_area = "California, United States"
osm_clipping = False

# data preparation variables
spark_master = "local[*]"
spark_temp_directory = "/ceph/mboeckli/sparkTmp"
spark_driver_memory = "150G"
spark_executor_memory = "150G"
kg_output_path = "/ceph/mboeckli/stkg/gold/complete_stkg"