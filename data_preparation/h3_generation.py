"""
Title: Generate h3 grid with flexible configuration options
Description: 

- Input:
    - grid_level: Level of h3 grid cell as integer in range 0 to 15 (Default: 9) Background on grid level can be found here: https://h3geo.org/docs/core-library/restable
    - grid_compaction: Boolean value whether the selection of grid cells should be compacted or not. Compaction background can be found here: https://h3geo.org/docs/highlights/indexing/
"""
# import packages
import h3
import geopandas as gpd
import json
import pandas as pd
from multiprocessing import Pool
from tqdm import tqdm
from shapely.geometry import Polygon
from shapely.geometry import box
from sedona.spark import SedonaContext
from sedona.sql.st_functions import ST_GeoHash
from helper.constants import *
from OSMPythonTools.nominatim import Nominatim
"""

"""
def extract_h3_grid(world_data_feature: dict) -> dict:
    """_summary_

    Args:
        world_data_feature (dict): Dictionary that contains country properties as a nested
        dictionary together with the respective geometry of a country

    Returns:
        dict: _description_
    """
    properties = world_data_feature['properties']
    geometry = world_data_feature['geometry']
    h3_id = h3.polyfill_geojson(geometry, res=grid_level)
    if grid_compaction:
        h3_id = h3.compact(h3_id)
    row_dict = {'country': properties['admin'], 'continent': properties['continent'], 'subregion': properties['subregion'], 'h3_id': list(h3_id), 'land_geometry': str(geometry)}
    return row_dict

def create_h3_grid() -> None:
    world_data = gpd.read_parquet('/ceph/mboeckli/worlddata/world_data.parquet')
    world_data = world_data.explode(ignore_index=True)
    nominatim = Nominatim()
    area_parameter = nominatim.query(osm_area).toJSON()[0].get('boundingbox')
    if grid_clipping:
        area_bbox = box(miny=float(area_parameter[0]), maxy=float(area_parameter[1]), minx=float(area_parameter[2]), maxx=float(area_parameter[3]))
        world_data = world_data.clip(area_bbox)
    world_data = json.loads(world_data.to_json())
    world_data = world_data['features']
    with Pool(cpu_cores) as pool:
        row_list = list(tqdm(pool.imap_unordered(extract_h3_grid, world_data), total=len(world_data)))
    data = pd.DataFrame.from_records(row_list)
    data = data.explode(column='h3_id')
    data = data.drop('land_geometry', axis=1)
    data = data.dropna()
    data['geometry'] = data['h3_id'].apply(lambda h3Id: h3.h3_to_geo_boundary(h=h3Id, geo_json=True))
    data['geometry'] = data['geometry'].apply(Polygon)
    gdf_data = gpd.GeoDataFrame(data, geometry="geometry")
    gdf_data = gdf_data.reset_index(drop=True)
    config = SedonaContext.builder().\
        master(spark_master).\
        appName('h3generation').\
        config('spark.driver.memory', spark_driver_memory).\
        config('spark.executor.memory', spark_executor_memory).\
        config('spark.jars.packages',
            'org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.5.1,'
            'org.datasyslab:geotools-wrapper:1.5.1-28.2').\
        getOrCreate()
    sedona = SedonaContext.create(config)
    sedona_df = sedona.createDataFrame(gdf_data)
    sedona_df = sedona_df.withColumn('geohash', ST_GeoHash('geometry', geo_hash_level)).orderBy('geohash')
    sedona_df.write.mode('overwrite').format('geoparquet').save(grid_parquet_path)

if __name__ == "__main__":
    create_h3_grid()