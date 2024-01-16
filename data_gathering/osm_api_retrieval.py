"""

"""
from OSMPythonTools.nominatim import Nominatim
from OSMPythonTools.overpass import overpassQueryBuilder, Overpass
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from tqdm import tqdm
from constants import osm_data_path, osm_start_date, osm_end_date, osm_area

def swap_xy(geom: shape) -> str:
    if geom.is_empty:
        return geom

    if geom.has_z:
        def swap_xy_coords(coords):
            for x, y, z in coords:
                yield (y, x, z)
    else:
        def swap_xy_coords(coords):
            for x, y in coords:
                yield (y, x)
    # Process coordinates from each supported geometry type
    if geom.geom_type in ('Point', 'LineString', 'LinearRing'):
        return type(geom)(list(swap_xy_coords(geom.coords)))
    elif geom.geom_type == 'Polygon':
        ring = geom.exterior
        shell = type(ring)(list(swap_xy_coords(ring.coords)))
        holes = list(geom.interiors)
        for pos, ring in enumerate(holes):
            holes[pos] = type(ring)(list(swap_xy_coords(ring.coords)))
        return type(geom)(shell, holes)
    elif geom.geom_type.startswith('Multi') or geom.geom_type == 'GeometryCollection':
        # Recursive call
        return type(geom)([swap_xy(part) for part in geom.geoms])
    else:
        raise ValueError('Type %r not recognized' % geom.geom_type)


def isvalid(geom: str) -> bool:
    try:
        shape(geom)
        return True
    except:
        return False


def crawlOpenStreetMapData(cityName: str, elementTypes: str | list, timeoutSpan: int, pathFile: str, crawlDate: str) -> None:
    nominatim = Nominatim()
    areaId = nominatim.query(cityName).areaId()
    overpass = Overpass()
    query = overpassQueryBuilder(area=areaId, elementType=elementTypes, includeGeometry=True, out='body')
    result = overpass.query(query, timeout=timeoutSpan,
                            date=crawlDate)
    resultData = [element.tags() for element in result.elements() if element.tags() is not None]
    resultDataGeometry = [{'ID': element.id(), 'geometry': element.geometry()} for element in result.elements() if element.tags() is not None]
    resultDataFrame = pd.DataFrame(resultData)
    resultDataFrameGeometry = pd.DataFrame(resultDataGeometry)
    resultDataFrame = pd.concat([resultDataFrame, resultDataFrameGeometry], axis=1)
    resultDataFrame['isValid'] = resultDataFrame['geometry'].apply(lambda x: isvalid(x))
    resultDataFrame = resultDataFrame[resultDataFrame['isValid']]
    resultDataFrame = resultDataFrame[resultDataFrame['isValid']]
    resultDataFrame = resultDataFrame.drop(columns=['isValid', 'created_by'])
    geoResultDataFrame = gpd.GeoDataFrame(resultDataFrame, geometry='geometry')
    geoResultDataFrame.geometry = geoResultDataFrame.geometry.map(swap_xy)
    geoResultDataFrame.to_parquet(pathFile)

def handleRequestOSM(startDate: str, endDate: str, dateFrequency: str, cityName: str, geometryTags: list, timeOut: int):
    dateRange = pd.date_range(start=startDate, end=endDate, freq=dateFrequency).values
    for date in tqdm(dateRange):
        date = str(date)
        crawlOpenStreetMapData(cityName, geometryTags, timeOut, f'{osm_data_path}/osm_datta_{date}.parquet', date)

if __name__ == "__main__":
    handleRequestOSM(startDate=osm_start_date, endDate=osm_end_date, dateFrequency='MS',
                    cityName=osm_area, geometryTags=['node', 'way', 'relation'], timeOut=200)