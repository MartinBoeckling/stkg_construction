from helper.constants import osm_data_path, osm_parquet_path, osm_area, osm_clipping
import subprocess
from pathlib import Path
from OSMPythonTools.nominatim import Nominatim

def transformation(folder_path: str, destination_path: str, area_information: str,
                   area_clipping: bool):
    folder_path = Path(folder_path)
    nominatim = Nominatim()
    area_id = nominatim.query(area_information).areaId()
    pbf_files = folder_path.glob("*osm.pbf")
    for pbf_file in pbf_files:
        folder_path = f'{destination_path}/{pbf_file.stem.split(".")[0]}'
        Path(folder_path).mkdir(exist_ok=True, parents=True)
        print(f"Transformation start for file: {pbf_file.stem.split('.')[0]}")
        if area_clipping:
            raise NotImplementedError
        else:
            subprocess.run(["bash", "data_gathering/osm_parquet_transform.sh", "/ceph/mboeckli/stkg/raw/ogrTemp",
                        "CONFIG_FILE=helper/osmconf.ini", folder_path, pbf_file])

if __name__ =='__main__':
    transformation(folder_path=osm_data_path, destination_path=osm_parquet_path, area_information=osm_area,
                   area_clipping=osm_clipping)