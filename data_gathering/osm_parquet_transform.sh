# Transform data from .pbf to geoparquet
export CPL_LOG=/dev/null
echo "Transform file: ${4}"

ogr2ogr -skipfailures -f parquet -progress -overwrite -explodecollections -makevalid --config CPL_TMPDIR $1 -oo $2 "${3}_osm_point.parquet" $4 points

ogr2ogr -skipfailures -f parquet -progress -overwrite -explodecollections -makevalid --config CPL_TMPDIR $1 -oo $2 "${3}_osm_line.parquet" $4 lines

ogr2ogr -skipfailures -f parquet -progress -overwrite -explodecollections -makevalid --config CPL_TMPDIR $1 -oo $2 "${3}_osm_multiline.parquet" $4 multilinestrings

ogr2ogr -skipfailures -f parquet -progress -overwrite -explodecollections -makevalid --config CPL_TMPDIR $1 -oo $2 "${3}_osm_multipolygon.parquet" $4 multipolygons

ogr2ogr -skipfailures -f parquet -progress -overwrite -explodecollections -makevalid --config CPL_TMPDIR $1 -oo $2 "${3}_osm_relation.parquet" $4 other_relations

