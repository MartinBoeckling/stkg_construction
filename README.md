# Construction of Spatio-Temporal Knowledge Graph based on h3 grid cells
## Introduction
OpenStreetMap provides a rich data set to operate with a geospatial database. OpenStreetMap consists of several geometries and adds for each individual object also additional metadata information. This repository provides a set of methods to construct based on a .osm.pbf file a Spatio-Temporal Knowledge Graph, which write it's structure in a Delta Table.

## Background to OpenStreetMap
The file that OpenStreetMap provides is the .osm.pbf file. It is an alternative to an XML file and provides a compressed representation of the OpenStreetMap features by a factor of 30% compared to a gzipped xml planet file. OpenStreetMap divides it geometry features into three different elements:
| OpenStreetMap features  | Vector Geometries   |
| ----------------------- | ------------------- |
| Node                    | Point               |
| Way                     | LineString, Polygon |
| Relation                | MultiLineString, MultiPolygon, MultiPoint, GeometryCollection |

Additionally to the geometry object OpenStreetMap uses map features to represent additional information associated to the individual OpenStreetMap object. those are organized in a key value pair. The key in general provides the main category, where for instance the value specifies the main category more precisely. The common tags are displayed under the following web page: https://wiki.openstreetmap.org/wiki/Map_features.
## Prerequisites
The coding within the repository has been created using Python 3.10.13, with the use of Spark 3.5.0 and Sedona 1.5.1 using the respective Python bindings. In general the script can run on a local laptop as also parts of the repository on computation clusters due to involvement of Apache Sedona and Apache Spark. It is generally recommended to use for the Planet file not the laptop as the memory footprint could be too huge.

## Data Gathering

## Data Preparation pipeline
For the data preparation pipeline the source is an osm.pbf file that is converted into a Delta Table which represents a Spatio-Temporal Knowledge Graph. In the following picture the complete pipeline for the data preparation is outlined.

**Image**

### Convert .osm.pbf file to geoparquet
### Creation of h3 grid
### Knowledge Graph creation
