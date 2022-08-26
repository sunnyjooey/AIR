import copy
import tempfile
from typing import Optional

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from osgeo import gdal, ogr
from osgeo.gdalconst import GA_ReadOnly
from rasterstats import zonal_stats
from shapely.geometry import Point

import geocoder


def extract_stats_buffer(df, raster_src, var_name, long, lat, radius, stats):
    if isinstance(df, gpd.GeoDataFrame):
        df["geometry"] = [Point(xy) for xy in zip(df[long], df[lat])]
    else:
        df = gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df[long], df[lat])])
    df["geometry"] = df.geometry.buffer(radius)

    sum_stats = zonal_stats(
        df, raster_src, geojson_out=True, all_touched=True, stats=stats
    )
    sum_stats = gpd.GeoDataFrame.from_features(sum_stats)
    sum_stats = sum_stats.rename(columns={i: f"{var_name}_{i}" for i in stats})
    return sum_stats


def reverse_geocode(row):
    try:
        g = geocoder.osm([row["Dlatitude"], row["Dlongitude"]], method="reverse")
        country_dest = g.country
    except ValueError:
        country_dest = np.nan
    try:
        g = geocoder.osm([row["Olatitude"], row["Olongitude"]], method="reverse")
        country_origin = g.country
    except ValueError:
        country_origin = np.nan
    travel_time, distance = np.nan, np.nan
    return country_dest, country_origin, travel_time, distance


def _get_xy(src, row):
    x, y = src.xy(row["row"], row["col"])
    return (x, y)


def raster_t_df(src_file, value_var, x="x", y="y", value=1):
    data = {}
    with rasterio.open(src_file) as src:
        data[value_var] = src.read(1)
        nodata = src.nodata

        data["row"], data["col"] = np.indices(data[value_var].shape)
        for i in data:
            data[i] = data[i].flatten()
        df = pd.DataFrame(data)
        if value:
            df = df.loc[df[value_var] == value]
        else:
            df = df.loc[df[value_var] != nodata]

        df[[x, y]] = df.apply(
            lambda row: _get_xy(src, row), result_type="expand", axis=1
        )
        return df


def great_circle_vec(lat1, lng1, lat2, lng2, earth_radius=6371009):
    """
    Vectorized function to calculate the great-circle distance between two points or between vectors of points.
    Parameters
    ----------
    lat1 : float or array of float
    lng1 : float or array of float
    lat2 : float or array of float
    lng2 : float or array of float
    earth_radius : numeric
        radius of earth in units in which distance will be returned (default is meters)

    Returns
    -------
    distance : float or array of float
        distance or vector of distances from (lat1, lng1) to (lat2, lng2) in units of earth_radius
    """

    phi1 = np.deg2rad(90 - lat1)
    phi2 = np.deg2rad(90 - lat2)

    theta1 = np.deg2rad(lng1)
    theta2 = np.deg2rad(lng2)

    cos = np.sin(phi1) * np.sin(phi2) * np.cos(theta1 - theta2) + np.cos(phi1) * np.cos(
        phi2
    )
    arc = np.arccos(cos)

    # return distance in units of earth_radius
    distance = arc * earth_radius
    return distance


def bbox_to_pixel_offsets(gt, bbox):
    originX = gt[0]
    originY = gt[3]
    pixel_width = gt[1]
    pixel_height = gt[5]
    x1 = int((bbox[0] - originX) / pixel_width)
    x2 = int((bbox[1] - originX) / pixel_width) + 1

    y1 = int((bbox[3] - originY) / pixel_height)
    y2 = int((bbox[2] - originY) / pixel_height) + 1

    xsize = x2 - x1
    ysize = y2 - y1
    return (x1, y1, xsize, ysize)


def custom_zonal_stats(
    vector_path, raster_path, nodata_value=None, global_src_extent=False, stats=None
):
    rds = gdal.Open(raster_path, GA_ReadOnly)
    assert rds
    rb = rds.GetRasterBand(1)
    rgt = rds.GetGeoTransform()

    if nodata_value:
        nodata_value = float(nodata_value)
        rb.SetNoDataValue(nodata_value)

    if isinstance(vector_path, str):
        vds = ogr.Open(
            vector_path, GA_ReadOnly
        )  # TODO maybe open update if we want to write stats
    else:
        temp_file = tempfile.NamedTemporaryFile(suffix=".geojson")
        with fiona.Env():
            vector_path.to_file(temp_file.name, driver="GeoJSON")
        vds = ogr.Open(temp_file.name)

    assert vds
    vlyr = vds.GetLayer(0)

    # create an in-memory numpy array of the source raster data
    # covering the whole extent of the vector layer
    if global_src_extent:
        # use global source extent
        # useful only when disk IO or raster scanning inefficiencies are your limiting factor
        # advantage: reads raster data in one pass
        # disadvantage: large vector extents may have big memory requirements
        src_offset = bbox_to_pixel_offsets(rgt, vlyr.GetExtent())
        src_array = rb.ReadAsArray(*src_offset)

        # calculate new geotransform of the layer subset
        new_gt = (
            (rgt[0] + (src_offset[0] * rgt[1])),
            rgt[1],
            0.0,
            (rgt[3] + (src_offset[1] * rgt[5])),
            0.0,
            rgt[5],
        )

    mem_drv = ogr.GetDriverByName("Memory")
    driver = gdal.GetDriverByName("MEM")

    # Loop through vectors
    zonal_stats = []
    feat = vlyr.GetNextFeature()
    while feat is not None:

        if not global_src_extent:
            # use local source extent
            # fastest option when you have fast disks and well indexed raster (ie tiled Geotiff)
            # advantage: each feature uses the smallest raster chunk
            # disadvantage: lots of reads on the source raster
            src_offset = bbox_to_pixel_offsets(rgt, feat.geometry().GetEnvelope())
            src_array = rb.ReadAsArray(*src_offset)

            # calculate new geotransform of the feature subset
            new_gt = (
                (rgt[0] + (src_offset[0] * rgt[1])),
                rgt[1],
                0.0,
                (rgt[3] + (src_offset[1] * rgt[5])),
                0.0,
                rgt[5],
            )

        # Create a temporary vector layer in memory
        mem_ds = mem_drv.CreateDataSource("out")
        mem_layer = mem_ds.CreateLayer("poly", None, ogr.wkbPolygon)
        mem_layer.CreateFeature(feat.Clone())

        # Rasterize it
        rvds = driver.Create("", src_offset[2], src_offset[3], 1, gdal.GDT_Byte)
        rvds.SetGeoTransform(new_gt)
        gdal.RasterizeLayer(rvds, [1], mem_layer, burn_values=[1])
        rv_array = rvds.ReadAsArray()

        # Mask the source data array with our current feature
        # we take the logical_not to flip 0<->1 to get the correct mask effect
        # we also mask out nodata values explictly
        masked = np.ma.MaskedArray(
            src_array,
            mask=np.logical_or(src_array == nodata_value, np.logical_not(rv_array)),
        )

        feature_stats = {}
        feature_stats["fid"] = int(feat.GetFID())
        if "min" in stats:
            feature_stats["min"] = float(masked.min())
        if "mean" in stats:
            feature_stats["mean"] = float(masked.mean())
        if "max" in stats:
            feature_stats["max"] = float(masked.max())
        if "std" in stats:
            feature_stats["std"] = float(masked.std())
        if "sum" in stats:
            feature_stats["sum"] = float(masked.sum())
        if "count" in stats:
            feature_stats["count"] = int(masked.count())
        if "median" in stats:
            feature_stats["median"] = float(np.ma.median(masked))

        zonal_stats.append(feature_stats)

        rvds = None
        mem_ds = None
        feat = vlyr.GetNextFeature()

    vds = None
    rds = None
    df = pd.DataFrame(zonal_stats)
    return df


def stats_buffer(df, raster_src, var_name, stats):
    sum_stats = custom_zonal_stats(df, raster_src, stats=stats)
    df = df.merge(sum_stats, how="outer", left_index=True, right_index=True)
    df = df.rename(columns={i: f"{var_name}_{i}" for i in stats})
    return df


def country_f_coordinates(df_count, lat, lon):
    cout = df_count.loc[df_count["geometry"].contains(Point(lon, lat)), "NAME_0"]
    try:
        return cout.tolist()[0]
    except IndexError:
        return np.nan


def rasterize_shapefile(output_raster, shape_file, pixel_size=0.00083333333333333):
    input_shp = ogr.Open(shape_file)
    shp_layer = input_shp.GetLayer()

    xmin, xmax, ymin, ymax = shp_layer.GetExtent()

    ds = gdal.Rasterize(
        output_raster,
        shape_file,
        xRes=pixel_size,
        yRes=pixel_size,
        attribute="surface_pr",
        outputBounds=[xmin, ymin, xmax, ymax],
        outputType=gdal.GDT_Int32,
    )
    ds = None
    return ds


def nested_to_record(
    ds, prefix: str = "", level: int = 0, max_level: Optional[int] = None,
):
    """
    Copied from https://github.com/pandas-dev/pandas/blob/master/pandas/io/json/_normalize.py#L31
    """
    singleton = False
    if isinstance(ds, dict):
        ds = [ds]
        singleton = True
    new_ds = []
    for d in ds:
        new_d = copy.deepcopy(d)
        for k, v in d.items():
            # each key gets renamed with prefix
            # if not isinstance(k, str):
            #     k = str(k)
            if level == 0:
                newkey = k
            else:
                newkey = (prefix, k)

            # flatten if type is dict and
            # current dict level  < maximum level provided and
            # only dicts gets recurse-flattened
            # only at level>1 do we rename the rest of the keys
            if not isinstance(v, dict) or (
                max_level is not None and level >= max_level
            ):
                if level != 0:  # so we skip copying for top level, common case
                    v = new_d.pop(k)
                    new_d[newkey] = v
                continue
            else:
                v = new_d.pop(k)
                new_d.update(nested_to_record(v, newkey, level + 1, max_level))
        new_ds.append(new_d)

    if singleton:
        return new_ds[0]
    return new_ds
