import calendar
import datetime
import gzip
import multiprocessing
import os
import tempfile

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from dateutil.relativedelta import relativedelta
from rasterio import Affine
from rasterio.crs import CRS
from rasterio.errors import RasterioIOError
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject
from rasterstats import zonal_stats

WGS84CRS = CRS.from_epsg(4326)


def mask_rasterfile(src_file, geo_mask, dst_file=None):
    """
    """
    try:
        with rasterio.open(src_file) as src:
            meta = src.meta.copy()
            masked, transform = mask(src, [geo_mask], crop=True)
    except RasterioIOError:
        with gzip.open(src_file) as gzipsrc:
            with rasterio.open(gzipsrc) as src:
                meta = src.meta.copy()
                masked, transform = mask(src, [geo_mask], crop=True)

    masked = masked.astype("float32")
    if meta["nodata"] != -9999.0:
        masked[masked == meta["nodata"]] = -9999.0
    meta.update(
        transform=transform,
        height=masked.shape[1],
        width=masked.shape[2],
        nodata=-9999.0,
        dtype="float32",
    )

    masked = np.squeeze(masked)
    if dst_file is not None:
        with rasterio.open(dst_file, "w", **meta) as dst:
            dst.write(masked.astype(meta["dtype"]), 1)
    else:
        return masked, meta


def resample_array(
    src_arry, src_meta, dst_meta, dst_file=None, resampling=Resampling.nearest
):
    src_arry = src_arry.astype("float32")
    dest_array = np.ones(shape=(dst_meta["height"], dst_meta["width"]))
    dest_array *= dst_meta["nodata"]
    reproject(
        source=src_arry,
        destination=dest_array,
        src_transform=src_meta["transform"],
        src_crs=src_meta["crs"],
        src_nodata=src_meta["nodata"],
        dst_transform=dst_meta["transform"],
        dst_crs=dst_meta["crs"],
        dst_nodata=dst_meta["nodata"],
        resampling=resampling,
    )
    if dst_file is not None:
        with rasterio.open(dst_file, "w", **dst_meta) as dst:
            dst.write(dest_array.astype(dst_meta["dtype"]), 1)
    else:
        return dest_array


def _chirps_name_f_date(date):
    month_str = date.strftime("%Y.%m")
    return f"chirps-v2.0.{month_str}.tif"


def _et_name_f_date(date):
    return f"pet_{date.year}_{date.month}.tif"


def read_data_f_given_period(data_dir, start_date, end_date, var):
    """
    """
    data_array = np.array([])

    for date in pd.date_range(start_date, end_date, freq="M"):
        if var == "chirps":
            src_file = _chirps_name_f_date(date)
        elif var == "et":
            src_file = _et_name_f_date(date)
        else:
            raise NotImplementedError

        with rasterio.open(os.path.join(data_dir, src_file)) as src:
            arr = src.read()
            try:
                arr[(arr == src.nodata) | (arr == -9999) | (arr == -32768.0)] = np.nan
            except ValueError:
                arr = arr.astype(float)
                arr[(arr == src.nodata) | (arr == -9999) | (arr == -32768.0)] = np.nan

            if data_array.shape[0] == 0:
                data_array = arr.copy()
            else:
                data_array = np.concatenate((data_array, arr), axis=0)
    return data_array


def convert_numpy_array_to_raster(array, start_date, dst_dir, meta, prefix):
    for i in range(array.shape[0]):
        date = start_date + relativedelta(months=i)
        out_arr = array[i]
        out_arr[np.isnan(out_arr)] = meta["nodata"]
        file_name = f"{prefix}_{date.year}_{date.month}.tif"
        with rasterio.open(os.path.join(dst_dir, file_name), "w", **meta) as dst:
            dst.write(out_arr.astype(meta["dtype"]), 1)


def _last_day_of_month(date):
    last_day = date.replace(day=calendar.monthrange(date.year, date.month)[1])
    return last_day.strftime("%Y-%m-%d")


def _time_filter(date_str, start_date, end_date):
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    return start_date <= date <= end_date


def _zonal_stat_single_band(
    band, shapefile_src, raster, stats, all_touched, categorical, tags
):
    temp = zonal_stats(
        shapefile_src,
        raster,
        stats=stats,
        geojson_out=True,
        band=band,
        all_touched=all_touched,
        categorical=categorical,
    )
    temp = gpd.GeoDataFrame.from_features(temp)
    temp["month"] = tags[f"band_{band}"]
    cols = [i for i in temp.columns if i != "geometry"]
    temp = temp[cols]
    return temp


def convert_rasters_to_geojson(
    src_file,
    shapefile_src,
    start_date,
    end_date,
    stats="mean",
    date_filter=False,
    all_touched=True,
    categorical=False,
):
    with rasterio.open(src_file) as src:
        tags = src.tags()
        meta = src.meta.copy()
        arr = src.read()
        count = src.count

    src_local = tempfile.NamedTemporaryFile(suffix=".tif")
    with rasterio.open(src_local.name, "w", **meta) as dst:
        dst.write(arr)

    band_list = list(range(1, count + 1))
    if date_filter:
        band_list = [
            i
            for i in band_list
            if _time_filter(tags[f"band_{i}"], start_date, end_date)
        ]

    cores = multiprocessing.cpu_count()
    p = multiprocessing.Pool(cores)
    df_list = p.starmap(
        _zonal_stat_single_band,
        [
            (band, shapefile_src, src_local.name, stats, all_touched, categorical, tags)
            for band in band_list
        ],
    )
    df = pd.concat(df_list)
    return df


def _get_spatial_dims(data_array):
    if "latitude" in data_array.dimensions and "longitude" in data_array.dimensions:
        x_dim = "longitude"
        y_dim = "latitude"
    elif "x" in data_array.dimensions and "y" in data_array.dimensions:
        x_dim = "x"
        y_dim = "y"
    elif "lat" in data_array.dimensions and "lon" in data_array.dimensions:
        x_dim = "lon"
        y_dim = "lat"
    else:
        raise KeyError

    return x_dim, y_dim


def _get_bounds(dataset):
    x_dim, y_dim = _get_spatial_dims(dataset)

    left = float(dataset[x_dim][0])
    right = float(dataset[x_dim][-1])
    top = float(dataset[y_dim][0])
    bottom = float(dataset[y_dim][-1])

    return left, bottom, right, top


def _get_shape(dataset):
    x_dim, y_dim = _get_spatial_dims(dataset)
    return dataset[x_dim].size, dataset[y_dim].size


def _get_resolution(dataset):
    left, bottom, right, top = _get_bounds(dataset)
    width, height = _get_shape(dataset)

    resolution_x = (right - left) / (width - 1)
    resolution_y = (bottom - top) / (height - 1)
    resolution = (resolution_x, resolution_y)
    return resolution


def _make_src_affine(dataset):
    bounds = _get_bounds(dataset)
    left, bottom, right, top = bounds
    src_resolution_x, src_resolution_y = _get_resolution(dataset)
    return Affine.translation(left, top) * Affine.scale(
        src_resolution_x, src_resolution_y
    )


def raster_metadata_f_dataset(data_array, crs=WGS84CRS, nodata=-9999.0):
    """
    Assume the data array is 2d
    """
    meta = {
        "transform": _make_src_affine(data_array),
        "dtype": "float32",
        "height": _get_shape(data_array)[1],
        "width": _get_shape(data_array)[0],
        "crs": crs,
        "nodata": nodata,
        "count": 1,
        "driver": "GTiff",
    }
    return meta


def read_list_of_targets(target_dict, date_list):
    for i, date in enumerate(date_list):
        data = target_dict[date].open().read()
        arr, meta = data["array"], data["meta"]

        if len(arr.shape) == 2:
            arr = arr.reshape(1, *arr.shape)
        if i == 0:
            out_arr = arr.copy()
        else:
            out_arr = np.concatenate((out_arr, arr), axis=0)
    out_arr[out_arr == meta["nodata"]] = np.nan
    return out_arr, meta
