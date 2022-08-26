import datetime
import os
import warnings

import dask
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from affine import Affine
from osgeo import gdal
from rasterio.crs import CRS
from rasterio.features import shapes
from rasterio.io import MemoryFile
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject

import rioxarray  # noqa: F401 - Xarray wrapper

CSR_FROM_EPSG = CRS.from_epsg(4326)


def calculate_stat_f_netcdf(src_file, stats=None, replace_negative=False):
    """
    Calculate statistic from netcdf file
    """
    ds_src = gdal.Open(src_file)
    band = ds_src.GetRasterBand(1)
    nodata = band.GetNoDataValue()
    scale = band.GetScale()
    offset = band.GetOffset()
    xsize = ds_src.RasterXSize
    ysize = ds_src.RasterYSize
    transform = Affine.from_gdal(*ds_src.GetGeoTransform())
    data = ds_src.ReadAsArray()
    if scale and offset:
        data = np.where(data == nodata, nodata, ((data * scale) + offset))

    data = data.astype("float32")
    data[data == nodata] = np.nan

    if replace_negative:
        data[data < 0] = 0
    if stats is None:
        data = data
    elif stats == "mean":
        data = np.nanmean(data, axis=0)
    elif stats == "sum":
        data = np.nansum(data, axis=0)
    else:
        raise NotImplementedError
    data[np.isnan(data)] = nodata
    return data, transform, ysize, xsize, nodata


def raster_metadata(
    transform,
    ysize,
    xsize,
    nodata=-9999.0,
    crs=CSR_FROM_EPSG,
    dtype="float32",
    count=1,
    driver="GTiff",
):
    meta = {
        "driver": driver,
        "dtype": dtype,
        "count": count,
        "height": ysize,
        "width": xsize,
        "transform": transform,
        "crs": crs,
        "nodata": nodata,
    }
    return meta


def extract_date(src_file):
    name = os.path.basename(src_file).replace(".nc", "")
    date_str = "_".join(name.split("_")[-3:])
    try:
        datetime.datetime.strptime(date_str, "%Y_%m_%d")
    except ValueError:
        date_str = "_".join(name.split("_")[-4:-1])
    return date_str


def scale_raster_to_match_another(
    src_file,
    target_file=None,
    dst_file=None,
    resampling=Resampling.nearest,
    src_meta=None,
    target_meta=None,
    save=True,
):
    if isinstance(src_file, str):
        src = rasterio.open(src_file)
        source = src.read(1)
        src_meta = src.meta.copy()
    else:
        source = src_file
        src_meta = src_meta
    if isinstance(target_file, str):
        dst = rasterio.open(target_file)
        dst_meta = dst.meta.copy()
    else:
        dst_meta = target_meta

    destination = (
        np.ones(shape=(dst_meta["height"], dst_meta["width"])) * src_meta["nodata"]
    )
    reproject(
        source=source,
        destination=destination,
        src_transform=src_meta["transform"],
        src_crs=src_meta["crs"],
        src_nodata=src_meta["nodata"],
        dst_transform=dst_meta["transform"],
        dst_crs=dst_meta["crs"],
        dst_nodata=src_meta["nodata"],
        resampling=resampling,
    )
    src_meta.update(
        transform=dst_meta["transform"],
        width=dst_meta["width"],
        height=dst_meta["height"],
        crs=dst_meta["crs"],
    )
    src, dst = None, None
    if save:
        with rasterio.open(dst_file, "w", **src_meta) as dst:
            dst.write(destination.astype(src_meta["dtype"]), 1)
    else:
        return destination, src_meta


def sample_features(df, num_features, bins=10):

    df["bins"] = pd.cut(df["river_discharge"], bins)

    first_bin = df["bins"].unique()[0]

    df1 = df.loc[df["bins"] == first_bin]
    df2 = df.loc[df["bins"] != first_bin]
    df1 = df1.sample(n=df2.shape[0], replace=True)
    out_df = pd.concat([df1, df2])
    out_df = out_df.sample(n=num_features, replace=True)
    return out_df


def samples_to_keep(df, location):
    for i in ["x", "y"]:
        df[i] = df[i].astype("str")
    df["index"] = df[["x", "y"]].apply(lambda i: "_".join(i), axis=1)

    df = df.loc[df["index"].isin(location)].copy()
    df = df.drop("index", axis=1)
    return df


def mask_geotiff(src_file, geo_mask):
    with rasterio.open(src_file) as src:
        masked, transform = mask(src, [geo_mask], crop=True)
        meta = src.meta.copy()
    h, w = masked.shape[1], masked.shape[2]
    meta.update(transform=transform, height=h, width=w)
    return masked, meta


def get_dynamic_vars_files(single_dir, pressure_dir, date):
    date_str = date.strftime("%Y_%m_%d")
    file_list = []
    for i in [
        "lwe_thickness_of_stratiform_precipitation_amount",
        "lwe_thickness_of_convective_precipitation_amount",
        "lwe_thickness_of_atmosphere_mass_content_of_water_vapor",
        "ro",
        "swvl1",
        "swvl2",
    ]:
        file_list.append(os.path.join(single_dir, f"{i}_{date_str}.tif"))
    for i in ["air_temperature", "geopotential", "specific_humidity"]:
        file_list.append(os.path.join(pressure_dir, f"{i}_{date_str}.tif"))
    return file_list


def convert_flooding_tiff_json(src_file, start_date, end_date):
    with rasterio.open(src_file) as src:
        arr = src.read(1)
        transform = src.transform
    masked = arr == 0
    json = [
        {
            "type": "Feature",
            "properties": {"flooded": v, "start": start_date, "end": end_date},
            "geometry": s,
        }
        for i, (s, v) in enumerate(shapes(arr, masked, 4, transform))
    ]
    return json


def _get_spatial_dims(data_array):
    if isinstance(data_array, xr.core.dataarray.DataArray) | isinstance(
        data_array, xr.core.dataset.Dataset
    ):
        dims = data_array.dims
    else:
        dims = data_array.dimensions

    if "latitude" in dims and "longitude" in dims:
        x_dim = "longitude"
        y_dim = "latitude"
    elif "x" in dims and "y" in dims:
        x_dim = "x"
        y_dim = "y"
    elif "lat" in dims and "lon" in dims:
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


def raster_metadata_f_dataset(data_array, crs=CSR_FROM_EPSG, nodata=-9999.0):
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


def mask_array(arr, meta, mask_geom):
    with MemoryFile() as memfile:
        with memfile.open(**meta) as dst:
            dst.write(arr.astype(meta["dtype"]), 1)
        with memfile.open() as src:
            masked, transform = mask(src, [mask_geom], crop=True)
    h, w = masked.shape[1], masked.shape[2]
    meta.update(transform=transform, height=h, width=w)
    return np.squeeze(masked), meta


def mask_dataset(ds, geom, mask_crs=4326):
    ds.rio.write_crs({"init": "epsg:4326"}, inplace=True)
    ds = ds.rio.clip(geom, mask_crs)
    return ds


def read_river_discharge(data_dict, date, geom):
    da = xr.open_rasterio(data_dict[date].path)
    da = da.expand_dims({"time": [date]})
    da = mask_dataset(da, geom)
    da = da.where(da != 1.0000000200408773e20)
    return da


def read_ERA5_data(data_dict, date, geom):
    temp = xr.open_dataset(data_dict[date].path)
    data = {}
    for var in [
        "d2m",
        "t2m",
        "e",
        "lai_hv",
        "lai_lv",
        "pev",
        "ro",
        "ssrd",
        "tp",
        "swvl1",
        "swvl2",
        "swvl3",
        "swvl4",
    ]:
        if var in ["e", "pev", "ro", "ssrd", "tp"]:
            data[var] = temp[var].sum(dim="time")
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                data[var] = temp[var].mean(dim="time")
    ds = xr.Dataset(data)
    ds = ds.expand_dims({"time": [date]})
    ds = mask_dataset(ds, geom)
    ds = ds.where(ds != -9999)
    return ds


def read_chirps(data_dict, date, geom):
    da = xr.open_rasterio(data_dict[date].path)
    da = da.rio.set_nodata(-9999.0)
    da = da.expand_dims({"time": [date]})
    da = da.where(da != -9999)
    da = mask_dataset(da, geom)
    da = da.where(da != -9999)
    return da


def data_array_w_time(da, y_dim, x_dim, name, data_start, data_end, geom):
    da = mask_dataset(da, geom)
    arr = da.data
    if da.rio.nodata:
        arr = arr.astype("float32")
        arr[arr == da.rio.nodata] = np.nan
    time_coord = list(pd.date_range(data_start, data_end))
    y_coord = list(da[y_dim].data)
    x_coord = list(da[x_dim].data)
    lazy_da = [dask.array.from_array(arr, 100) for _ in range(len(time_coord))]
    out_arr = dask.array.stack(lazy_da)
    out_da = xr.DataArray(
        data=out_arr,
        coords={"time": time_coord, "y": y_coord, "x": x_coord},
        dims=("time", "y", "x"),
        name=name,
        attrs=da.attrs,
    )
    out_da.rio.write_crs({"init": "epsg:4326"}, inplace=True)
    return out_da


def data_t_lstm_format(data, date, length, y=False):
    start = date - datetime.timedelta(length)
    end = date - datetime.timedelta(1)
    time_slice = slice(start, end)
    ds_sel = data.sel(time=time_slice)
    ds_sel["time"] = range(0, length)
    ds_sel = ds_sel.expand_dims({"date": [date]})
    if y:
        y = data["dis"].sel(time=date)
        y = y.expand_dims({"date": [date]})
        return ds_sel, y
    else:
        return ds_sel


def chunk_list(initial_list, chunk_size):
    final_list = []
    for i in range(0, len(initial_list), chunk_size):
        final_list.append(
            initial_list[i : i + chunk_size]  # noqa: E203 - ignore whitespace check
        )
    return final_list


def read_dem(f_src, data_start, data_end, geom, match_da, reproject_chunk=False):
    dem = xr.open_rasterio(f_src)
    dem = dem.squeeze(dim="band")
    dem_da = data_array_w_time(dem, "y", "x", "dem", data_start, data_end, geom)
    if reproject_chunk:
        size = dem_da.shape[0]
        chunks = chunk_list(list(range(size)), 100)
        dem_da = xr.concat(
            [dem_da.isel(time=i).rio.reproject_match(match_da) for i in chunks], "time"
        )
    else:
        dem_da = dem_da.rio.reproject_match(match_da)
    for dim in ["x", "y"]:
        dem_da[dim] = match_da[dim]
    return dem_da


def read_up_area(f_src, data_start, data_end, geom, match_da):
    area_ds = xr.open_dataset(f_src)
    area = area_ds["upArea"]
    area = area.rename({"lat": "y", "lon": "x"})
    area_da = data_array_w_time(area, "y", "x", "up_area", data_start, data_end, geom)
    area_da = area_da.rio.reproject_match(match_da)
    for dim in ["x", "y"]:
        area_da[dim] = match_da[dim]
    return area_da
