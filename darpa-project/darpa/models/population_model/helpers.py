import zipfile

import gdal
import numpy as np
import pandas as pd

import cartopy.crs as ccrs
import geopandas as gpd
import matplotlib as mpl
import matplotlib.pyplot as plt
from cartopy.mpl.gridliner import LATITUDE_FORMATTER, LONGITUDE_FORMATTER
from mpl_toolkits.axes_grid1 import make_axes_locatable


def unzip_shapefile(zipped_shp_path, shp_name):
    zip_ref = zipfile.ZipFile(zipped_shp_path, "r")
    zip_ref.extractall()
    shp_path = zip_ref.extract(shp_name)
    zip_ref.close()
    return shp_path


def retrieve_file(file_path, file_ext):
    if file_path.endswith(".zip") and (file_ext != ".zip"):
        zip_ref = zipfile.ZipFile(file_path, "r")
        zip_ref.extractall()
        zip_ref.close()

        file_retrieved = []
        for file in zip_ref.namelist():
            if file.endswith(file_ext):
                file_retrieved.append(file)

    # just unzip the whole folder
    if file_path.endswith(".zip") and (file_ext == ".zip"):
        zip_ref = zipfile.ZipFile(file_path, "r")
        new_dir = "/".join(file_path.split("/")[:-1]) + "/"
        zip_ref.extractall(new_dir)
        zip_ref.close()

    if file_ext == ".shp":
        file_df = gpd.read_file(file_retrieved[0])
    elif file_ext == ".xlsx":
        file_df = pd.read_excel(file_path)
    elif file_ext == ".csv":
        file_df = pd.read_csv(file_path)
    else:
        file_df = []

    return file_df


def plot_raster_shapefile(
    raster, shapefile, title, gridline=False, cmap=mpl.cm.viridis, savefig=False
):

    # Read raster file
    raster_ds = gdal.Open(raster)
    raster_array = raster_ds.GetRasterBand(1).ReadAsArray().astype(np.float)

    # Get reference projection and geotransform for output raster
    raster_ds.GetProjection()
    ref_geotrans = raster_ds.GetGeoTransform()
    nrows = raster_ds.RasterYSize
    ncols = raster_ds.RasterXSize
    no_data_val = raster_ds.GetRasterBand(1).GetNoDataValue()
    ulx, xres, xskew, uly, yskew, yres = ref_geotrans
    lrx = ulx + (ncols * xres)
    lry = uly + (nrows * yres)
    img_extent = (ulx, lrx, lry, uly)
    raster_ds = None
    raster_array[raster_array == no_data_val] = np.nan

    # Plot Density
    fig = plt.figure(figsize=(14, 5))
    ax = plt.axes(projection=ccrs.PlateCarree())

    # Read shapfile data
    shp = gpd.read_file(shapefile)

    shp.plot(ax=ax, color="black", markersize=10)

    cmap.set_bad("#e5e6e8")
    h = ax.imshow(
        raster_array,
        origin="upper",
        extent=img_extent,
        transform=ccrs.PlateCarree(),
        cmap=cmap,
    )
    if gridline:
        gl = ax.gridlines(
            crs=ccrs.PlateCarree(),
            draw_labels=True,
            linewidth=2,
            color="gray",
            alpha=0.5,
            linestyle="-",
        )
        gl.xlabels_top = False
        gl.ylabels_right = False
        gl.xformatter = LONGITUDE_FORMATTER
        gl.yformatter = LATITUDE_FORMATTER
        gl.xlabel_style = {"size": 12, "color": "gray"}
        gl.ylabel_style = {"size": 12, "color": "gray"}
        divider = make_axes_locatable(ax)
        ax_cb = divider.new_horizontal(size="3%", pad=0.3, axes_class=plt.Axes)
        fig.add_axes(ax_cb)
    plt.colorbar(h)
    ax.set_title(title, fontsize=14)
    if savefig:
        plt.savefig(title + ".svg", bbox_inches="tight")

    plt.show()


def plot_raster(raster_tiff, plot_title="Density Data(county)"):
    """
    raster_tiff: path to the raster .tiff file
    """
    srcfile = gdal.Open(raster_tiff)
    src_band = srcfile.GetRasterBand(1).ReadAsArray().astype(np.float)
    srcfile.GetProjection()
    ref_geotrans = srcfile.GetGeoTransform()
    nrows = srcfile.RasterYSize
    ncols = srcfile.RasterXSize
    no_data_val = srcfile.GetRasterBand(1).GetNoDataValue()
    ulx, xres, xskew, uly, yskew, yres = ref_geotrans
    lrx = ulx + (ncols * xres)
    lry = uly + (nrows * yres)
    img_extent = (ulx, lrx, lry, uly)
    srcfile = None

    src_band[src_band == no_data_val] = np.nan

    fig = plt.figure(figsize=(14, 14))
    ax = plt.axes(projection=ccrs.PlateCarree())
    cmap = mpl.cm.pink  # terrain
    cmap.set_bad("#e5e6e8")
    h = ax.imshow(
        src_band,
        origin="upper",
        extent=img_extent,
        transform=ccrs.PlateCarree(),
        cmap=cmap,
    )
    # norm=mpl.colors.LogNorm()
    gl = ax.gridlines(
        crs=ccrs.PlateCarree(),
        draw_labels=True,
        linewidth=1,
        color="gray",
        alpha=0.2,
        linestyle="-",
    )
    gl.xlabels_top = False
    gl.ylabels_right = False
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER
    gl.xlabel_style = {"size": 12, "color": "gray"}
    gl.ylabel_style = {"size": 12, "color": "gray"}
    divider = make_axes_locatable(ax)
    ax_cb = divider.new_horizontal(size="3%", pad=0.3, axes_class=plt.Axes)
    fig.add_axes(ax_cb)
    plt.colorbar(h, cax=ax_cb)
    ax.set_title(plot_title, fontsize=24)
