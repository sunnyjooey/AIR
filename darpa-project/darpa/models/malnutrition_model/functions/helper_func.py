import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import zipfile
import geopandas as gpd
from shapely.geometry import Point
from geopandas.tools import sjoin
import gdal
import os
import cartopy.crs as ccrs
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from mpl_toolkits.axes_grid1 import make_axes_locatable


def unzip_shapefile(zipped_shp_path, shp_name):
    zip_ref = zipfile.ZipFile(zipped_shp_path, "r")
    zip_ref.extractall()
    shp_path = zip_ref.extract(shp_name)
    zip_ref.close()
    return shp_path


def retrieve_file(file_path, file_ext, output_path=None):
    """
    output_path (optional): usually required for unzipping files, if file_ext is  'zip'. defaults to None
    """

    if output_path is not None and file_path.endswith(".zip") and (file_ext == ".zip"):
        zip_ref = zipfile.ZipFile(file_path, "r")
        new_dir = os.path.join(output_path, file_path.split("/")[-2])
        zip_ref.extractall(new_dir)
        zip_ref.close()

    if output_path is None:

        if file_ext == ".shp":
            file_df = gpd.read_file(file_path)
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


def plot_raster(raster_tiff, plot_title="Malnutrition"):

    """
    raster_tiff: path to the raster .tiff file
    """
    srcfile = gdal.Open(raster_tiff)
    src_band = srcfile.GetRasterBand(1).ReadAsArray().astype(np.float)
    ref_geotrans = srcfile.GetGeoTransform()
    nrows = srcfile.RasterYSize
    ncols = srcfile.RasterXSize
    no_data_val = srcfile.GetRasterBand(1).GetNoDataValue()
    ulx, xres, xskew, uly, yskew, yres = ref_geotrans
    lrx = ulx + (ncols * xres)
    lry = uly + (nrows * yres)
    img_extent = (ulx, lrx, lry, uly)
    srcfile = None

    src_band[src_band == no_data_val] = 0  # np.nan

    fig = plt.figure(figsize=(14, 14))
    ax = plt.axes(projection=ccrs.PlateCarree())
    cmap = mpl.cm.viridis  # terrain
    cmap.set_bad("#e5e6e8")
    h = ax.imshow(
        src_band,
        origin="upper",
        extent=img_extent,
        transform=ccrs.PlateCarree(),
        cmap=cmap,
        norm=mpl.colors.LogNorm(),
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


def rastercsv_admin(raster_fname, shape_fname):
    """
    raster_fname: RasterToCSV output with columns X,Y,Z
    shape_fname: shape file with 'ADMIN1NAME', and ADMIN2NAME'
    (https://data.kimetrica.com/dataset/south-sudan/resource/4c110606-e7c7-45b3-8f8b-9f8032b9c3fe )
    """
    admin_file = gpd.read_file(shape_fname)
    raster_map = pd.read_csv(raster_fname)
    # convert x, y coordinates into geopanda geometry attribute
    raster_map["Coordinates"] = list(zip(raster_map["X"], raster_map["Y"]))
    raster_map["Coordinates"] = raster_map["Coordinates"].apply(Point)
    raster_map_gpd = gpd.GeoDataFrame(raster_map, geometry="Coordinates")
    raster_map_gpd.crs = admin_file.crs
    # spatial join, this takes a long time for large raster_fname!
    point_admin_file = sjoin(
        raster_map_gpd, admin_file[["ADMIN1NAME", "ADMIN2NAME", "geometry"]], how="left"
    )
    df_out = point_admin_file[["X", "Y", "Z", "ADMIN1NAME", "ADMIN2NAME"]]
    return df_out
