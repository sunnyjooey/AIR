import collections
import configparser
import json
import logging
import os
import pickle  # noqa: S403
from pathlib import Path

import fiona
import numpy as np
import pandas as pd
import pyproj
import rasterio
from affine import Affine
from geopandas import GeoDataFrame
from kiluigi.parameter import GeoParameter
from osgeo import gdal, gdalconst, ogr, osr
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject
from scipy.interpolate import griddata
from shapely.geometry import Point, mapping
from shapely.ops import transform

logger = logging.getLogger("luigi-interface")


def open_raster(raster_file):
    """
    Open raster file using GDAL
    """
    raster = gdal.Open(raster_file)
    return raster


def close_raster(raster):
    """
    Close raster file
    """
    raster = None
    del raster


def calculate_mask_stats(rast, area, lat, lon):

    # Mask raster based on buffered shape
    rtrn_data = {}
    out_img, out_transform = mask(rast, [area], crop=True)
    no_data_val = rast.nodata

    out_data = out_img[0]

    # Remove grid with no data values
    clipd_img = out_data[out_data != no_data_val]
    clipd_img = clipd_img[~np.isnan(clipd_img)]

    # Calculate stats on masked array
    rtrn_data["mean"] = np.ma.mean(clipd_img)
    rtrn_data["median"] = np.ma.median(clipd_img)
    rtrn_data["min"] = np.ma.min(clipd_img)
    rtrn_data["max"] = np.ma.max(clipd_img)
    rtrn_data["std"] = np.ma.std(clipd_img)

    rtrn_data["cnt"] = clipd_img.size

    rtrn_data["latitude"] = lat
    rtrn_data["longitude"] = lon

    return pd.Series(rtrn_data)


def create_grid(lat_lim, lon_lim, res=0.5, init_z=True, logger=None):
    """Create a spatial mesh grid given upper and lower limits of latitude
    and longitude along with resolution. Spatial grid is filled with nans.
    lat_lim = [lower latitude limit, upper latitude limit]
    lon_lim = [lower longitude limit, upper longitude limit]
    res = resolution of grid spacing
    xx = longitudes
    yy = latitudes
    """

    # -------
    # Checks
    # -------
    if len(lat_lim) != 2:
        if logger:
            logger("lat_lim must have length = 2")
        return False
    if len(lon_lim) != 2:
        if logger:
            logger("lon_lim must have length = 2")
        return False
    if lat_lim[0] > lat_lim[1]:
        if logger:
            logger("lat_lim must be [lower limit, upper limit]")
        return False
    if lon_lim[0] > lon_lim[1]:
        if logger:
            logger("lon_lim must be [lower limit, upper limit]")
        return False

    lats = np.arange(lat_lim[0], lat_lim[1], res)
    lats = lats[::-1]
    lons = np.arange(lon_lim[0], lon_lim[1], res)

    xx, yy = np.meshgrid(lons, lats)

    if init_z:
        zz = np.empty(len(xx), len(yy))
        zz[:] = np.nan
        return xx, yy, zz
    else:
        return xx, yy


def grid_data_mean(org_lats, org_lons, org_data, new_lats, new_lons, logger=None):
    """ This method grids unstructured data to a regular grid. The
    incomming data (lat, lon, data) are vectors to be gridded.
    org_lats = Original data latitudes. Should be vector
    org_lons = Original data longitudes. Should be a vector
    org_data = Original data. Should be a vector
    new_lats = New latitude coordinates. Vector
    new_lons = New longitude coordintes. Vector
    new_data = returned gridded data set [n_lats, n_lons]
    TODO: Support sparse matrix
    """

    # ------------------------------
    # Check lengths of input vectors
    # ------------------------------
    if (len(org_lats) != len(org_data)) or (len(org_lons) != len(org_data)):
        if logger:
            logger("Unmatched input latitude, longitude, data vectors")
        return False

    # -------------------------------------
    # Create holder for gridded output data
    # -------------------------------------
    zz_cnt = np.zeros((len(new_lats), len(new_lons)))
    zz = np.zeros(np.shape(zz_cnt)) * np.NaN

    # ----------------------------------
    # loop through data and find nearest
    # grid point for each data point
    # ----------------------------------
    lat_inds = [np.abs(new_lats - i).argmin() for i in org_lats]
    lon_inds = [np.abs(new_lons - i).argmin() for i in org_lons]

    # -----------------
    # Bin data in grid
    # -----------------
    for i, d in enumerate(org_data):
        zz_cnt[lat_inds[i], lon_inds[i]] += 1
        zz[lat_inds[i], lon_inds[i]] += d

    # -------------------
    # Find mean for grid
    # -------------------
    zz = zz / zz_cnt

    return zz


def interp_basic(
    org_lats, org_lons, org_data, new_lats, new_lons, interp_method="linear"
):
    """ Interpolate unstructured D-dimensional data

    Methods:
    'linear' -- tesselate the input point set to n-dimensional
                simplices, and interpolate linearly on each simplex
    'nearest' -- return the value at the data point closest to the
                 point of interpolation
    'cubic' --- return the value determined from a piecewise cubic,
                continuously differentiable (C1), and approximately
                curvature-minimizing polynomial surface.
    """

    # -------------------------------------
    # Find values that are not NaN in data
    # -------------------------------------
    vals = ~np.isnan(org_data)

    zz = griddata(
        (org_lons[vals], org_lats[vals]),
        org_data[vals],
        (new_lons, new_lats),
        method=interp_method,
    )

    return zz


def xy(transform, rows, cols, offset="center"):
    """Returns the x and y coordinates of pixels at `rows` and `cols`.
    The pixel's center is returned by default, but a corner can be returned
    by setting `offset` to one of `ul, ur, ll, lr`.

    Parameters
    ----------
    transform : affine.Affine
        Transformation from pixel coordinates to coordinate reference system.
    rows : list or int
        Pixel rows.
    cols : list or int
        Pixel columns.
    offset : str, optional
        Determines if the returned coordinates are for the center of the
        pixel or for a corner.

    Returns
    -------
    xs : list
        x coordinates in coordinate reference system
    ys : list
        y coordinates in coordinate reference system
    """

    single_col = False
    single_row = False
    if not isinstance(cols, collections.Iterable):
        cols = [cols]
        single_col = True
    if not isinstance(rows, collections.Iterable):
        rows = [rows]
        single_row = True

    if offset == "center":
        coff, roff = (0.5, 0.5)
    elif offset == "ul":
        coff, roff = (0, 0)
    elif offset == "ur":
        coff, roff = (1, 0)
    elif offset == "ll":
        coff, roff = (0, 1)
    elif offset == "lr":
        coff, roff = (1, 1)
    else:
        raise ValueError("Invalid offset")

    xs = []
    ys = []
    for col, row in zip(cols, rows):
        x, y = transform * transform.translation(coff, roff) * (col, row)
        xs.append(x)
        ys.append(y)

    if single_row:
        ys = ys[0]
    if single_col:
        xs = xs[0]

    return xs, ys


def buffer_points(
    ref_rast_file,
    loc_csv_file,
    buf_rad=10000,
    lat_name="geo_latitude",
    lon_name="geo_longitude",
    fname_out="buffered_points.pkl",
):
    """
    """

    xy_proj = pyproj.Proj("+proj=sinu +R=6371007.181 +nadgrids=@null +wktext")

    # Get raster projection to use for cluster coordinates
    with rasterio.open(ref_rast_file) as rastf:
        rast_proj_raw = rastf.crs.data
        rast_proj = pyproj.Proj(rast_proj_raw)

    # Create transform with two projections
    def project(x, y, z=None):
        return pyproj.transform(xy_proj, rast_proj, x, y)

    # Read CSV file in pandas
    loc_df = pd.read_csv(loc_csv_file)

    # Convert latitude and longitude to x y coordinates
    loc_df["pointlatlon"] = loc_df.apply(
        lambda x: Point(x[lon_name], x[lat_name]), axis=1
    )
    loc_df["pointxy"] = loc_df.apply(
        lambda x: Point(xy_proj(x[lon_name], x[lat_name])), axis=1
    )

    # Create polygon using point and buffer
    loc_df["bufferxy"] = loc_df.apply(lambda x: x["pointxy"].buffer(buf_rad), axis=1)

    # Convert polygon to projection in raster file
    loc_df["buffer_latlon"] = loc_df.apply(
        lambda x: transform(project, x["bufferxy"]), axis=1
    )

    # Create Point from cluster lat, lon coordinates
    geo_cluster = GeoDataFrame(
        loc_df, crs=rast_proj_raw, geometry=loc_df["buffer_latlon"]
    )

    # Convert geometries to geojson
    geo_cluster["geom_geojson"] = geo_cluster.apply(
        lambda x: mapping(x["geometry"]), axis=1
    )

    if fname_out:
        # Write dataframe out
        with open(fname_out, "wb") as fout:
            pickle.dump(geo_cluster, fout)
    else:
        return geo_cluster


def mask_raster(
    ref_rast_file,
    buffered_points,
    lat_name="geo_latitude",
    lon_name="lon_name",
    fname_out="buffered_stats.csv",
):
    """ """

    # Open and read file with buffered geometeries
    with open(buffered_points, "rb") as fid:
        geo_cluster = pickle.load(fid)  # noqa: S301

    # Open raster file in rasterio and mask with buffered cluster geometries
    with rasterio.open(ref_rast_file) as rastf:
        calc_stats = geo_cluster.apply(
            lambda x: calculate_mask_stats(
                rastf, x["geom_geojson"], x[lon_name], x[lat_name]
            ),
            axis=1,
        )

    calc_stats.to_csv(fname_out, index=False)


def get_raster_proj_configfile(config_file, fname_out="raster_proj.json"):
    """
    Get projection and spatial system from a configuration file:
    -- Spatial reference system
    -- Origin X,Y
    -- Pixel width, height
    -- Number rows, cols

    This data is used in creating the new raster file with the same spatial characteristics
    as the reference raster file.
    """
    projdata = {}

    config = configparser.ConfigParser()
    config.read(config_file)

    # ----------------------------
    # Get spatial reference system
    # ----------------------------
    srs_wkt = config["projection"]["srs"]
    wkp = config["projection"]["wkp"]
    originx = config.getfloat("projection", "originx")
    originy = config.getfloat("projection", "originy")
    pixel_width = config.getfloat("projection", "pixel_width")
    pixel_height = config.getfloat("projection", "pixel_height")
    ncols = config.getint("projection", "ncols")
    nrows = config.getint("projection", "nrows")

    projdata["srs"] = srs_wkt
    projdata["wkp"] = wkp
    projdata["pixel"] = (originx, pixel_width, 0, originy, 0, pixel_height)
    projdata["ncolsrows"] = (ncols, nrows)

    with open(fname_out, "w") as fid:
        json.dump(projdata, fid)


def get_raster_proj(raster_file, fname_out="raster_proj.json"):
    """
        Get projection and spatial system of reference raster file:
        -- Spatial reference system
        -- Origin X,Y
        -- Pixel width, height
        -- Number rows, cols

        This data is used in creating the new raster file with the same spatial characteristics
        as the reference raster file.
    """
    projdata = {}

    # ----------------------------------------------------------
    # Opening the raster file can not be a separate Luigi task,
    # because the return object of gdal read is of Swig type
    # which cannot be pickled. Also, maybe it doesn't make sense
    # passing a large raster file as a parameter
    # ----------------------------------------------------------
    raster = open_raster(raster_file)

    # ----------------------------
    # Get spatial reference system
    # ----------------------------
    srs_wkt = raster.GetProjectionRef()

    # ----------------------------
    # Get grid (pixel) coordinates
    # ----------------------------
    geotransform = raster.GetGeoTransform()
    originx = geotransform[0]
    originy = geotransform[3]
    pixelwidth = geotransform[1]
    pixelheight = geotransform[5]

    # ------------------------------
    # Find number of rows and pixels
    # ------------------------------
    ncols = raster.RasterXSize
    nrows = raster.RasterYSize
    close_raster(raster)

    projdata["srs"] = srs_wkt
    projdata["pixel"] = (originx, pixelwidth, 0, originy, 0, pixelheight)
    projdata["ncolsrows"] = (ncols, nrows)

    with open(fname_out, "w") as fid:
        json.dump(projdata, fid)


def raster_2array(raster_file, fname_out="raster_array.pkl"):
    """
    Convert raster file to numpy array
    """
    # ----------------------------------------------------------
    # Opening the raster file can not be a separate Luigi task,
    # because the return object of gdal read is of Swig type
    # which cannot be pickled. Also, maybe it doesn't make sense
    # passing a large raster file as a parameter
    # ----------------------------------------------------------
    raster = open_raster(raster_file)

    band = raster.GetRasterBand(1)
    rast_array = band.ReadAsArray()
    close_raster(raster)

    with open(fname_out, "wb") as fid:
        pickle.dump(rast_array, fid)


def array_2raster(
    ref_proj, ref_array, no_data_val=-9999, proj_type="wkp", fname_out="out.tif"
):
    """Converts a numpy array to a raster file (geo-tiff)"""

    (ncols, nrows) = ref_proj["ncolsrows"]

    driver = gdal.GetDriverByName("GTiff")

    if len(ref_array.shape) == 2:
        out_raster = driver.Create(fname_out, ncols, nrows, 1, gdal.GDT_Float32)
        out_raster.SetGeoTransform(ref_proj["pixel"])
        outband = out_raster.GetRasterBand(1)
        outband.SetNoDataValue(no_data_val)
        outband.WriteArray(ref_array, 0, 0)

    # Multiband rasters
    elif len(ref_array.shape) == 3:
        band = int(ref_array.shape[2])
        out_raster = driver.Create(fname_out, ncols, nrows, band, gdal.GDT_Float32)
        out_raster.SetGeoTransform(ref_proj["pixel"])
        for i in np.arange(0, band):
            outband = out_raster.GetRasterBand(int(i + 1))
            outband.SetNoDataValue(no_data_val)
            outband.WriteArray(ref_array[:, :, i])

    proj = osr.SpatialReference()

    if proj_type.lower() == "wkp":
        proj.SetWellKnownGeogCS(ref_proj["wkp"])

    elif proj_type.lower() == "srs":
        proj.ImportFromWkt(ref_proj["srs"])

    else:
        logger.error(
            "Unrecongnized projection type for creating output raster file, "
            "must be wkp or srs"
        )
        raise Exception(
            "Unrecongnized projection type for creating output raster file, "
            "must be wkp or srs"
        )

    out_raster.SetProjection(proj.ExportToWkt())
    outband.FlushCache()

    close_raster(driver)
    close_raster(out_raster)
    close_raster(outband)


def rasterize_vectorfile(
    ref_proj,
    vector_file,
    attr_field,
    no_data_val=-9999,
    fname_out="out.tif",
    proj_type="wkp",
):
    """
    Base class for rasterizing a vector file.
    -- Vector file should be an ERSI Shapefile
    -- Raster file will be a geotiff

    TO DO: Add support for other raster and vector file types
    """
    # prj_wkt = '+proj=longlat +datum=WGS84 +no_defs'
    (ncols, nrows) = ref_proj["ncolsrows"]

    driver = ogr.GetDriverByName("ESRI Shapefile")

    data_source = driver.Open(vector_file, 0)
    layer = data_source.GetLayer()

    target_ds = gdal.GetDriverByName("GTiff").Create(
        fname_out, ncols, nrows, 1, gdal.GDT_Float32
    )

    target_ds.SetGeoTransform(ref_proj["pixel"])

    proj = osr.SpatialReference()
    if proj_type.lower() == "wkp":
        proj.SetWellKnownGeogCS(ref_proj["wkp"])

    elif proj_type.lower() == "srs":
        proj.ImportFromWkt(ref_proj["srs"])

    else:
        logger.error(
            "Unrecongnized projection type for creating output raster file, "
            "must be wkp or srs"
        )
        raise Exception(
            "Unrecongnized projection type for creating output raster file, "
            "must be wkp or srs"
        )

    target_ds.SetProjection(proj.ExportToWkt())

    band = target_ds.GetRasterBand(1)
    band.SetNoDataValue(no_data_val)
    band.FlushCache()

    gdal.RasterizeLayer(target_ds, [1], layer, options=["ATTRIBUTE=%s" % attr_field])
    close_raster(data_source)
    close_raster(layer)
    close_raster(target_ds)
    close_raster(band)


def gdal_regrid(
    src_fname, out_name, height, width, transform, proj=None, gdal_const_gra=0
):
    """
    """

    # Open and read source file
    src = gdal.Open(src_fname, gdalconst.GA_ReadOnly)
    src_proj = src.GetProjection()
    src_nodata = src.GetRasterBand(1).GetNoDataValue()
    if not src_nodata:
        src_nodata = -9999.0

    # Setup output file
    Path(out_name).parent.mkdir(parents=True, exist_ok=True)
    dst = gdal.GetDriverByName("GTiff").Create(
        out_name, width, height, 1, gdalconst.GDT_Float32
    )
    if proj is None:
        proj = src_proj

    out_band1 = dst.GetRasterBand(1)
    out_band1.SetNoDataValue(src_nodata)
    try:
        dst.SetGeoTransform(transform.to_gdal())
    except AttributeError:
        dst.SetGeoTransform(transform)
    dst.SetProjection(proj)

    # Resample type
    gdal.ReprojectImage(src, dst, src_proj, proj, gdal_const_gra)

    dst = None
    src = None


def convert_shapefile_to_json(shp_filename, json_filename):
    """
    Converts a shapefile to json.
    """
    features = []
    with fiona.open(shp_filename, "r") as src:
        for feature in src:
            features.append(feature)
        srid = src.crs["init"].split(":")[1]

    layer = {}
    if len(features) > 1:
        layer.update({"type": "FeatureCollection", "features": features})
    else:
        layer.update(features[0])

    # Set a named CRS using the format recommended by
    # the geojson spec (http://geojson.org/geojson-spec.html#named-crs)
    crs = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::%s" % srid}}
    layer["crs"] = crs

    with open(json_filename, "w", encoding="utf-8") as dst:
        dst.write(json.dumps(layer, indent=2) + "\n")


def convert_raster_to_json(
    raster_filename, json_filename, raster_val=None, geojson_name=None
):
    """
    Converts a raster to a geojson. Works for single band rasters.
    raster_filename = filename of raster file input
    json_filename = filename for json flie output
    raster_val = name of the value being extracted from the raster band.
                 default value is the name of the raster file.
    geojson_name = name property of the geojson dictionary.
                   default value is the name of the raster file.
    """
    # If no name passed for the raster_val, set it to be the tif file
    if not raster_val:
        raster_val = raster_filename.split("/")[-1].split(".tif")[0]

    if not raster_val:
        raster_val = raster_val

    # Open raster file
    with rasterio.open(raster_filename) as src:
        image = src.read(1)

        # Extract features from raster
        results = (
            {"properties": {raster_val: v}, "geometry": s}
            for i, (s, v) in enumerate(
                rasterio.features.shapes(image, transform=src.transform)
            )
        )
    geoms = list(results)
    geojson = {"type": "FeatureCollection", "features": geoms, "name": geojson_name}
    with open(json_filename, "w") as f:
        json.dump(geojson, f)


def downsample_raster(src_fname, out_fname=None, dscale=0.5, resampling="bilinear"):
    """
    Downsample raster by a percentage, dscale.  The number of rows and columns will
    be reduced by the value set by dscale, meaning the width of each pixel will be
    increased by a factor of 1/dscale. e.g. if dscale=0.5, the pixels will become 2x
    wider and longer.

    src_fname = Inoming raster filename to be downscaled
    out_fname = New raster filename the downscaleed raster will be written to
                The default is src_fname_d50 for downscale 50%
    dscale = Factor to downscale, default is 0.5.
             dscale should be 0-1 to project the raster to lower resolution.
    """
    # Set out_fname
    if not out_fname:
        out_fname = os.path.splitext(src_fname)[0]
        out_fname = out_fname + "_d{}.tif".format(int(dscale * 100))

    # Open and read source file
    with rasterio.open(src_fname) as src:

        # Replace src.nodata value with Nan
        arr = src.read()
        no_data_value = src.nodata

        if no_data_value is None:
            no_data_value = -9999
        arr[arr == no_data_value] = np.float32(np.nan)

        newarr = np.empty(
            shape=(
                arr.shape[0],  # same number of bands
                round(arr.shape[1] * dscale),
                round(arr.shape[2] * dscale),
            )
        )

        # Adjust the new affine transform to the 150% smaller cell size
        aff = src.transform
        newaff = Affine(aff.a / dscale, aff.b, aff.c, aff.d, aff.e / dscale, aff.f)

        if resampling == "bilinear":
            r = Resampling.bilinear
        if resampling == "mode":
            r = Resampling.mode

        # Reproject the raster to be saved in newarr
        reproject(
            arr,
            newarr,
            src_transform=aff,
            dst_transform=newaff,
            src_crs=src.crs,
            dst_crs=src.crs,
            resampling=r,
        )

        # Replace Nan values with src.nodata value
        newarr[np.isnan(newarr)] = no_data_value
        if len(newarr.shape) == 3:
            newarr = newarr[0]

        new_transform = [newaff[2], newaff[0], 0, newaff[5], 0, newaff[4]]
        new_proj = {
            "ncolsrows": [newarr.shape[1], newarr.shape[0]],
            "pixel": new_transform,
            "wkp": "EPSG:4326",
        }

        array_2raster(
            new_proj,
            newarr,
            no_data_val=no_data_value,
            proj_type="wkp",
            fname_out=out_fname,
        )

    return out_fname


def convert_raster_to_csv_points(raster_filename, csv_filename=None, dscale=1.0):
    """
    Converts a raster file to point data in a csv file. Outputs a csv file with
    the same root filename as the raster_filename input.

    raster_filename = complete path to raster file
    dscale = degree of downscaling if desired. Default is 1.0 which is no downscaling.
    """
    # if downscaling is required, call the downsample_raster function
    # and replace raster_filename with the downscaled raster.
    if dscale != 1.0:
        raster_filename = downsample_raster(raster_filename, dscale=dscale)

    # set csv_filename to be the same root name as raster_filename
    if not csv_filename:
        csv_filename = os.path.splitext(raster_filename)[0]
        csv_filename = csv_filename + ".csv"

    with rasterio.open(raster_filename) as src:
        no_data_value = int(src.nodata)
        os.system(  # noqa S605
            r"""gdal_translate -q -of xyz -co ADD_HEADER_LINE=YES\
                  -co COLUMN_SEPARATOR="," {} /vsistdout | grep -v "\{}"\
                  > {}""".format(
                raster_filename, no_data_value, csv_filename
            )  # noqa S608
        )

    return csv_filename


def convert_multiband_raster_to_csv_points(src_fname, drop_na=False):
    """
    Convert a multiband raster file to a dataframe that has two columns for
    'X' (longitude) and 'Y' (latitude) and the other columns for raster band values.
    """
    raster = gdal.Open(src_fname)
    with rasterio.open(src_fname) as src:
        image = src.read()

        # transform image
        if len(image.shape) == 3:
            bands, rows, cols = np.shape(image)

        elif len(image.shape) == 2:
            image = raster.GetRasterBand(1).ReadAsArray()
            rows, cols = np.shape(image)
            bands = 1

        # Convert band values to 1D
        multiband_data = image.reshape(bands, rows * cols)

        # Bounding box and resoltuion of image
        l, b, r, t = src.bounds
        res = src.res

        # Create meshgrid of X and Y
        x = np.arange(l, r, res[0])
        y = np.arange(t, b, -res[0])
        X, Y = np.meshgrid(x, y)

        # Flatten X and Y
        newX = np.array(X.flatten("C"))
        newY = np.array(Y.flatten("C"))

        # Join newX, newY and multiband Z information
        export = np.column_stack((newX, newY, multiband_data.T))
        df = pd.DataFrame(export)
        if drop_na is True:
            df = df.drop(df[df[2] == -9999].index)

        return df


def geography_f_country(country):
    file_name = f"{country.lower().replace(' ', '_')}_2d.geojson"
    file_path = Path("models/geography/boundaries") / file_name
    return GeoParameter().get_geojson_from_input(file_path)
