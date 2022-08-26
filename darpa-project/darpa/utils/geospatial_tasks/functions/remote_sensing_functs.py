import collections
import logging
import pickle  # noqa: S403
from typing import Dict

import pyproj

import luigi
import numpy as np
import pandas as pd
import rasterio
from geopandas import GeoDataFrame
from luigi import LocalTarget
from osgeo import gdal, gdalconst
from rasterio.mask import mask
from scipy.interpolate import griddata
from shapely.geometry import Point, mapping
from shapely.ops import transform

logger = logging.getLogger("luigi-interface")


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
    #    rtrn_data["cnt"] = clipd_img.count()
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
    dst = gdal.GetDriverByName("GTiff").Create(
        out_name, width, height, 1, gdalconst.GDT_Float32
    )
    if proj is None:
        proj = src_proj

    out_band1 = dst.GetRasterBand(1)
    out_band1.SetNoDataValue(src_nodata)
    dst.SetGeoTransform(transform)
    dst.SetProjection(proj)

    # Resample type
    gdal.ReprojectImage(src, dst, src_proj, proj, gdal_const_gra)

    dst = None
    src = None


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


def calc_cluster_data(
    geo_cluster, fname, lon="longitude", lat="latitude", geom="geom_geojson"
):
    """
    geo_cluster = geodataframe with clusters defined as polygons, with points and buffer zone as well
    fname = tif file with the raster data stored
    """
    # Open raster file in rasterio and mask with buffered cluster geometries
    with rasterio.open(fname) as rastf:
        calc_stats = geo_cluster.apply(
            lambda x: calculate_mask_stats(rastf, x[geom], x[lat], x[lon]), axis=1
        )

    return calc_stats


class BufferPoints(luigi.Task):
    """
    """

    buf_rad = luigi.FloatParameter(default=10000.0)
    lat_name = luigi.Parameter(default="geo_latitude")
    lon_name = luigi.Parameter(default="geo_longitude")
    fname_out = luigi.Parameter(default="buffered_points.pkl")

    def requires(self) -> Dict[str, luigi.Target]:
        raise NotImplementedError(  # type: ignore
            "Subclasses must specify \n"
            "{ref_rast_file: Reference raster for projection,\n"
            "loc_csv_file: CSV with latitude and longitude coordinates}"
        )

    def output(self):
        return LocalTarget(path=self.fname_out, format=luigi.format.Nop)

    def run(self):

        xy_proj = pyproj.Proj("+proj=sinu +R=6371007.181 +nadgrids=@null +wktext")

        # Get raster projection to use for cluster coordinates
        ref_rast_file = self.input()["ref_rast_file"]
        with rasterio.open(ref_rast_file.path) as rastf:
            rast_proj_raw = rastf.crs.data
            rast_proj = pyproj.Proj(rast_proj_raw)

        # Create transform with two projections
        def project(x, y, z=None):
            return pyproj.transform(xy_proj, rast_proj, x, y)

        # Read CSV file in pandas
        loc_csv_file = self.input()["loc_csv_file"]
        loc_df = pd.read_csv(loc_csv_file.path)

        # Convert latitude and longitude to x y coordinates
        loc_df["pointlatlon"] = loc_df.apply(
            lambda x: Point(x[self.lon_name], x[self.lat_name]), axis=1
        )
        loc_df["pointxy"] = loc_df.apply(
            lambda x: Point(xy_proj(x[self.lon_name], x[self.lat_name])), axis=1
        )

        # Create polygon using point and buffer
        loc_df["bufferxy"] = loc_df.apply(
            lambda x: x["pointxy"].buffer(self.buf_rad), axis=1
        )

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

        # Write dataframe out
        with self.output().open("w") as fout:
            pickle.dump(geo_cluster, fout)


class MaskRaster(luigi.Task):

    lat_name = luigi.Parameter(default="geo_latitude")
    lon_name = luigi.Parameter(default="geo_longitude")
    fname_out = luigi.Parameter(default="buffered_stats.csv")

    def requires(self) -> Dict[str, luigi.Target]:
        raise NotImplementedError(  # type: ignore
            "Subclasses must specify \n"
            "{ref_rast_file: Reference raster for projection,\n"
            "buffered_points: Geopandas with buffered coordinates}"
        )

    def output(self):
        return LocalTarget(path=self.fname_out)

    def run(self):

        # Open and read file with buffered geometeries
        with open(self.input()["buffered_points"].path, "rb") as fin:
            geo_cluster = pickle.load(fin)  # noqa: S301

        # Open raster file in rasterio and mask with buffered cluster geometries
        ref_rast_file = self.input()["ref_rast_file"]

        with rasterio.open(ref_rast_file.path) as rastf:
            calc_stats = geo_cluster.apply(
                lambda x: calculate_mask_stats(
                    rastf, x["geom_geojson"], x[self.lon_name], x[self.lat_name]
                ),
                axis=1,
            )

        with self.output().open("w") as fout:
            calc_stats.to_csv(fout, index=False)
