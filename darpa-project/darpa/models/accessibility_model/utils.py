"""Helper functions for accessibility model
"""
import logging
import os
import zipfile
from math import sqrt

import geopandas as gpd
import networkx as nx
import numpy as np
import rasterio
from numba import njit
from osgeo import gdal, ogr, osr
from rasterio import features

logger = logging.getLogger("luigi-interface")


def extract_zipfile(zipath, filename, path):
    """ Un zip zipped file and one file

    Parameters:
        zippath (str): zipped file path
        filename (str): file name to be extrctaed

    Returns:
        str: The location of the file
    """
    zip_ref = zipfile.ZipFile(zipath, "r")
    zip_ref.extractall(path=path)
    fp = zip_ref.extract(filename, path=path)
    zip_ref.close()
    return fp


def check_reproject(
    dataset, new_name, ref_proj, proj_type="srs", mode=gdal.GRA_NearestNeighbour
):
    """ Check the projection of the dataset and if neccesary reproject

    Parameter:
        dataset (str): Raster file path
        new_name (str): The new file name if reprojected
        ref_proj (dict): Expected projection
        proj_type (str): Cordinate system (default is 'srs')
        mode (): The Gdal mode used for warping

    Returns:
        str: The file location of the output raster
    """
    ds = gdal.Open(dataset)
    # -----------------------------------
    # Get Raster and expected raster info
    # ----------------------------------
    # Raster size
    exp_col, exp_rows = ref_proj["width"], ref_proj["height"]
    ds_col, ds_rows = ds.RasterXSize, ds.RasterYSize

    # XY corner position
    exp_pixel = ref_proj["transform"]
    ds_pixel = ds.GetGeoTransform()

    # Projection
    exp_proj = ref_proj["crs"]

    # Extract Epsg code
    ds_proj = osr.SpatialReference(wkt=ds.GetProjection())
    epsg = str(ds_proj.GetAttrValue("AUTHORITY", 1))

    if not all(
        [
            exp_col == ds_col,
            exp_rows == ds_rows,
            exp_pixel == ds_pixel,
            epsg in exp_proj,
        ]
    ):
        logger.warning(f"Reprojecting {dataset}")
        (ncols, nrows) = ref_proj["width"], ref_proj["height"]
        new = gdal.GetDriverByName("GTiff").Create(
            new_name, ncols, nrows, 1, gdal.GDT_Float32
        )
        new.SetGeoTransform(ref_proj["transform"])

        proj = osr.SpatialReference()

        if proj_type.lower() == "wkp":
            proj.ImportFromWkt(ref_proj["crs"])
        elif proj_type.lower() == "srs":
            proj.ImportFromWkt(ref_proj["crs"])
        else:
            logger.error(
                "Unrecongnized projection type for creating output raster file, "
                "must be wkp or srs"
            )
            raise Exception(
                "Unrecongnized projection type for creating output raster file, "
                "must be wkp or srs"
            )

        new.SetProjection(proj.ExportToWkt())

        # Set no datavalue
        band = new.GetRasterBand(1)
        band.SetNoDataValue(-9999)
        band.Fill(-9999)

        # Reproject
        gdal.ReprojectImage(ds, new, ds.GetProjection(), ref_proj["crs"], mode)

        ds = None
        os.remove(dataset)
        # Switch the file
        dataset = new_name

    return dataset


@njit
def build_node(matrix, nodatavalue):
    """Create node according to the matrix size

    Parameters:
        matrix (array): friction surface array
        nodatavalue (float): The value tha represent mising data

    Returns:
        a list: A list of nodes
    """
    matrix = np.where(matrix != nodatavalue)
    data_rows = list(matrix[0])
    data_cols = list(matrix[1])
    nodes = [(row, col) for row, col in zip(data_rows, data_cols)]
    return nodes


@njit
def convert_paths_to_array(matrix, path_dict):
    """Converts path dict into array

    Parameters:
        matrix (array): friction surface array
        paths_dict (dict): paths from source to target

    Returns:
        numpy.ndarray
    """
    result = np.ones(np.shape(matrix)) * -9999.0
    for loc in path_dict:
        tsum = 0
        for path in path_dict[loc]:
            if isinstance(path, tuple):
                tsum += matrix[path[0], path[1]]
            else:
                for i in path:
                    tsum += matrix[i[0], i[1]]
            result[loc] = tsum
    return result


# @njit
def least_cost_destination(matrix, path_dict, indicies_id_map):
    """ Return least cost destination from

    Parameters:
        matrix (array): friction surface array
        path_dict (dict): least cost path from source to target
        indicies_id_map (dict): A dict of markets' indicies and id

    Returns:
        numpy.ndarray
    """
    result = np.ones(np.shape(matrix)) * -9999.0
    for loc in path_dict:
        result[loc] = indicies_id_map.get(path_dict[loc][0])
    return result


@njit
def convert_path_lenth_to_array(matrix, path_lenth):
    """Convert a dict of node and least cost path into array

    Parameters:
        matrix (array): Friction surface array
        path_lenth (dict): Least cost path lenght

    Returns:
        array: An array for least cost path
    """
    result = np.ones(np.shape(matrix)) * -9999.0
    for node in path_lenth:
        result[node] = path_lenth[node]
    return result


# @njit
def get_destination_indices(raster, dest):
    """Get the indices of the target destination.

    Parameters:
        raster (dataset): A dataset object opened in 'r' mode
        dest (GeoDataFrame): Target destination geodataframe

    Returns:
        GeoDataFrame: Geodataframe of destination plus indicies
    """
    # Add latitude and longitude
    dest["latitude"] = dest.geometry.apply(lambda x: x.y)
    dest["longitude"] = dest.geometry.apply(lambda x: x.x)
    dest = dest.drop_duplicates(subset=["latitude", "longitude"])

    # Add indices of the destination
    dest["row"], dest["col"] = 0, 0
    for index, d_row in dest.iterrows():
        row, col = raster.index(d_row["longitude"], d_row["latitude"])
        dest.loc[index, ["row", "col"]] = row, col
    return dest


# @njit
def town_indices_from_raster(src_array, town_val):
    """Get destination indcies from a raster file

    Parameters:
        dest_file (np.ndarray): The town surface numpy array
        town_val (int): The pixel value of towns

    Returns:
        a set: The indcies of the towns
    """
    src_ind = np.where(src_array == town_val)
    rows = list(src_ind[0])
    cols = list(src_ind[1])
    indices = {(row, col) for row, col in zip(rows, cols)}
    return indices


# @njit
def convert_indicies_dict(dest):
    """Convert row and column into a dictionary

    Parameters:
        dest (dataframe): dataframe with row and columns

    Returns:
        A tuple: destination indicies and indicies id map
    """
    dest["indicies"] = list(map(lambda x, y: (x, y), dest["row"], dest["col"]))
    dest_indcies = set(dest["indicies"])
    dest["id"] = dest.index

    indicies_id_map = {x: y for x, y in zip(dest["indicies"], dest["id"])}

    return dest_indcies, indicies_id_map, dest


def _path_length_t_array(path_length, shape, nodatavalue):
    logger.info("Convert path dict to a array")
    arr = np.ones(shape) * nodatavalue
    for loc, cost in path_length.items():
        arr[loc] = cost
    return arr


@njit
def get_neighbors(node, dirs=((1, 0), (0, 1), (-1, 0), (0, -1))):
    return [(node[0] + i[0], node[1] + i[1]) for i in dirs]


@njit
def get_horizontal_weight(matrix, neighbor, node):
    return (matrix[neighbor] + matrix[node]) / 2


@njit
def get_diagonal_weight(matrix, neighbor, node):
    return (sqrt(2 * (matrix[neighbor]) ** 2) + sqrt(2 * (matrix[node]) ** 2)) / 2


def calculate_accesibility_surface(
    friction,
    destination,
    nearest_dest=False,
    res=1000.0,
    nodatavalue=-9999.0,
    dest_pixel=3,
    diagonal_movement=True,
):
    """Create accesibility surface

    Parameters:
        friction rasterio.io.DatasetReader: Raster whose value is the cost of moving across the cell
        destination (np.array or GeodataFrame): The file location of the destination
        res (float): The cell width in meters
        nearest_dest (bool): A flag for returning another raster for destination
                            (default is False)
        nodatavalue (float): The value representing missing value
                            (default is -9999.)
        dest_pixel (float): The pixel value of destination if variable `destination` is a np.array
                            (default is None)

    Returns:
        numpy.ndarray: Array whose values is travel time to destination
    """
    logger.info("calculating the least cost distance")
    if isinstance(destination, gpd.GeoDataFrame):
        # Find the row and column of the destination in the raster
        dest = get_destination_indices(friction, destination)

        # Convert the indicies into a dict
        destination, indicies_id_map, gdf = convert_indicies_dict(dest)
    else:
        assert dest_pixel, "The pixel value of town is None"
        destination = town_indices_from_raster(destination, dest_pixel)

    friction = friction.read(1)
    matrix = np.where(friction != nodatavalue, friction * res, friction)

    logger.info("Getting the nodelist")
    nodelist = build_node(matrix, nodatavalue)

    logger.info("Building the graph")

    G = nx.Graph()
    nodeset = set(nodelist)

    logger.info("Adding edges to the graph")
    G.add_weighted_edges_from(
        (
            (node, neighbor, get_horizontal_weight(matrix, neighbor, node))
            for node in nodeset
            for neighbor in get_neighbors(node)
            if neighbor in nodeset
        )
    )
    if diagonal_movement:
        G.add_weighted_edges_from(
            (
                (node, neighbor, get_diagonal_weight(matrix, neighbor, node))
                for node in nodeset
                for neighbor in get_neighbors(
                    node, ((1, 1), (1, -1), (-1, 1), (-1, -1))
                )
                if neighbor in nodeset
            )
        )
    # Drop destination outside the friction surface
    destination = destination.intersection(nodeset)

    # Get least cost path from any destination to all the indices
    if nearest_dest:
        logger.info("Get the least cost path")
        path_len, path_dict = nx.multi_source_dijkstra(G, destination)
        surface = _path_length_t_array(path_len, matrix.shape, nodatavalue)
        market_id = least_cost_destination(matrix, path_dict, indicies_id_map)
        shape = (1, market_id.shape[0], market_id.shape[1])
        surface = surface.reshape(shape)
        market_id = market_id.reshape(shape)
        surface = np.concatenate((surface, market_id))
        return surface, gdf

    else:
        logger.info("Get the least cost path")
        path_len = nx.multi_source_dijkstra_path_length(G, destination)

        # Convert path to a surface
        surface = _path_length_t_array(path_len, matrix.shape, nodatavalue)
        return surface


def array_to_raster(array, src_file, dst_file, nodata=None):
    """Convert array to raster with raster properties from another raster

    Parameters:
        array (numpy.ndarray): Array to be converted to raster
        src_file (str): The file location of the raster to get meta from
        dst_file (str): The file location of the new raster
        nodata (float): The value for missing data

    Returns:
        None
    """
    if isinstance(src_file, rasterio.io.DatasetReader):
        kwargs = src_file.profile.copy()
    elif isinstance(src_file, dict):
        kwargs = src_file.copy()
    else:
        with rasterio.open(src_file) as src:
            kwargs = src.profile.copy()

    kwargs["dtype"] = "float32"
    if nodata:
        kwargs["nodata"] = nodata

    with rasterio.open(dst_file, "w", **kwargs) as dst:
        dst.write(np.float32(array), 1)


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
    Vector file should be an ERSI Shapefile
    Raster file will be a geotiff
    TO DO: Add support for other raster and vector file types
    """
    # prj_wkt = '+proj=longlat +datum=WGS84 +no_defs'
    (ncols, nrows) = ref_proj["width"], ref_proj["height"]

    driver = ogr.GetDriverByName("ESRI Shapefile")

    data_source = driver.Open(vector_file, 0)
    layer = data_source.GetLayer()

    target_ds = gdal.GetDriverByName("GTiff").Create(
        fname_out, ncols, nrows, 1, gdal.GDT_Float32
    )

    target_ds.SetGeoTransform(ref_proj["transform"])

    proj = osr.SpatialReference()
    if proj_type.lower() == "wkp":
        proj.SetWellKnownGeogCS(ref_proj["crs"])

    elif proj_type.lower() == "srs":
        proj.ImportFromWkt(ref_proj["crs"])

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
    data_source = None
    layer = None
    target_ds = None
    band = None


def rasterize_like(raster, dst_path, shp, value, all_touched=False):
    """Rasterize shapefile like raster

    Parameters:
        raster (str): Raster metadata
        dst_path (str): The file location of the rasterized shapefile
        shp (str): Shapefile file location or geopandasdataframe
        value (str): The column to rasterized
        all_touched (bool): (default is false)

    Returns:
        None
    """
    if isinstance(shp, gpd.GeoDataFrame):
        shp_df = shp
    else:
        shp_df = gpd.read_file(shp)

    if isinstance(raster, str):
        with rasterio.open(raster) as src:
            meta = src.meta.copy()
    else:
        meta = raster

    meta.update(compress="lzw")
    meta.update(dtype="float32")

    with rasterio.open(dst_path, "w+", **meta) as dst:
        dst_arr = dst.read(1)
        shapes = ((geom, val) for geom, val in zip(shp_df.geometry, shp_df[value]))
        burned = features.rasterize(
            shapes=shapes,
            fill=-9999.0,
            out=dst_arr,
            transform=dst.transform,
            all_touched=all_touched,
        )
        dst.write_band(1, np.float32(burned))
