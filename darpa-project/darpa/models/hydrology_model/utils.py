import datetime
import json
import os
import socket
import zipfile
from ftplib import FTP  # noqaL S402 - ignore security check
from socket import gaierror
from xml.etree import ElementTree  # noqaL S402 - ignore security check

import fiona
import geopandas as gpd
import numpy as np
import rasterio
import requests
from bs4 import BeautifulSoup
from fiona.crs import from_epsg
from keras.layers import Dense
from keras.models import Sequential
from rasterio import features
from rasterio.features import shapes
from rasterio.mask import mask
from rasterio.warp import Resampling, calculate_default_transform, reproject
from shapely.geometry import box, shape


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


def mask_raster(src_name, dst_name, features, no_data_val=-9999.0, date=None):
    """ Mask raster using shapefile

        Parameters:
            src_name (str): The file location of the raster to be masked
            dst_name (str): The file location of the output raster
            features (): The fatures to be used to mask raster
            no_data_val (float): The value rep missing values

        Returns:
            None
        """
    if isinstance(features, str):
        with fiona.open(features, "r") as shp:
            features = [feature["geometry"] for feature in shp]

    if isinstance(src_name, rasterio.io.DatasetReader):
        out_image, out_transform = rasterio.mask.mask(
            src_name, features, filled=True, crop=True, nodata=no_data_val
        )
        out_meta = src_name.meta.copy()
    else:
        with rasterio.open(src_name) as src:
            out_image, out_transform = rasterio.mask.mask(
                src, features, filled=True, crop=True, nodata=no_data_val
            )
        out_meta = src.meta.copy()

    out_meta.update(
        {
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
            "nodata": no_data_val,
            "dtype": "float32",
        }
    )
    with rasterio.open(dst_name, "w", **out_meta) as dst:
        dst.write(np.float32(out_image))
        if date:
            dst.update_tags(Time=date)


def weighted_average(low_res_file, med_res_file):

    """Calculate weighted average of low res data using
        a window centred in medium res data

    Parameters:
        low_res_file (str): The file location of the low res raster
        med_res_file (str): The file location of the medium res raster

    Returns:
        numpy.ndarray: An array of values weighted by area
    """
    # Read ratser data
    med_res_src = rasterio.open(med_res_file)
    low_res_src = rasterio.open(low_res_file)

    # Get indicies
    med_res_array = np.squeeze(med_res_src.read())
    data_inds = np.where(med_res_array != med_res_src.nodata)

    out_array = np.ones(np.shape(med_res_array)) * med_res_src.nodata

    def indices_generator(indices):
        for row, col in zip(indices[0], indices[1]):
            yield row, col

    indices_iter = indices_generator(data_inds)

    for _, indice in enumerate(indices_iter):

        row, col = indice
        # Get the coordinate of pixel index (row, col)
        x, y = med_res_src.xy(row, col)

        # Create a box 45km wide
        len_radius = med_res_src.res[0] * 2.5
        minx, miny = x - len_radius, y - len_radius
        maxx, maxy = x + len_radius, y + len_radius
        bbox = box(minx, miny, maxx, maxy)

        # Insert bbox into a GeoDataFrame
        bbox_shp = gpd.GeoDataFrame({"geometry": bbox}, index=[0], crs=from_epsg(4326))

        # Reproject to match the raster
        bbox_shp = bbox_shp.to_crs(crs=med_res_src.crs.data)

        # Get the features
        coords = [json.loads(bbox_shp.to_json())["features"][0]["geometry"]]

        # Mask the low res rasterm with the window
        out_img_touched, out_transform = mask(
            dataset=low_res_src, shapes=coords, all_touched=True
        )

        # Remove the extra dimension
        out_img_touched = np.squeeze(out_img_touched)

        # crete a mask of nodata values
        masked = out_img_touched != low_res_src.nodata

        # Convert the array to Geojson
        array_json = [
            {"properties": {"pixel_val": v}, "geometry": s}
            for i, (s, v) in enumerate(
                shapes(out_img_touched, masked, 4, out_transform)
            )
        ]
        bbox_area = bbox.area
        output = 0
        for poly in array_json:
            val = poly["properties"]["pixel_val"]
            if val != low_res_src.nodata:
                inter_area = shape(poly["geometry"]).intersection(bbox).area
                new_val = (val / bbox_area) * inter_area
                output += new_val
        out_array[row, col] = output

    return out_array


def reproject_like(
    rast_file, proj_file, output_file, mode=Resampling.bilinear, nodata=None
):
    """Match the projection and resolution of a raster to another

    Parameters:
        rast_file (str): The file location of the raster to be reprojected
        proj_file (str): The file location of raster to get profile from or raster meta
        output_file (str): The file location of the output raster
        mode (Resampling): The resampling technique

    Returns:
        None
    """
    if isinstance(proj_file, dict):
        kwargs = proj_file
    else:
        with rasterio.open(proj_file) as proj_src:
            kwargs = proj_src.meta.copy()

    if nodata:
        kwargs.update({"nodata": nodata})

    with rasterio.open(rast_file) as src:
        with rasterio.open(output_file, "w", **kwargs) as dst:
            reproject(
                source=rasterio.band(src, 1),
                destination=rasterio.band(dst, 1),
                src_transform=src.transform,
                src_crs=src.crs,
                src_nodata=src.nodata,
                dst_transform=dst.transform,
                dst_crs=dst.crs,
                dst_nodata=dst.nodata,
                resampling=mode,
            )


def mlp_model():
    """ Multi-Layer Perceptrons
    """
    # Intialize the model
    model = Sequential()

    # Input layer
    model.add(Dense(3, input_dim=3, kernel_initializer="normal", activation="relu"))

    # Hidden layer
    model.add(Dense(5, activation="relu"))

    # Output layer
    model.add(Dense(1, activation="linear"))

    model.compile(loss="mean_squared_error", optimizer="sgd")

    return model


def resample_raster(
    src_path, dst_path, res=9.0, mode=Resampling.bilinear, nodata=-9999.0
):
    """Change the spatial resolution of a raster

    Parameters:
        src_path (str): The file location of the raster
        dst_path (str): The file location of the output raster
        res (float): The number used to multiply current resolution (default is 9)
        mode (): Resampling technique
        nodata (float): The value representing  missing data

    Returns:
        None
    """
    with rasterio.open(src_path) as src:
        dst_transform, dst_width, dst_height = calculate_default_transform(
            src_crs=src.crs,
            dst_crs=src.crs,
            width=src.width,
            height=src.height,
            left=src.bounds.left,
            bottom=src.bounds.bottom,
            right=src.bounds.right,
            top=src.bounds.top,
            resolution=tuple(i * res for i in src.res),
        )

        kwargs = src.meta.copy()
        kwargs.update(
            height=dst_height, width=dst_width, transform=dst_transform, nodata=nodata
        )

        with rasterio.open(dst_path, "w", **kwargs) as dst:
            reproject(
                source=rasterio.band(src, 1),
                destination=rasterio.band(dst, 1),
                src_transform=src.transform,
                src_crs=src.crs,
                src_nodata=src.nodata,
                dst_transform=dst_transform,
                dst_crs=src.crs,
                dst_nodata=nodata,
                resampling=mode,
            )


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


def name_f_path(path):
    """Return

    Parameters:
        path (str): The file path

    Returns:
        name (str): name
    """
    name_w_ext = os.path.basename(path)
    name, _ = os.path.splitext(name_w_ext)
    return name


def map_f_path(path):
    maps = {os.path.splitext(i)[0]: os.path.join(path, i) for i in os.listdir(path)}
    return maps


def inundation_extent(src_file, dst_file, threshold=0.4):
    """Identify areas that are flooded

    Parameters:
        src_file (str): The file path for soil moisture
        dst_file (str): The file path inundation extent
        threshold (float): The threshold for identifying inudation

    Returns:
        None
    """
    with rasterio.open(src_file) as src:
        arr = np.squeeze(src.read(1))
        nodata = src.nodata
        meta = src.meta.copy()

    mask = arr == nodata
    arr = np.where(arr >= threshold, 1, 0)
    arr[mask] = nodata

    with rasterio.open(dst_file, "w", **meta) as dst:
        dst.write(np.float32(arr), 1)


def generate_earthdata_token(earthdata_username, earthdata_password):
    """ Generate earthdata token for accessing the data
    """
    hostname = socket.gethostname()

    try:
        ipaddress = socket.gethostbyname(hostname)
    except gaierror:
        ipaddress = socket.gethostbyname("127.0.0.1")

    xml = """<token>
             <username>{}</username>
             <password>{}</password>
             <client_id>earthlab</client_id>
             <user_ip_address>{}</user_ip_address>
             </token>""".format(
        earthdata_username, earthdata_password, ipaddress
    )

    headers = {"Content-Type": "application/xml"}

    r = requests.post(
        "https://cmr.earthdata.nasa.gov/legacy-services/rest/tokens",
        data=xml,
        headers=headers,
    )
    tree = ElementTree.fromstring(r.content)  # noqaL S402 - ignore security check
    token = tree[0].text
    return token


def get_url_paths(url, ext=".zip"):
    """ Get the list of zipped files in a given url

    Parameters:
        url (str): A link to website that has zipped files
        ext (str): Extenstion of files default is (.zip)

    Returns:
        a list of zipped files
    """
    response = requests.get(url)
    if response.ok:
        response_text = response.text
    else:
        return response.raise_for_status()
    soup = BeautifulSoup(response_text, "html.parser")
    parent = [
        url + node.get("href")
        for node in soup.find_all("a")
        if node.get("href").endswith(ext)
    ]
    return parent


def download_chirps_data(data_path, filename, download_file):
    """
    Download the chirps file and return the filename of the downloaded file.
    """
    # @TODO: keep FTP connection. Either make this a method on an object
    # that has the FTP connection as property, or pass in a function
    # that provides the FTP connection. But the connection should be re-used,
    # until it was closed by the server then it can be re-opened
    # intialize ftp
    ftp = FTP("ftp.chg.ucsb.edu")  # noqaL S402 - ignore security check
    ftp.sendcmd("USER anonymous")
    ftp.sendcmd("PASS anonymous@")

    # Download the file
    local_filename = os.path.join(data_path, filename)
    lf = open(local_filename, "wb")
    ftp.retrbinary("RETR " + download_file, lf.write)
    lf.close()
    return local_filename


def get_chirps_data(date, data_path):
    """Download and mask chirps data if not available in the local directory

        Parameters:
            date (str): The date to download chirps
            dat_path (str): The file location to be used to store the data
            features (str): The file location of the shapefile

        Returns:
            None
        """

    # Convert date parameter to data
    if isinstance(date, str):
        date = datetime.datetime.strptime(date, "%Y-%m-%d")

    # Create download file name
    url = f"pub/org/chg/products/CHIRPS-2.0/africa_daily/tifs/p05/{date.year}"
    filename = f"chirps-v2.0.{date.year}.{date.month:02}.{date.day:02}.tif.gz"
    download_file = os.path.join(url, filename)

    local_filename = download_chirps_data(data_path, filename, download_file)

    # Unzip the file
    error_code = os.system(f"gunzip {local_filename}")  # noqa: S605

    # If the file failed to unzip, download and try to unzip again
    if error_code != 0:
        local_filename = download_chirps_data(data_path, filename, download_file)
        new_error_code = os.system(f"gunzip {local_filename}")  # noqa S605

        # If the file still fails to unzip, raise an error
        if new_error_code != 0:
            raise ValueError(f"Unable to unzip the file: {local_filename}")

    # rename raster
    local_filename = local_filename.replace(".gz", "")
    date_str = date.strftime("%Y%m%d")
    dst_name = os.path.join(data_path, f"{date_str}.tif")
    os.rename(local_filename, dst_name)


def resample_raster_to_array(src_file, dst_file):
    """
    Resample raster files to match DEM
    """
    with rasterio.open(dst_file) as dem_src:
        dst_crs = dem_src.crs
        dst_transform = dem_src.transform
        destination = np.ones(dem_src.shape) * -9999.0

    with rasterio.open(src_file) as rain_src:
        src_arr = rain_src.read(1)
        src_transform = rain_src.transform
        src_crs = rain_src.crs
        src_nodata = rain_src.nodata

    reproject(
        source=src_arr,
        destination=destination,
        src_transform=src_transform,
        src_crs=src_crs,
        src_nodata=src_nodata,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        dst_nodata=-9999.0,
    )
    return destination
