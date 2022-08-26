import datetime
import json
import logging
import os
import re
import shutil
import tempfile
import zipfile

import fiona
import geopandas as gpd
import luigi
import numpy as np
import rasterio
import requests
from dateutil import parser, rrule
from fiona.crs import from_epsg
from luigi import ExternalTask, Task
from luigi.configuration import get_config
from luigi.util import requires
from lxml import html  # noqa: S410
from osgeo import gdal
from rasterio.features import shapes
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.warp import Resampling, calculate_default_transform, reproject
from shapely.geometry import box, shape

from kiluigi.targets import CkanTarget, IntermediateTarget, RESTTarget
from models.hydrology_model.utils import (
    extract_zipfile,
    generate_earthdata_token,
    map_f_path,
    mask_raster,
    reproject_like,
)

CONFIG = get_config()

CACHE_DIR = CONFIG.get("core", "cache_dir")

logger = logging.getLogger("luigi-interface")

RELATIVE_PATH = "models/hydrology_model/soil_moisture/data_cleaning"


class CkanFile(ExternalTask):

    """
    Get shapefile from ckan
    """

    def output(self):
        return {
            "boundary": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "c8bf5ca8-af58-44d7-a25a-66ad7786b516"},
            ),
            "ss_boundary": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "4ca5e4e8-2f80-44c4-aac4-379116ffd1d9"},
            ),
        }


class RasterFiles(ExternalTask):
    """
    Pull raster from ckan
    """

    def output(self):
        return {
            "twi": CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "30a35367-29f4-4302-901b-02b91d80c4ea"},
            ),
            "landforms": CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "2bcdd6af-f239-4093-b816-1880da462840"},
            ),
            "texture": CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "23c342a0-21da-4706-aef8-32a517a6d7fe"},
            ),
        }


class ScrapeNDVIData(ExternalTask):
    """
    Scrape NDVI (MOD13A3) from https://e4ftl01.cr.usgs.gov/MOLT/MOD13A3.006/
    """

    tiles = luigi.ListParameter(default=[(7, 20), (7, 21), (8, 20), (8, 21)])

    download_start_date = luigi.DateParameter(default=datetime.date(2015, 3, 1))
    download_end_date = luigi.DateHourParameter(default=datetime.date(2015, 5, 1))

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            baseurl = "https://e4ftl01.cr.usgs.gov/MOLT/MOD13A3.006"

            for dt in list(
                rrule.rrule(
                    rrule.MONTHLY,
                    dtstart=self.download_start_date,
                    until=self.download_end_date,
                )
            ):
                url = f"{baseurl}/{dt.year}.{(dt.month):02d}.01"
                for t in self.tiles:
                    yearday = dt.strftime("%Y%j")
                    pattern = f"MOD13A3.A{yearday}.h{(t[1]):02d}v{(t[0]):02d}.006.*.hdf"
                    session = requests.session()
                    resp = session.get(url)
                    links = html.fromstring(resp.content).xpath(  # noqa: S410
                        "//a/@href"
                    )
                    matches = [re.match(pattern, link) for link in links]
                    filenames = [m.group(0) for m in matches if m]
                    filenames = list(set(filenames))
                    for i in filenames:
                        logger.debug(f"Downloading {url}/{i}")
                        resp_file = session.get(f"{url}/{i}")
                        dst_file = os.path.join(tmpdir, i)
                        with open(dst_file, "wb") as output:
                            for chunk in resp_file:
                                output.write(chunk)


@requires(ScrapeNDVIData)
class ExtractNDVI(Task):
    """Extract NDVI from hdf files and merge the data from different tiles
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            tifdir = tempfile.mkdtemp()
            if os.path.exists(tifdir):
                shutil.rmtree(tifdir)
            os.makedirs(tifdir)
            data_path = self.input().path
            for file in os.listdir(data_path):
                src_file = os.path.join(data_path, file)
                dst_file = os.path.join(tifdir, file.replace("hdf", "tif"))
                src_ds = gdal.Open(src_file)
                ndvi_ds = gdal.Open(src_ds.GetSubDatasets()[0][0])
                ds = gdal.Translate(dst_file, ndvi_ds, bandList=[1])
                ds = self.close_dataset(ds)

            tiles = os.listdir(tifdir)
            year_day = list({i.split(".")[1][1:] for i in tiles})
            for i in year_day:
                file_merge = [os.path.join(tifdir, file) for file in tiles if i in file]
                dst_name = os.path.join(tmpdir, self.get_name_f_year_day(i))
                self.merge_rasterfiles(file_merge, dst_name)

            shutil.rmtree(tifdir, ignore_errors=True)

    def close_dataset(self, ds):
        ds = None
        return ds

    def merge_rasterfiles(self, raster_list, dst_filepath):
        files_to_merge = []
        for i in raster_list:
            src = rasterio.open(i)
            files_to_merge.append(src)
        merge_array, out_trans = merge(files_to_merge)
        meta = src.meta.copy()

        height, width = merge_array.shape[1], merge_array.shape[2]
        meta.update(height=height, width=width, transform=out_trans)

        with rasterio.open(dst_filepath, "w", **meta) as dst:
            dst.write(np.squeeze(merge_array), 1)

    def get_name_f_year_day(self, yearday):
        date = datetime.datetime.strptime(yearday, "%Y%j").strftime(format="%Y%m%d")
        filename = f"{date}.tif"
        return filename


@requires(ExtractNDVI)
class RescaleNDVIData(Task):
    """
    Scale NDVI and reproject NDVI
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        data_path = self.input().path

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)

            for i in os.listdir(data_path):
                src_file = os.path.join(data_path, i)

                with rasterio.open(src_file) as src:
                    dst_meta = self.get_transform(src)
                    src_array = np.squeeze(src.read())
                    # Scale the data
                    src_array = np.where(
                        (src_array >= -2000) & (src_array <= 10000),
                        src_array * 0.0001,
                        -9999.0,
                    )
                    dst_shape = (dst_meta["height"], dst_meta["width"])
                    dst_array = np.ones(dst_shape) * -9999.0
                    reproject(
                        src_array,
                        dst_array,
                        src_transform=src.transform,
                        src_crs=src.crs,
                        src_nodata=-9999.0,
                        dst_transform=dst_meta["transform"],
                        dst_crs="EPSG:4326",
                        dst_nodata=-9999.0,
                        resampling=Resampling.bilinear,
                    )
                dst_file = os.path.join(tmpdir, i)
                with rasterio.open(dst_file, "w", **dst_meta) as dst:
                    dst.write(np.float32(dst_array), 1)

    def get_transform(self, src, dst_crs="EPSG:4326"):
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds
        )
        meta = src.meta.copy()
        meta.update(
            crs=dst_crs,
            transform=transform,
            width=width,
            height=height,
            dtype="float32",
            nodata=-9999.0,
        )
        return meta


class NSIDCTarget(RESTTarget):
    """
    Task for downloading SMAP soil moisture data
    """

    username = CONFIG.get("earthdata", "username")
    password = CONFIG.get("earthdata", "password")

    def __init__(
        self,
        short_name,
        version,
        time,
        coverage,
        filename,
        revisit,
        bbox="23.42, 3.45, 36.1, 12.41",
        url="https://n5eil01u.ecs.nsidc.org/egi/request?",
    ):
        self.short_name = short_name
        self.version = version
        self.time = time
        self.coverage = coverage
        self.filename = filename
        self.revisit = revisit
        self.token = generate_earthdata_token(self.username, self.password)
        self.bbox = bbox
        self.url = url

    def iter_num(self):
        start, end = [parser.parse(i) for i in self.time.split(",")]
        days = end - start
        num_files = days.days * self.revisit
        num_pages = np.ceil(num_files / 10).astype(int)
        return num_pages

    def get(self):
        path, file = os.path.split(self.filename.path)
        os.makedirs(path, exist_ok=True)

        file, ext = os.path.splitext(file)
        num_pages = self.iter_num() + 1
        for page_num in range(1, num_pages):
            url = (
                f"{self.url}short_name={self.short_name}&version={self.version}&"
                f"format=GeoTIFF&time={self.time}&coverage={self.coverage}"
                f"&bbox={self.bbox}&projection=Geographic"
                f"&token={self.token}&page_num={page_num}"
            )
            logger.debug(f"Downloading {url}")
            response = requests.get(url, stream=True)
            temp = os.path.join(path, f"{file}_{page_num}{ext}")
            with open(temp, "wb") as handle:
                for block in response.iter_content(1000):
                    handle.write(block)
        return path


class SoilMoisture36km(ExternalTask):

    """
    Download soil moisture with 36km resolution from SMAP
    """

    def output(self):
        filename = IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )
        short_name = "SPL3SMP"
        version = "005"
        time = "2015-03-01,2015-04-01"
        revisit = 1
        bbox = "23.42, 3.45, 36.1, 12.41"
        return NSIDCTarget(
            short_name,
            version,
            time,
            "/Soil_Moisture_Retrieval_Data_AM/soil_moisture",
            filename,
            revisit,
            bbox,
        )


class SoilMoisture9km(ExternalTask):

    """
    Download soil moisture with 9km resolution from SMAP
    """

    def output(self):
        filename = IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )
        short_name = "SPL3SMP_E"
        version = "002"
        time = "2015-03-01,2015-04-01"
        revisit = 1
        return NSIDCTarget(
            short_name,
            version,
            time,
            "/Soil_Moisture_Retrieval_Data_AM/soil_moisture",
            filename,
            revisit,
        )


@requires(SoilMoisture36km, CkanFile)
class ScrapeSoilMoisture36km(Task):
    """
    Scrape and clip Soil moisture data at 36km spatial resolution
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        folder = self.input()[0].get()
        zip_file = self.input()[1]["boundary"].path

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)

            for file in os.listdir(folder):
                file_path = os.path.join(folder, file)
                try:
                    with zipfile.ZipFile(file_path, "r") as archive:
                        archive.extractall(path=tmpdir)
                except zipfile.BadZipFile:
                    pass

            # Mask the files
            shp_file = extract_zipfile(
                zipath=zip_file, filename="box.shp", path=CACHE_DIR
            )
            with fiona.open(shp_file, "r") as shp:
                features = [feature["geometry"] for feature in shp]

            for subdirect in os.listdir(tmpdir):
                if os.path.isdir(f"{tmpdir}/{subdirect}/"):
                    name = os.listdir(f"{tmpdir}/{subdirect}/")[0]
                    src_path = os.path.join(tmpdir, subdirect, name)
                    dst_name = re.search("\d{8}", name).group()  # noqa: W605
                    dst_path = os.path.join(tmpdir, f"{dst_name}.tif")
                    mask_raster(src_path, dst_path, features)
                    shutil.rmtree(os.path.join(tmpdir, subdirect))


@requires(SoilMoisture9km, CkanFile)
class ScrapeSoilMoisture9km(ScrapeSoilMoisture36km):
    """
    Scrape and clip Soil moisture data at 9km spatial resolution
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )


@requires(RescaleNDVIData, CkanFile)
class MaskRasterNDVI(Task):

    """
    Mask NDVI data to South Sudan box
    """

    shpfile_key = luigi.Parameter(default="boundary")
    shpfile_name = luigi.Parameter(default="box.shp")

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        data_path = self.input()[0].path
        boundary_dict = self.input()[1]

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            ssudan_zip_bbox_path = boundary_dict[self.shpfile_key].path

            ssudan_bbox_path = extract_zipfile(
                ssudan_zip_bbox_path, self.shpfile_name, CACHE_DIR
            )

            with fiona.open(ssudan_bbox_path, "r") as shp:
                features = [feature["geometry"] for feature in shp]

            for raster in os.listdir(data_path):
                src_path = os.path.join(data_path, raster)
                dst_file = os.path.join(tmpdir, raster)
                date = datetime.datetime.strptime(raster.split(".")[0], "%Y%m%d")
                mask_raster(src_path, dst_file, features, date=date)


@requires(MaskRasterNDVI, ScrapeSoilMoisture36km)
class ReprojectNDVI36km(Task):

    """
    Resample NDVI from 250m to 36km spatial resolution
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 50 * 24 * 365
        )

    def run(self):
        ndvi_input = self.input()[0]
        if type(ndvi_input) == dict:
            try:
                ndvi__dict = {k: v.path for k, v in ndvi_input.items()}
            except AttributeError:
                ndvi__dict = ndvi_input
        else:
            ndvi__dict = map_f_path(ndvi_input.path)

        SPL3SMP_input = self.input()[1]
        if isinstance(SPL3SMP_input, dict):
            try:
                SPL3SMP_file = list(SPL3SMP_input.values())[0].path
            except AttributeError:
                SPL3SMP_file = list(SPL3SMP_input.values())[0]
        else:
            SPL3SMP_map = map_f_path(SPL3SMP_input.path)
            SPL3SMP_file = list(SPL3SMP_map.values())[0]

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            for key in ndvi__dict:
                reproject_like(
                    rast_file=ndvi__dict[key],
                    proj_file=SPL3SMP_file,
                    output_file=os.path.join(tmpdir, f"{key}.tif"),
                    nodata=-9999.0,
                )


@requires(MaskRasterNDVI, ScrapeSoilMoisture9km)
class ReprojectNDVI9km(ReprojectNDVI36km):

    """
    Resample NDVI from 250m to 9 km resolution
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )


@requires(MaskRasterNDVI)
class ReprojectRetrivalNDVI2_25km(Task):

    """
    Resample NDVI from 1km to 2.25 km resolution
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        data_path = self.input().path
        dst_meta = None
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)

            for file in os.listdir(data_path):
                src_path = os.path.join(data_path, file)
                dst_path = os.path.join(tmpdir, file)
                with rasterio.open(src_path) as src:
                    if not dst_meta:
                        dst_meta = self.get_dst_meta(src)
                    with rasterio.open(dst_path, "w", **dst_meta) as dst:
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=rasterio.band(dst, 1),
                            resampling=Resampling.bilinear,
                        )

    def get_dst_meta(self, src):
        dst_trans, width, height = calculate_default_transform(
            src_crs=src.crs,
            dst_crs=src.crs,
            width=src.width,
            height=src.height,
            left=src.bounds.left,
            bottom=src.bounds.bottom,
            right=src.bounds.right,
            top=src.bounds.top,
            resolution=tuple(i * 2.5 for i in src.res),
        )
        meta = src.profile.copy()
        meta.update(height=height, width=width, transform=dst_trans)
        return meta


class AverageUsingMovingWindowMixin:
    """
    Mixin provided method for resampling raster using weighted average by moving window
    """

    def get_indicies(self, src):
        array = np.squeeze(src.read())
        data_inds = np.where(array != src.nodata)
        indices = ((row, col) for row, col in zip(data_inds[0], data_inds[1]))
        return indices

    def centred_square(self, src_file, row, col):
        if isinstance(src_file, rasterio.io.MemoryFile):
            src = src_file.open()
        else:
            src = rasterio.open(src_file)
        x, y = src.xy(row, col)
        res = src.res
        crs = src.crs.data
        src = None
        # square bounds
        half_radius = res[0] * 2.5
        minx, miny = x - half_radius, y - half_radius
        maxx, maxy = x + half_radius, y + half_radius
        bbox = box(minx, miny, maxx, maxy)
        bbox_shp = gpd.GeoDataFrame({"geometry": bbox}, index=[0], crs=from_epsg(4326))
        bbox_shp = bbox_shp.to_crs(crs=crs)
        return bbox_shp, bbox

    def convert_array_geojson(self, array, mask, transform):
        geojson = [
            {"properties": {"pixel_val": v}, "geometry": s}
            for i, (s, v) in enumerate(shapes(array, mask, 4, transform))
        ]
        return geojson

    def average_using_window(self, med_res_file, low_res_file, dst_file):
        logger.debug(f"Getting weighted average for {low_res_file}")
        if isinstance(med_res_file, rasterio.io.MemoryFile):
            med_res_src = med_res_file.open()
        else:
            med_res_src = rasterio.open(med_res_file)

        meta = med_res_src.profile.copy()
        indicies_list = self.get_indicies(med_res_src)
        height, width = med_res_src.height, med_res_src.width
        out_array = np.ones((height, width)) * med_res_src.nodata

        for row, col in indicies_list:
            bbox_shp, bbox = self.centred_square(med_res_file, row, col)

            # Get feafures
            coords = [json.loads(bbox_shp.to_json())["features"][0]["geometry"]]

            # Mask low res raster
            if isinstance(low_res_file, rasterio.io.MemoryFile):
                low_res_src = low_res_file.open()
            else:
                low_res_src = rasterio.open(low_res_file)
            out_img, transform = mask(
                dataset=low_res_src, shapes=coords, all_touched=True
            )
            nodata = low_res_src.nodata
            out_img = np.squeeze(out_img)
            masked = out_img != nodata

            # Convert array to geojson
            json_temp = self.convert_array_geojson(out_img, masked, transform)
            sum_val = 0
            for poly in json_temp:
                val = poly["properties"]["pixel_val"]
                if val != nodata:
                    inter_area = shape(poly["geometry"]).intersection(bbox).area
                    val_weighted = (val / bbox.area) * inter_area
                    sum_val += val_weighted

            out_array[row, col] = sum_val

        if isinstance(dst_file, rasterio.io.MemoryFile):
            with dst_file.open(**meta) as dst:
                dst.write(np.float32(out_array), 1)
        else:
            with rasterio.open(dst_file, "w", **meta) as dst:
                dst.write(np.float32(out_array), 1)

    def tifs_f_path(self, file_dir):
        files = [i for i in os.listdir(file_dir) if i.endswith(".tif")]
        return files


@requires(ScrapeSoilMoisture9km, ScrapeSoilMoisture36km)
class WeightedResampleSoilMoistureFrom36To9km(AverageUsingMovingWindowMixin, Task):
    """
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        sm_9km_dir = self.input()[0].path
        sm_36km_dir = self.input()[1].path

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            sm_9km_file_list = self.tifs_f_path(sm_9km_dir)
            sm_9km_file = os.path.join(sm_9km_dir, sm_9km_file_list[0])
            sm_36km_file_list = self.tifs_f_path(sm_36km_dir)
            for i in sm_36km_file_list:
                sm_36km_file = os.path.join(sm_36km_dir, i)
                dst_file = os.path.join(tmpdir, i)
                self.average_using_window(sm_9km_file, sm_36km_file, dst_file)


@requires(ScrapeSoilMoisture9km, ReprojectNDVI36km)
class WeightedResampleNDVIFrom36To9km(AverageUsingMovingWindowMixin, Task):
    """
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        target_res_dir = self.input()[0].path
        ndvi_36km_res_dir = self.input()[1].path

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)

            target_file = self.tifs_f_path(target_res_dir)[0]
            target_filepath = os.path.join(target_res_dir, target_file)
            ndvi36km_files = self.tifs_f_path(ndvi_36km_res_dir)
            for i in ndvi36km_files:
                ndvi36km_filepath = os.path.join(ndvi_36km_res_dir, i)
                dst_filepath = os.path.join(tmpdir, i)
                self.average_using_window(
                    target_filepath, ndvi36km_filepath, dst_filepath
                )


@requires(ReprojectRetrivalNDVI2_25km, ScrapeSoilMoisture9km)
class WeightedResampleSoilMoistureFrom9To2_25km(AverageUsingMovingWindowMixin, Task):
    """
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVE_PATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        target_res_dir = self.input()[0].path
        sm_11_25km_dir = self.input()[1].path

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            target_file = self.tifs_f_path(target_res_dir)[0]
            target_filepath = os.path.join(target_res_dir, target_file)
            sm_11_25km_files = self.tifs_f_path(sm_11_25km_dir)
            for i in sm_11_25km_files:
                src_filepath = os.path.join(sm_11_25km_dir, i)
                dst_filepath = os.path.join(tmpdir, i)
                self.average_using_window(target_filepath, src_filepath, dst_filepath)


@requires(ReprojectRetrivalNDVI2_25km, ReprojectNDVI9km)
class WeightedResampleNDVIFrom9To2_25km(AverageUsingMovingWindowMixin, Task):

    """
    Resample NDVI data from 9km to 11.25km using a moving window
    """

    def output(self):
        return IntermediateTarget(
            path="resampled_ndvi/", task=self, timeout=60 * 60 * 24 * 365
        )

    def run(self):
        target_res_dir = self.input()[0].path
        ndvi_9km_dir = self.input()[1].path

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            target_file = self.tifs_f_path(target_res_dir)[0]
            target_filepath = os.path.join(target_res_dir, target_file)
            ndi_9km_files = self.tifs_f_path(ndvi_9km_dir)
            for i in ndi_9km_files:
                src_filepath = os.path.join(ndvi_9km_dir, i)
                dst_filepath = os.path.join(tmpdir, i)
                self.average_using_window(target_filepath, src_filepath, dst_filepath)
