import datetime
import json
import logging
import os
import re
import shutil
import zipfile

import geopandas as gpd
import luigi
import numpy as np
import rasterio
from fiona.crs import from_epsg
from luigi import ExternalTask, Task
from luigi.configuration import get_config
from luigi.util import requires
from pydap.cas.urs import setup_session
from pydap.client import open_url
from rasterio.crs import CRS
from rasterio.mask import mask
from rasterio.transform import from_origin
from shapely.geometry import box, mapping, shape

from kiluigi.targets import CkanTarget, IntermediateTarget
from models.hydrology_model.flood_index.mapping import short_long_variable_name
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters
from utils.scenario_tasks.functions.time import MaskDataToTime

logger = logging.getLogger("luigi-interface")

config = get_config()

RELATIVE_PATH = "models/hydrology_model/flood_index/data_cleaning"


class PullFilesFromCKAN(ExternalTask):
    """
    Pull DEM, South Sudan admin boundary and subasin from CKAN
    """

    def output(self):
        return {
            # Ethiopia and South Sudan 3 Sec DEM
            "dem": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "befe08dc-7e84-4132-ba56-1be39d25c0d7"},
            ),
            # Ethiopia and South Sudan Subbasin
            "basin": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "1c3b4411-5190-4091-a5fa-73931ea77412"},
            ),
        }


class PullHighLowRainfallFromCKAN(ExternalTask):
    """
    """

    def output(self):
        return {
            "low": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "8dec1309-2bdd-4f81-8c97-c58012c08bed"},
            ),
            "high": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "a402271a-5dd6-4eec-a905-e720c9cd7ac3"},
            ),
            "mean": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "4fe010a5-ceaa-42b6-889e-35c823e50bc8"},
            ),
        }


@requires(PullHighLowRainfallFromCKAN)
class UnZipLowHIGHRainfall(Task):
    """
    """

    def output(self):
        rain_scenario = ["low", "high", "mean"]
        base_path = f"{RELATIVE_PATH}/{self.task_id}"
        targets = {k: IntermediateTarget(f"{base_path}/{k}/") for k in rain_scenario}
        return targets

    def run(self):
        with self.output()["low"].temporary_path() as low_dir:
            os.makedirs(low_dir, exist_ok=True)
            with self.output()["high"].temporary_path() as high_dir:
                os.makedirs(high_dir, exist_ok=True)
                with self.output()["mean"].temporary_path() as mean_dir:
                    os.makedirs(mean_dir, exist_ok=True)
                    for k, path in zip(
                        ["low", "high", "mean"], [low_dir, high_dir, mean_dir]
                    ):
                        self.extract_zipped_file(self.input()[k].path, path)

    def extract_zipped_file(self, src_zipfile, out_dir):
        zip_ref = zipfile.ZipFile(src_zipfile)
        zip_ref.extractall(path=out_dir)


@requires(PullFilesFromCKAN, GlobalParameters)
class SelectRiverBasinFromGeography(Task):
    """
    Select river basin from the geography
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 365)

    def run(self):
        basin_zip = self.input()[0]["basin"].path
        basin_gdf = gpd.read_file(f"zip://{basin_zip}")
        try:
            geomask = self.geography["features"][0]["geometry"]
        except KeyError:
            geomask = self.geography

        geomask = shape(geomask)
        basin_gdf["intersects"] = np.nan
        for index in basin_gdf.index:
            basin_gdf.loc[index, "intersects"] = geomask.intersects(
                basin_gdf.loc[index, "geometry"]
            )
        basin_gdf = basin_gdf[basin_gdf["intersects"]]
        with self.output().open("w") as out:
            out.write(basin_gdf)


@requires(SelectRiverBasinFromGeography, PullFilesFromCKAN)
class MaskDEMTOBasin(ExternalTask):
    """
    Mask DEM to the river basins
    """

    def output(self):
        out_dir = f"{RELATIVE_PATH}/{self.task_id}/"
        return IntermediateTarget(path=out_dir, timeout=60 * 60 * 24 * 365)

    def run(self):
        logger.info("Masking DEM to river basin")
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            dem_file = self.input()[1]["dem"].path
            df = self.input()[0].open().read()
            df = df.set_index("SUB_BAS")
            with rasterio.open(dem_file) as src:
                meta = src.meta.copy()
                for index in df.index:
                    geo_mask = mapping(df.loc[index, "geometry"])

                    masked, out_transform = mask(src, [geo_mask], crop=True)
                    meta.update(
                        transform=out_transform,
                        height=masked.shape[1],
                        width=masked.shape[2],
                    )
                    dst_file = os.path.join(tmpdir, f"{index}.tif")
                    with rasterio.open(dst_file, "w", **meta) as dst:
                        dst.write(masked.astype(meta["dtype"]))


class ScrapeFLDASData(ExternalTask):
    """
    Download FLDAS data files from Earthdata search
    """

    month = luigi.DateParameter(default=datetime.date(2017, 1, 1))
    variable = luigi.Parameter(default="Qs_tavg")

    def output(self):
        return IntermediateTarget(self.dst_name(), timeout=31536000)

    def dst_name(self):
        data_dir = f"{RELATIVE_PATH}/FLDAS_DATA"
        month_str = self.month.strftime("%Y%m")
        long_name = short_long_variable_name[self.variable]
        return f"{data_dir}/{long_name}/{long_name}_{month_str}.tif"

    def run(self):
        logger.info(
            f"Downloading {short_long_variable_name[self.variable]} for {self.month}"
        )
        url = "https://hydro1.gesdisc.eosdis.nasa.gov:443/opendap/FLDAS/"
        base_url = url + "FLDAS_NOAH01_C_GL_M.001"
        dst_file = self.output().path
        os.makedirs(os.path.dirname(dst_file), exist_ok=True)
        data_url = self.get_url(base_url, self.month)
        self.scrape_data(data_url, dst_file)

    def get_filename(self, date):
        date_str = date.strftime("%Y%m")
        filename = f"FLDAS_NOAH01_C_GL_M.A{date_str}.001.nc"
        return filename

    def get_url(self, base_url, date):
        url = f"{base_url}/{date.year}/{self.get_filename(date)}"
        return url

    def open_fldas_url(self, opendap_url):
        username = config.get("earthdata", "username")
        password = config.get("earthdata", "password")
        session = setup_session(username, password, check_url=opendap_url)
        dataset = open_url(opendap_url, session=session)
        return dataset

    def scrape_data(self, data_url, dst_file):
        dataset = self.open_fldas_url(data_url)
        variable = dataset[self.variable]
        var = variable[:, :, :]
        data = var.array.data
        dims = {k: v.data for k, v in var.maps.items()}
        nodata = dataset.attributes["NC_GLOBAL"]["missing_value"]
        data = np.flip(data, 1).astype("float32")
        nodata = data.dtype.type(nodata)
        attr = dataset.attributes["NC_GLOBAL"]
        transform = from_origin(
            dims["X"][0].item(),  # west
            dims["Y"][-1].item(),  # north (flip!)
            attr["DX"],  # xsize
            attr["DY"],
        )  # ysize
        meta = {
            "driver": "GTiff",
            "dtype": data.dtype,
            "count": data.shape[0],
            "height": data.shape[1],
            "width": data.shape[2],
            "nodata": nodata,
            "crs": CRS.from_epsg(4326),
            "transform": transform,
        }
        with rasterio.open(dst_file, "w", **meta) as dst:
            dst.write(data[0, :, :], 1)


class MaskDataToMatchDEM(Task):
    """
    Helper methods for masking data to match dem extent
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=60 * 60 * 24 * 365)

    def get_input(self):
        file_list = self.input()[0].path
        dem_dir = self.input()[1].path
        return file_list, dem_dir

    def run(self):
        file_list, dem_dir = self.get_input()
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            for dem in os.listdir(dem_dir):
                geo_mask = self.get_raster_extent(os.path.join(dem_dir, dem))
                for src_file in file_list:
                    i = os.path.basename(src_file)
                    img, meta = self.mask_raster(src_file, geo_mask)
                    time = self.get_month_f_string(i)
                    filename = f"{dem.replace('.tif', '')}_{i}"
                    dst_filename = os.path.join(tmpdir, filename)
                    with rasterio.open(dst_filename, "w", **meta) as dst:
                        dst.write(img.astype(meta["dtype"]))
                        dst.update_tags(Time=time)

    def mask_raster(self, src_raster, shapes):
        with rasterio.open(src_raster) as src:
            img, transform = mask(src, shapes, crop=True)
            meta = src.meta.copy()
        meta.update(transform=transform, height=img.shape[1], width=img.shape[2])
        return img, meta

    def get_raster_extent(self, rasterfile):
        with rasterio.open(rasterfile) as src:
            bounds = src.bounds
        bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
        gdf = gpd.GeoDataFrame({"geometry": bbox}, index=[0], crs=from_epsg(4326))
        geo_mask = [json.loads(gdf.to_json())["features"][0]["geometry"]]
        return geo_mask

    def get_month_f_string(self, string, time_pattern=r"\d{6}"):
        match = re.search(time_pattern, string)
        date = datetime.datetime.strptime(match.group(), "%Y%m")
        return date.strftime("%Y-%m-%d")


class ScrapeFLDASVariableAndMaskToMatchDEM(GlobalParameters, MaskDataToMatchDEM):
    """
    Download FLDAS for {variable} for period specified by {time} and mask to river basins using DEM.
    """

    variable = luigi.Parameter(default="Qs_tavg")

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=60 * 60 * 24 * 365)

    def requires(self):
        data_period = {datetime.date(i.year, i.month, 1) for i in self.time.dates()}
        data = [ScrapeFLDASData(month=i, variable=self.variable) for i in data_period]
        return {"dem": self.clone(MaskDEMTOBasin), "data": data}

    def get_input(self):
        file_list = [i.path for i in self.input()["data"]]
        dem_dir = self.input()["dem"].path
        return file_list, dem_dir

    def complete(self):
        return super(MaskDataToMatchDEM, self).complete()


class MaskSurfaceRunoffToMatchDEM(ScrapeFLDASVariableAndMaskToMatchDEM):
    """
    Download surface runoff for the period {time} and mask to river basin
    """

    variable = "Qs_tavg"

    def output(self):
        out_dir = f"{RELATIVE_PATH}/{self.task_id}/"
        return IntermediateTarget(path=out_dir, timeout=60 * 60 * 24 * 365)


class MaskSubSurfaceRunoffToMatchDEM(ScrapeFLDASVariableAndMaskToMatchDEM):
    """
    Download sub surface runoff for the period {time} and mask to river basin
    """

    variable = "Qsb_tavg"

    def output(self):
        out_dir = f"{RELATIVE_PATH}/{self.task_id}/"
        return IntermediateTarget(path=out_dir, timeout=60 * 60 * 24 * 365)


class RainfallData(GlobalParameters, MaskDataToTime):
    """
    Download rainfall for the period {time} and mask to river basin
    """

    def output(self):
        out_dir = f"{RELATIVE_PATH}/{self.task_id}/"
        return IntermediateTarget(path=out_dir, timeout=60 * 60 * 24 * 365)

    def requires(self):
        data_period = {datetime.date(i.year, i.month, 1) for i in self.time.dates()}
        data = [ScrapeFLDASData(month=i, variable="Rainf_f_tavg") for i in data_period]
        return {"data": data, "scenario": UnZipLowHIGHRainfall()}

    def complete(self):
        return super(MaskDataToTime, self).complete()

    def run(self):
        file_list = [i.path for i in self.input()["data"]]
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            for src_file in file_list:
                dst_file = os.path.join(tmpdir, os.path.basename(src_file))
                if self.rainfall_scenario == "normal":
                    shutil.copyfile(src_file, dst_file)
                else:
                    sce_dir = self.input()["scenario"][self.rainfall_scenario].path
                    date = self.get_date_f_file(src_file)
                    if self.apply_time_filter(
                        date,
                        self.rainfall_scenario_time.date_a,
                        self.rainfall_scenario_time.date_b,
                    ):
                        scenario_file = os.path.join(
                            sce_dir, f"{date.strftime('%m')}.tif"
                        )
                        shutil.copyfile(scenario_file, dst_file)
                    else:
                        shutil.copyfile(src_file, dst_file)

    def get_date_f_file(self, file_path):
        file_name = os.path.basename(file_path)
        date_str = file_name.split("_")[-1].replace(".tif", "")
        date = datetime.datetime.strptime(date_str, "%Y%m").date()
        return date


@requires(RainfallData, MaskDEMTOBasin, GlobalParameters)
class MaskRainfallDataToMatchDEM(MaskDataToMatchDEM):
    """
    """

    def output(self):
        out_dir = f"{RELATIVE_PATH}/{self.task_id}/"
        return IntermediateTarget(path=out_dir, timeout=60 * 60 * 24 * 365)

    def get_input(self):
        rain_dir = self.input()[0].path
        file_list = [os.path.join(rain_dir, i) for i in os.listdir(rain_dir)]
        dem_dir = self.input()[1].path
        return file_list, dem_dir


class MaskSoilMositureToMatchDEM(ScrapeFLDASVariableAndMaskToMatchDEM):
    """
    Download soil moisture for the period {time} and mask to river basin
    """

    variable = "SoilMoi00_10cm_tavg"

    def output(self):
        out_dir = f"{RELATIVE_PATH}/{self.task_id}/"
        return IntermediateTarget(path=out_dir, timeout=60 * 60 * 24 * 365)
