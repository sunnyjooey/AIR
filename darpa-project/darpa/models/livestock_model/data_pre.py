import datetime
import gzip
import logging
import os
import tempfile
import zipfile
from pathlib import Path

import luigi
import numpy as np
import pandas as pd
import rasterio
import requests
import rioxarray  # noqa: F401
import xarray as xr
from kiluigi.targets import CkanTarget, IntermediateTarget
from luigi import ExternalTask, Task
from luigi.util import inherits, requires
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject

from models.hydrology_model.river_discharge.utils import mask_dataset
from utils.geospatial_tasks.functions.geospatial import geography_f_country
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from .utils import mask_raster

logger = logging.getLogger("luigi-interface")

RELATIVEPATH = "models/livestock_model/data_pre"


class ScrapeMonthlyRainFallData(ExternalTask):
    """Scrape precipitation data for one day.

    The spatial coverage of the rainfall data is Africa.
    """

    date = luigi.DateParameter(default=datetime.date(2020, 1, 1))

    def output(self):
        file_name = f"chirps-v2.0.{self.date.strftime('%Y.%m')}.tif.gz"
        return IntermediateTarget(path=f"chirps_monthly/{file_name}", timeout=5184000)

    def run(self):
        src_file = self.get_chirps_src(self.date.year)
        req = requests.get(src_file)
        assert req.status_code == 200, "Download Failed"
        with self.output().open("w") as out:
            with open(out.name, "wb") as fd:
                for chunk in req.iter_content(chunk_size=1024):
                    fd.write(chunk)

    def get_chirps_src(self, year):
        src_dir = f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/{year}/"
        return src_dir + self.get_filename_f_date()

    def get_filename_f_date(self):
        return f"chirps-v2.0.{self.date.strftime('%Y.%m.%d')}.tif.gz"


@inherits(GlobalParameters)
class MaskRainfallData(Task):
    def requires(self):
        month_list = list(pd.date_range(self.time.date_a, self.time.date_b, freq="M"))
        # Ensure data cover one year to calculate annual rainfall
        year = self.time.date_a.year
        temp = list(
            pd.date_range(
                datetime.date(year, 1, 1), datetime.date(year + 1, 1, 1), freq="M"
            )
        )
        month_list = set(temp + month_list)
        return [ScrapeMonthlyRainFallData(date=i) for i in month_list]

    def output(self):
        dst = Path(RELATIVEPATH) / "maskedRainfall/"
        return IntermediateTarget(path=str(dst), task=self, timeout=5184000)

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            for i in self.input():
                with gzip.open(i.path) as gzip_src:
                    arr, meta = mask_raster(gzip_src, geom, -9999)
                meta.update(nodata=-9999.0)
                # convert from mm to cm
                arr = np.where(arr == -9999.0, -9999.0, arr / 10)
                f = os.path.join(tmpdir, os.path.basename(i.path).replace(".gz", ""))
                with rasterio.open(f, "w", **meta) as dst:
                    dst.write(arr)


class ScrapeMonthlyNDVIData(ExternalTask):
    date = luigi.DateParameter(default=datetime.date(2020, 1, 1))

    def output(self):
        name = f"{self.date.strftime('%Y_%m')}.tif"
        return CkanTarget(
            dataset={"id": "a4f582dc-7309-40bf-8b25-7502bb682553"},
            resource={"name": name},
        )


@inherits(GlobalParameters)
class MaskNDVIData(Task):
    def requires(self):
        month_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
        return [ScrapeMonthlyNDVIData(date=i) for i in month_list]

    def output(self):
        dst = Path(RELATIVEPATH) / "masked_ndvi/"
        return IntermediateTarget(path=str(dst), task=self, timeout=5184000)

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            for i in self.input():
                with rasterio.open(i.path) as src:
                    meta = src.profile.copy()
                    masked, transform = mask(src, geom, crop=True)
                h, w = masked.shape[1], masked.shape[2]
                meta.update(transform=transform, height=h, width=w, nodata=-9999.0)
                f = os.path.join(tmpdir, f"ndvi_{os.path.basename(i.path)}")
                with rasterio.open(f, "w", **meta) as dst:
                    dst.write(masked)


class PuLLMaxTemperature(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "109cce1d-91b3-41f1-b6cf-c1c82f6fb73c"},
            resource={"id": "fb582299-ecd4-4080-954e-d877a49cfb5b"},
        )


@requires(PuLLMaxTemperature)
@inherits(GlobalParameters)
class MaskMaxTemperature(Task):
    def output(self):
        dst = Path(RELATIVEPATH) / "mask_max_temp/"
        return IntermediateTarget(path=str(dst), task=self, timeout=5184000)

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]
        zip_ref = zipfile.ZipFile(self.input().path)
        data_dir = tempfile.mkdtemp()
        zip_ref.extractall(data_dir)

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            for i in os.listdir(data_dir):
                arr, meta = mask_raster(os.path.join(data_dir, i), geom)
                with rasterio.open(os.path.join(tmpdir, i), "w", **meta) as dst:
                    dst.write(arr)


class PuLLMinTemperature(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "109cce1d-91b3-41f1-b6cf-c1c82f6fb73c"},
            resource={"id": "db78aafa-d11d-43b0-8ba9-5d4f0367517d"},
        )


@requires(PuLLMinTemperature)
@inherits(GlobalParameters)
class MaskMinTemperature(Task):
    def output(self):
        dst = Path(RELATIVEPATH) / "mask_min_temp/"
        return IntermediateTarget(path=str(dst), task=self, timeout=5184000)

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]
        zip_ref = zipfile.ZipFile(self.input().path)
        data_dir = tempfile.mkdtemp()
        zip_ref.extractall(data_dir)

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            for i in os.listdir(data_dir):
                arr, meta = mask_raster(os.path.join(data_dir, i), geom)
                with rasterio.open(os.path.join(tmpdir, i), "w", **meta) as dst:
                    dst.write(arr)


class PullPFTData(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
            resource={"id": "44d586c5-0bb1-4854-9a1f-ded333f148c0"},
        )


@requires(PullPFTData)
@inherits(GlobalParameters)
class MaskPFTData(Task):
    pft = luigi.IntParameter(default=9)

    def output(self):
        return IntermediateTarget(
            path=f"{self.task_id}/pft{self.pft}.tif", timeout=5184000
        )

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]

        ds = xr.open_dataset(self.input().path)
        ds = ds.sel(PFT=self.pft)
        da = ds["PFTfrac"]
        da = da.rename({"lat": "y", "lon": "x"})
        da = mask_dataset(da, geom)
        with self.output().open("w") as out:
            da.rio.to_raster(out.name)


class PullSoilData(ExternalTask):
    def output(self):
        return {
            "ph": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "73e873f2-353c-4395-8dae-7e41ef2d3faa"},
            ),
            "sand": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "cc512e0b-a9a8-4ade-ba72-529705236251"},
            ),
            "clay": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "7516d70e-8726-4900-8231-0a45944944b3"},
            ),
            "silt": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "517ffa0e-c4bc-4a9a-bc36-a21f7abfdc38"},
            ),
            "bulk": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "af387b7f-f555-463e-b035-cd653623ac37"},
            ),
        }


@requires(PullSoilData)
class ReprojectSoilData(Task):
    def output(self):
        soil = ["ph", "sand", "clay", "silt", "bulk"]
        out = {
            i: IntermediateTarget(path=self.get_dst(i), timeout=5184000) for i in soil
        }
        return out

    def get_dst(self, var):
        dst = Path(RELATIVEPATH) / f"{self.task_id}_{var}.tif"
        return str(dst)

    def run(self):
        dst_crs = "EPSG:4326"
        for var, target in self.input().items():
            with rasterio.open(target.path) as src:
                transform, width, height = calculate_default_transform(
                    src.crs, dst_crs, src.width, src.height, *src.bounds
                )
                kwargs = src.meta.copy()
                kwargs.update(
                    crs=dst_crs, transform=transform, width=width, height=height
                )
                with self.output()[var].open("w") as out:
                    with rasterio.open(out, "w", **kwargs) as dst:
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=rasterio.band(dst, 1),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform,
                            dst_crs=dst_crs,
                        )


@requires(ReprojectSoilData)
@inherits(GlobalParameters)
class MaskSoilData(Task):
    def output(self):
        soil = ["ph", "sand", "clay", "silt", "bulk"]
        out = {
            i: IntermediateTarget(path=self.get_dst(i), timeout=5184000) for i in soil
        }
        return out

    def get_dst(self, var):
        dst = Path(RELATIVEPATH) / f"{self.task_id}_{var}.tif"
        return str(dst)

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]

        for var, src_target in self.input().items():
            arr, meta = mask_raster(src_target.path, geom)
            meta.update(dtype="float32")
            arr = arr.astype("float32")
            if var == "ph":
                arr = np.where(arr == meta["nodata"], meta["nodata"], arr / 10)
            elif var == "bulk":
                arr = np.where(arr == meta["nodata"], meta["nodata"], arr / 1000)
            else:
                arr = np.where(arr == meta["nodata"], meta["nodata"], arr / 100)

            with self.output()[var].open("w") as out:
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write(arr.astype(meta["dtype"]))


@requires(MaskPFTData)
class PastureLegumeFraction(Task):
    fraction = luigi.FloatParameter(default=0.0)

    def output(self):
        dst = Path(RELATIVEPATH) / "pasture_legume.tif"
        return IntermediateTarget(path=str(dst), task=self, timeout=5184000)

    def run(self):
        with rasterio.open(self.input().path) as src:
            arr = src.read(1)
            meta = src.meta.copy()

        out_arr = np.ones_like(arr) * self.fraction
        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write(out_arr, 1)


@requires(MaskPFTData)
class SiteIndex(Task):
    def output(self):
        dst = Path(RELATIVEPATH) / "site_index.tif"
        return IntermediateTarget(path=str(dst), task=self, timeout=5184000)

    def run(self):
        with rasterio.open(self.input().path) as src:
            arr = src.read(1)
            meta = src.meta.copy()
        meta.update(dtype="int32", nodata=-99)
        out_arr = np.ones_like(arr)
        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write(out_arr.astype(meta["dtype"]), 1)
