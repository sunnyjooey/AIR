import calendar
import datetime
import gzip
import logging
import os
import tempfile

import luigi
import numpy as np
import rasterio
import requests
from luigi.contrib.ftp import RemoteTarget
from luigi.util import inherits, requires
from netCDF4 import Dataset

from kiluigi.targets import CkanTarget, IntermediateTarget
from kiluigi.tasks import ExternalTask, Task
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from .utils import mask_rasterfile, raster_metadata_f_dataset, resample_array

logger = logging.getLogger("luigi-interface")

RELATIVEPATH = "models/agromet/data_pre/"


class PullAvailableWaterCapacity(ExternalTask):
    """
    Pull available water capacity from CKAN
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "3b1f9aa8-8920-4c03-92c2-79f5b44c5df1"},
            resource={"id": "d84bbb3c-9f9f-45ec-aa55-cb1f11941efc"},
        )


class PullAdmin0Shapefile(ExternalTask):
    """
    Pull shapefile from CKAN to clip the area of intreset
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
            resource={"id": "7a49733f-728f-492c-b4ab-4fee899f243f"},
        )


class PullPrecipitationData(ExternalTask):
    """Pull precipitation data for one month.

    The spatial coverage of the rainfall data is Africa.
    """

    date = luigi.DateParameter(default=datetime.date(2015, 1, 1))

    def output(self):
        host = "ftp.chg.ucsb.edu"
        user = "anonymous"  # noqa: S105
        pwd = "anonymous@"  # noqa: S105
        dst_name = self.get_filename_f_date()
        path = f"pub/org/chc/products/CHIRPS-2.0/africa_monthly/tifs/{dst_name}"
        return RemoteTarget(path, host, username=user, password=pwd)  # noqa: S105

    def get_filename_f_date(self):
        return f"chirps-v2.0.{self.date.strftime('%Y.%m')}.tif.gz"


@requires(PullPrecipitationData)
class ScrapePrecipitationData(Task):
    """ Scrape precipitation data from one month
    """

    def output(self):
        dst = os.path.join(RELATIVEPATH, f"chirps/{self.get_filename_f_date()}")
        return IntermediateTarget(path=dst, timeout=5184000)

    def run(self):
        temp = tempfile.NamedTemporaryFile(suffix=".gz")
        self.input().get(temp.name)
        with gzip.open(temp.name) as gzipin:
            with rasterio.open(gzipin) as src:
                arr = src.read(1)
                meta = src.meta.copy()

        arr[arr == meta["nodata"]] = -9999.0
        meta.update(nodata=-9999.0)
        with self.output().open("w") as f:
            with rasterio.open(f, "w", **meta) as dst:
                dst.write(arr.astype(meta["dtype"]), 1)
        temp.close()

    def get_filename_f_date(self):
        return f"chirps-v2.0.{self.date.strftime('%Y.%m')}.tif"


@requires(ScrapePrecipitationData)
@inherits(GlobalParameters)
class MaskPrecipitationDataToGeography(Task):
    """
    Mask precipitation data to match areas of interest
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=5184000)

    def run(self):
        try:
            mask_geom = self.geography["features"][0]["geometry"]
        except KeyError:
            mask_geom = self.geography

        data_dir = self.input().path

        masked, meta = mask_rasterfile(src_file=data_dir, geo_mask=mask_geom)
        with self.output().open("w") as out:
            out.write({"array": masked, "meta": meta})


class ScrapePotentialEvapotranspiration(ExternalTask):
    """
    Scrape potential evapotranspiration data
    """

    date = luigi.DateParameter(default=datetime.date(1982, 1, 1))
    var = luigi.Parameter(default="pet")
    maxy = luigi.FloatParameter(default=22.23222)
    minx = luigi.FloatParameter(default=21.829102)
    maxx = luigi.FloatParameter(default=47.988243)
    miny = luigi.FloatParameter(default=3.406666)

    def output(self):
        file_name = f"pet/pet_{self.date.strftime('%Y_%m')}.tif"
        dst = os.path.join(RELATIVEPATH, file_name)
        return IntermediateTarget(path=dst, timeout=5184000)

    def last_day(self):
        return calendar.monthrange(self.date.year, self.date.month)[1]

    def get_url(self):
        start_date = self.date.replace(day=1).strftime("%Y-%m-%d") + "T00"
        end_date = self.date.replace(day=self.last_day()).strftime("%Y-%m-%d") + "T00"
        year = self.date.year
        base_url = "http://thredds.northwestknowledge.net"
        src = f":8080/thredds/ncss/TERRACLIMATE_ALL/data/TerraClimate_pet_{year}.nc?"
        subset_params = (
            f"var={self.var}&maxy={self.maxy}&minx={self.minx}&maxx={self.maxx}&"
            f"miny={self.miny}%20&horizStride=1&time_start={start_date}%3A00%3A00Z&"
            f"time_end={end_date}%3A00%3A00Z&timeStride=1&accept=netcdf"
        )
        return base_url + src + subset_params

    def run(self):
        logger.info(f"Downloading PET data for {self.date}")
        res = requests.get(self.get_url())
        logger.info(res.raise_for_status())
        dst_file = self.output().path
        os.makedirs(os.path.dirname(dst_file), exist_ok=True)
        temp = dst_file.replace("tif", "nc")

        with open(temp, "wb") as out:
            for chunk in res:
                out.write(chunk)

        ds = Dataset(temp)
        meta = raster_metadata_f_dataset(ds)
        array = ds["pet"][:, :, :]
        if isinstance(array, np.ma.core.MaskedArray):
            meta.update(nodata=array.fill_value)
        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write(array.astype(meta["dtype"]))
        os.remove(temp)


@requires(ScrapePotentialEvapotranspiration, MaskPrecipitationDataToGeography)
@inherits(GlobalParameters)
class MaskAndResamplePotentailEvapotranspiration(Task):
    """
    Mask and resample potential evapotranspiration data to match precipitation data
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=5184000)

    def run(self):
        pet_src = self.input()[0].path

        # Mask PET data
        try:
            mask_geom = self.geography["features"][0]["geometry"]
        except KeyError:
            mask_geom = self.geography

        masked, meta = mask_rasterfile(src_file=pet_src, geo_mask=mask_geom)

        # Resample PET data to match CHIRPS
        with self.input()[1].open() as src:
            dst_meta = src.read()["meta"]

        out_array = resample_array(masked, meta, dst_meta)
        with self.output().open("w") as out:
            out.write({"array": out_array, "meta": dst_meta})


@requires(PullAvailableWaterCapacity, MaskPrecipitationDataToGeography)
@inherits(GlobalParameters)
class MaskAndResampleWaterCapacity(Task):
    """
    Mask and resample available water capacity to match precipitation data
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=5184000)

    def run(self):
        awc_src = self.input()[0].path

        try:
            geo_mask = self.geography["features"][0]["geometry"]
        except KeyError:
            geo_mask = self.geography

        src_arry, src_meta = mask_rasterfile(awc_src, geo_mask)

        with self.input()[1].open() as src:
            dst_meta = src.read()["meta"]

        # Resample the data
        out_array = resample_array(src_arry, src_meta, dst_meta)

        with self.output().open("w") as out:
            out.write({"array": out_array, "meta": dst_meta})
