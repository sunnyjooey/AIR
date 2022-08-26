import datetime
import gzip
import logging
import os
import tempfile
from pathlib import Path

import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import rasterio
import requests
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from luigi import ExternalTask, Task
from luigi.configuration import get_config
from luigi.date_interval import Custom as CustomDateInterval
from luigi.parameter import DateIntervalParameter
from luigi.util import requires
from rasterio.mask import mask
from shapely.geometry import mapping

from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

logger = logging.getLogger("luigi-interface")

config = get_config()

RELATIVE_PATH = "low_high_rainfall/chirps_monthly/"


class RainfallSimulationParameters(GlobalParameters):
    """
    Add simulation parameter that are specific rainfall stimation
    """

    high_rainfall_const = luigi.FloatParameter(default=2.0)
    low_rainfall_const = luigi.FloatParameter(default=0.25)
    stat = luigi.Parameter(default="mean")


class ScrapeMonthlyRainfallDataFromCHIRPS(ExternalTask):
    """
    Pull CHIRPS monthly rainfall data from ftp and unzip
    """

    time_period = DateIntervalParameter(
        default=CustomDateInterval(
            datetime.date.fromisoformat("1982-01-01"),
            datetime.date.fromisoformat("2016-12-31"),
        )
    )

    def output(self):
        return IntermediateTarget(path="chirps_monthly/", timeout=31536000)

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            for month in pd.date_range(
                self.time_period.date_a, self.time_period.date_b, freq="M"
            ):
                logger.info(f"Downloading rainfall data for {month}")
                src_file = self.get_chirps_src(month) + ".gz"
                temp = tempfile.NamedTemporaryFile(suffix=".gz")
                req = requests.get(src_file)
                assert req.status_code == 200, "Download Failed"
                with open(temp.name, "wb") as fd:
                    for chunk in req.iter_content(chunk_size=1024):
                        fd.write(chunk)

                with gzip.open(temp.name) as gzip_src:
                    with rasterio.open(gzip_src) as src:
                        arr = src.read()
                        meta = src.meta.copy()
                        meta.update(nodata=-9999.0)
                out = Path(tmpdir) / self.get_filename_f_date(month)
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write(arr)

    def get_chirps_src(self, month):
        src_dir = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/"
        return src_dir + self.get_filename_f_date(month)

    @staticmethod
    def get_filename_f_date(month):
        return f"chirps-v2.0.{month.strftime('%Y.%m')}.tif"


@requires(ScrapeMonthlyRainfallDataFromCHIRPS, RainfallSimulationParameters)
class CHIRPSMonthlyRainfallSimulation(Task):
    """
    """

    def output(self):
        rainfall_scenarios = ["high_rainfall", "low_rainfall", "mean"]
        return {
            scenario: IntermediateTarget(
                path=f"data_simulation/{self.task_id}/{scenario}/",
                timeout=60 * 60 * 24 * 365,
            )
            for scenario in rainfall_scenarios
        }

    def run(self):
        with self.output()["high_rainfall"].temporary_path() as high_dir:
            os.makedirs(high_dir, exist_ok=True)
            with self.output()["low_rainfall"].temporary_path() as low_dir:
                os.makedirs(low_dir, exist_ok=True)
                with self.output()["mean"].temporary_path() as mean_dir:
                    os.makedirs(mean_dir, exist_ok=True)

                    data_dir = self.input()[0].path
                    month_list = [f"{str(i).zfill(2)}.tif" for i in range(1, 13)]
                    for month in month_list:
                        month_files = [
                            os.path.join(data_dir, i)
                            for i in os.listdir(data_dir)
                            if i.endswith(month)
                        ]
                        rain_arry, meta = self.read_rasterfiles(month_files, 35)

                        rain_mean = np.nanmean(rain_arry, axis=0)
                        rain_mean[np.isnan(rain_mean)] = meta["nodata"]
                        low_rainfall = np.where(
                            rain_mean == meta["nodata"],
                            meta["nodata"],
                            rain_mean * self.low_rainfall_const,
                        )
                        high_rainfall = np.where(
                            rain_mean == meta["nodata"],
                            meta["nodata"],
                            rain_mean * self.high_rainfall_const,
                        )

                        with rasterio.open(
                            os.path.join(high_dir, month), "w", **meta
                        ) as dst:
                            dst.write(high_rainfall.astype(meta["dtype"]), 1)
                        with rasterio.open(
                            os.path.join(low_dir, month), "w", **meta
                        ) as dst:
                            dst.write(low_rainfall.astype(meta["dtype"]), 1)
                        with rasterio.open(
                            os.path.join(mean_dir, month), "w", **meta
                        ) as dst:
                            dst.write(rain_mean.astype(meta["dtype"]), 1)

    def read_rasterfiles(self, rasterfile_list, num_year):
        num_file_error_msg = "The number of raster files not equal to num years required to calculate std and mean"
        assert len(rasterfile_list) == num_year, num_file_error_msg
        array_list = []
        for i in rasterfile_list:
            with rasterio.open(i) as src:
                temp = src.read(1)
                nodata = src.nodata
                # CHIRPS data nodata is None instead of -9999.
                nodata = nodata if nodata else -9999.0
                meta = src.meta.copy()
                temp = np.where(temp == nodata, np.nan, temp)
                array_list.append(temp)

        meta.update(nodata=nodata)
        out_array = np.stack(array_list, axis=0)
        assert len(rasterfile_list) == out_array.shape[0], "array stacking invalid"
        return out_array, meta


class PullAdminShapefileFromCkan(Task):
    """
    Pull admin shapefile from ckan to be used to mask raster files
    """

    def output(self):
        return {
            "South Sudan": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "0cf2b13b-3c9f-4f4e-8074-93edc01ab1bd"},
            ),
            "Ethiopia": CkanTarget(
                dataset={"id": "d07b30c6-4909-43fa-914b-b3b435bef314"},
                resource={"id": "d4804e8a-5146-48cd-8a36-557f981b073c"},
            ),
            "Djibouti": CkanTarget(
                dataset={"id": "cf5f891c-6fec-4509-9e3a-0ac69bb1b6e7"},  # Djibouti
                resource={"id": "412a2c5b-cee2-4e44-bd2a-2d8511834c8e"},
            ),
            "Sudan": CkanTarget(
                dataset={"id": "c055fdcb-9820-429d-8aac-440f84fd02ef"},  # Sudan
                resource={"id": "9ed3b234-63ca-401b-92a5-a1145291bde3"},
            ),
            "Kenya": CkanTarget(
                dataset={"id": "dd7275ad-4391-4fbb-a732-783d565ebfc1"},  # Kenya
                resource={"id": "8dae2ece-4ddc-43f3-ab90-847e01c8bfbd"},
            ),
            "Uganda": CkanTarget(
                dataset={"id": "dcd8b1bd-f6bd-4422-82b4-97d53337f68e"},  # Uganda
                resource={"id": "f93640e7-5543-4f75-9a53-d6d091692d34"},
            ),
            "Somalia": CkanTarget(
                dataset={"id": "abbde93b-d00c-4e45-9a00-a958096a9b12"},  # Somalia
                resource={"id": "5cc93072-b77b-4973-a1d0-2d9a93e96e6b"},
            ),
            "Eritrea": CkanTarget(
                dataset={"id": "785eb6ef-87f5-4a62-aa90-b89b835e8805"},  # Eritrea
                resource={"id": "04ed5da4-1baf-449e-9c15-b2f564dda80b"},
            ),
        }


@requires(
    PullAdminShapefileFromCkan,
    CHIRPSMonthlyRainfallSimulation,
    RainfallSimulationParameters,
)
class RainfallSimulationStatAtAdmin2(Task):
    """
    Calculate rainfall simulation statistics
    """

    def output(self):
        return FinalTarget(
            f"rainfall_scenario_{self.stat}_at_admin2_{self.country_level}.csv",
            task=self,
        )

    def run(self):
        admin_zip = self.input()[0][self.country_level].path
        gdf = gpd.read_file(f"zip://{admin_zip}")

        df_map = {}
        for simulation in self.input()[1]:
            logger.info(f"Getting stats for  {simulation}")
            data_dir = self.input()[1][simulation].path
            for index, i in enumerate(os.listdir(data_dir)):
                with rasterio.open(os.path.join(data_dir, i)) as src:
                    temp = gdf.copy()
                    temp["month"] = i.replace(".tif", "")
                    temp["scenario"] = simulation
                    temp["rainfall"] = temp["geometry"].apply(
                        lambda x: self.calculate_mask_statistic(
                            src, mapping(x), self.stat
                        )
                    )
                    if index == 0:
                        df_map[simulation] = temp.copy()
                    else:
                        df_map[simulation] = pd.concat([df_map[simulation], temp])
                        df_map[simulation] = df_map[simulation].reset_index(drop=True)

        out_df = pd.concat(df_map)
        # Drop geometry
        out_df = out_df.drop("geometry", axis=1)
        out_df.to_csv(self.output().path)

    def calculate_mask_statistic(self, rast, area, stat):
        # Mask raster based on buffered shape
        out_img, out_transform = mask(rast, [area], crop=True)
        no_data_val = rast.nodata

        out_data = out_img[0]

        # Remove grid with no data values
        clipd_img = out_data[out_data != no_data_val]
        clipd_img = clipd_img[~np.isnan(clipd_img)]

        # Calculate stats on masked array
        if stat == "mean":
            value = np.ma.mean(clipd_img)
        elif stat == "median":
            value = np.ma.median(clipd_img)
        elif stat == "min":
            value = np.ma.min(clipd_img)
        elif stat == "max":
            value = np.ma.max(clipd_img)
        elif stat == "std":
            value = np.ma.std(clipd_img)
        else:
            raise NotImplementedError

        return value


@requires(RainfallSimulationStatAtAdmin2, RainfallSimulationParameters)
class UploadRainfallSimulationToCkan(Task):
    """
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
            resource={
                "name": f"High constant is {self.high_rainfall_const} low is {self.low_rainfall_const} Rainfall {self.stat} at admin2 in {self.country_level}"
            },
        )

    def run(self):
        file_path = self.input()[0].path
        self.output().put(file_path)
