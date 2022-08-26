import datetime
import os
from ftplib import FTP  # noqa: S402 - ignore secuirty check

import luigi
import numpy as np
import pandas as pd
import pyspatialml
import rasterio
import rasterio.mask
from luigi.configuration import get_config
from luigi.util import requires
from rasterio.warp import Resampling
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from kiluigi.tasks import ExternalTask, Task, VariableInfluenceMixin
from models.accessibility_model.mapping import REF_PROJ_CONFIG
from models.hydrology_model.soil_moisture.data_cleaning import ReprojectNDVI36km
from models.hydrology_model.soil_moisture.down_scaling import EstimateSoilMoisture
from models.hydrology_model.utils import (
    extract_zipfile,
    inundation_extent,
    map_f_path,
    mask_raster,
    reproject_like,
)

CONFIG = get_config()


class PullShapefileFilesFromCkan(ExternalTask):
    """
    Pull South Sudan boundary shapefile from ckan
    """

    def output(self):
        return {
            "boundary": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "4ca5e4e8-2f80-44c4-aac4-379116ffd1d9"},
            )
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


class ProjectionFromConfig(ExternalTask):
    """
    Task for creating projection file
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        ncols, nrows = REF_PROJ_CONFIG["ncolsrows"]
        profile = {
            "driver": "GTiff",
            "height": nrows,
            "width": ncols,
            "count": 1,
            "dtype": np.float32,
            "crs": REF_PROJ_CONFIG["srs"],
            "transform": REF_PROJ_CONFIG["transform"],
            "nodata": -9999.0,
        }
        with self.output().open("w") as output:
            output.write(profile)


class ScrapeDailyRainfallData(ExternalTask):
    """
    Pull CHIRPS daily rainfall data from ftp and unzip
    """

    download_start_date = luigi.DateParameter(
        default=datetime.date(2017, 8, 15), significant=False
    )
    download_end_date = luigi.DateParameter(
        default=datetime.date(2017, 8, 30), significant=False
    )

    def output(self):
        return IntermediateTarget(
            path="ScrapeDailyRainfallData/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            # connect to FTP
            ftp = FTP("ftp.chg.ucsb.edu")  # noqa: S321 - ignore secuirty check
            ftp.sendcmd("USER anonymous")
            ftp.sendcmd("PASS anonymous@")

            # Date for the data to be downloaded
            delta = self.download_end_date - self.download_start_date
            dates = [
                self.download_start_date + datetime.timedelta(i)
                for i in range(0, delta.days + 1)
            ]
            dates_str = [i.strftime("%Y.%m.%d") for i in dates]
            # Download data
            for year in range(
                self.download_start_date.year, self.download_end_date.year + 1
            ):
                ftp.cwd(
                    f"pub/org/chg/products/CHIRPS-2.0/africa_daily/tifs/p05/{year}/"
                )

                files = ftp.nlst()
                files = [i for i in files if any(d in i for d in dates_str)]
                # Remove folders from the list
                download = [f for f in files if f.endswith(".gz")]

                for f in download:
                    if not os.path.isfile(os.path.join(tmpdir, f.replace(".gz", ""))):
                        ftp.retrbinary(
                            "RETR " + f, open(os.path.join(tmpdir, f), "wb").write
                        )
                ftp.cwd("/")
            os.system(f"gunzip {tmpdir}/*.gz")  # noqa: S605 - ignore secuirty check


@requires(ScrapeDailyRainfallData, PullShapefileFilesFromCkan)
class ClipDailyRainfallData(Task):
    """
    Clip rainfall data
    """

    def output(self):
        return IntermediateTarget("ClipDailyRainfallData/", timeout=3600)

    def run(self):
        data_map = map_f_path(self.input()[0].path)
        zip_shp = self.input()[1]["boundary"].path
        shapefile = extract_zipfile(zip_shp, "ss_admin0.shp", self.input()[0].root_path)

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            for key, src_name in data_map.items():
                i = key.replace("chirps-v2.0.", "")
                i = i.replace(".", "")
                dst_name = os.path.join(tmpdir, f"{i}.tif")
                mask_raster(src_name, dst_name, shapefile)


@requires(EstimateSoilMoisture, ClipDailyRainfallData)
class ResampleSMDataToMatchChirps(ReprojectNDVI36km):

    """
    Resample soil moisture to match Chirps
    """

    def output(self):
        return IntermediateTarget(path="hydrology_model/sm_chirps/", timeout=36000)


@requires(RasterFiles, ClipDailyRainfallData)
class ResampleRastersToMatchChirps(ReprojectNDVI36km):

    """
    Resample TWI, texture and landforms to match Chirps
    """

    def output(self):
        return IntermediateTarget(path="hydrology_model/rasters/")


@requires(
    ClipDailyRainfallData, ResampleSMDataToMatchChirps, ResampleRastersToMatchChirps
)
class ExtractSMChirpsData(Task):

    """
    Prepare data for training the model
    """

    sm_train_data_year = luigi.Parameter(default="2016", significant=False)
    sm_train_start_month = luigi.IntParameter(default=4, significant=False)
    sm_train_end_month = luigi.IntParameter(default=11, significant=False)
    num_days = luigi.IntParameter(default=10, significant=False)

    def output(self):
        return IntermediateTarget(
            path="hydrology_model/sm_train_data.csv", timeout=36000
        )

    def run(self):
        chirps_data = self.input()[0]
        sm_data = self.input()[1]
        spatial_data = self.input()[2]

        c_vars = sorted(spatial_data[i].path for i in spatial_data)
        lags = [
            "rain_lag10",
            "rain_lag9",
            "rain_lag8",
            "rain_lag7",
            "rain_lag6",
            "rain_lag5",
            "rain_lag4",
            "rain_lag3",
            "rain_lag2",
            "rain_lag1",
        ]
        df = None
        for key in sm_data:
            if key.startswith(self.sm_train_data_year):
                month = pd.to_datetime(key).month
                if self.sm_train_start_month <= month <= self.sm_train_end_month:
                    day_lst = sorted(self.get_previous_days(key, self.num_days))
                    rename_map = {day: lag for day, lag in zip(day_lst, lags)}
                    try:
                        chirps_data_list = [chirps_data[i].path for i in day_lst]
                        features = c_vars + chirps_data_list
                        stack = pyspatialml.stack_from_files(features)
                        px = rasterio.open(sm_data[key].path)
                        temp = stack.extract_raster(px, value_name="soil_moisture")
                        temp = temp.rename(columns=rename_map)
                        temp = temp.sample(n=1000)
                        if df is None:
                            df = temp.copy()
                        else:
                            df = pd.concat([df, temp])

                    except KeyError:
                        pass

        with self.output().open("w") as output:
            output.write(df)

    def get_previous_days(self, date, num_days):
        date = pd.to_datetime(date)
        day_lst = []
        for i in range(1, num_days + 1):
            day_lst.append(
                (date - datetime.timedelta(days=i)).date().strftime(format="%Y/%m/%d")
            )
        day_lst = [i.replace("/", "") for i in day_lst]
        return day_lst


@requires(ExtractSMChirpsData)
class UploadTrainDataToCKAN(Task):
    """
    """

    def output(self):
        return CkanTarget(
            resource={"id": "bde01941-1980-45a4-b83c-7e865b25baf5"},
            dataset={"id": "5a778a98-8ef4-4d69-97b8-d87682e00728"},
        )

    def run(self):
        file_path = self.input().path
        if self.output().exists():
            self.output().remove()
        self.output().put(file_path=file_path)


class PullTrainDataFromCKAN(ExternalTask):
    """
    Pull data for training the model from CKAN
    """

    def output(self):
        return CkanTarget(
            resource={"id": "bde01941-1980-45a4-b83c-7e865b25baf5"},
            dataset={"id": "5a778a98-8ef4-4d69-97b8-d87682e00728"},
        )


@requires(PullTrainDataFromCKAN)
class TrainSMFromChirpsModel(Task, VariableInfluenceMixin):

    """
    Train the model for estimating soil moisture from rainfall data
    """

    hydrology_test_size = luigi.FloatParameter(default=0.33, significant=False)
    hydrology_ind_vars = luigi.ListParameter(
        default=[
            "landforms",
            "texture",
            "twi",
            "rain_lag10",
            "rain_lag9",
            "rain_lag8",
            "rain_lag7",
            "rain_lag6",
            "rain_lag5",
            "rain_lag4",
            "rain_lag3",
            "rain_lag2",
            "rain_lag1",
        ],
        significant=False,
    )
    hydrology_dep_var = luigi.Parameter(default="soil_moisture", significant=False)
    calculate_influence = luigi.BoolParameter(default=False, significant=False)

    def output(self):
        output = {"model": IntermediateTarget(task=self, timeout=3600)}
        if self.calculate_influence:
            output["infuence_grid"] = self.get_influence_grid_target()
        return output

    def run(self):
        df_path = self.input().path

        df = pd.read_csv(df_path)

        X, y = df[list(self.hydrology_ind_vars)], df[self.hydrology_dep_var]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.hydrology_test_size, random_state=None
        )

        ETR = ExtraTreesRegressor(
            max_depth=200,
            max_features="auto",
            max_leaf_nodes=None,
            min_impurity_decrease=0.0,
            min_samples_leaf=1,
            min_samples_split=5,
            n_estimators=500,
            n_jobs=-1,
        )

        model = Pipeline([("scaling", StandardScaler()), ("regression", ETR)])

        model.fit(X_train, y_train)

        # Calculate the influence grid if needed
        if self.calculate_influence is True:
            model_importance = model.named_steps["regression"].feature_importances_
            influence_grid = pd.DataFrame(
                [model_importance, self.hydrology_ind_vars],
                index=["influence_percent", "source_variable"],
                columns=np.arange(0, len(model_importance)),
            ).T
            self.store_influence_grid(self.hydrology_dep_var, influence_grid)

        with self.output()["model"].open("w") as output:
            output.write(model)


@requires(
    ClipDailyRainfallData,
    TrainSMFromChirpsModel,
    ResampleRastersToMatchChirps,
    ProjectionFromConfig,
)
class RainfallSimulation(Task):

    """
    Impact of rainfall on soil moisture and inundation extent
    """

    date = luigi.parameter.DateParameter(
        default=datetime.datetime(2017, 8, 28), significant=False
    )
    PercentOfNormalRainfall = luigi.FloatParameter(default=1.0, significant=False)
    num_days = luigi.IntParameter(default=10, significant=False)

    def output(self):
        output = {
            "soil_moisture": FinalTarget(path="soil_moisture.tif", task=self),
            "inundation": FinalTarget(path="inundation.tif", task=self),
        }
        return output

    def run(self):

        chirp_map = map_f_path(self.input()[0].path)
        raster_map = map_f_path(self.input()[2].path)

        with self.input()[1]["model"].open("r") as f:
            model = f.read()

        with self.input()[3].open("r") as f:
            ref_proj = f.read()

        files = sorted(
            (self.date - datetime.timedelta(days=i + 1)).strftime("%Y%m%d")
            for i in range(self.num_days)
        )
        # Simulate rainfall data
        for key in files:
            with rasterio.open(chirp_map[key]) as src:
                arr = np.squeeze(src.read(1))
                nodata = src.nodata
                mask = arr == nodata
                arr = arr * self.PercentOfNormalRainfall
                arr[mask] = nodata
                meta = src.meta.copy()
                dst_name = os.path.join(
                    self.input()[0].root_path, "hydrology_model", f"{key}.tif"
                )
                with rasterio.open(dst_name, "w", **meta) as dst:
                    dst.write(arr, 1)

        # Predict soil moisture
        c_vars = sorted(raster_map[key] for key in raster_map)
        chirps = [
            os.path.join(self.input()[0].root_path, "hydrology_model", f"{key}.tif")
            for key in files
        ]
        features = c_vars + chirps

        # Stack Features
        stack = pyspatialml.stack_from_files(features)

        sm_path = self.output()["soil_moisture"].path

        # Predict the soil moisture
        stack.predict(estimator=model, file_path=sm_path, nodata=-9999.0)

        # Inundation extent
        extent_file = self.output()["inundation"].path
        inundation_extent(sm_path, extent_file)

        # Resample Inundation extent to 1km resolution
        reproject_like(
            rast_file=extent_file,
            proj_file=ref_proj,
            output_file=extent_file,
            mode=Resampling.nearest,
        )

        # Resample soil moisture to 1km
        reproject_like(rast_file=sm_path, proj_file=ref_proj, output_file=sm_path)
