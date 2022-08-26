import datetime
import gzip
import io
import logging
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import cdsapi
import luigi
import numpy as np
import pandas as pd
import rasterio
import requests
import rioxarray  # noqa: F401 - Xarray wrapper
import xarray as xr
from kiluigi.targets import CkanTarget, IntermediateTarget
from luigi import ExternalTask, LocalTarget, Task
from luigi.configuration import get_config
from luigi.util import inherits, requires
from scipy import stats
from sklearn.cluster import DBSCAN
from sklearn.model_selection import train_test_split

from models.hydrology_model.river_discharge.utils import (
    calculate_stat_f_netcdf,
    data_t_lstm_format,
    mask_dataset,
    raster_metadata,
    read_chirps,
    read_dem,
    read_ERA5_data,
    read_river_discharge,
    read_up_area,
)
from utils.geospatial_tasks.functions.geospatial import geography_f_country
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

config = get_config()

logger = logging.getLogger("luigi-interface")

RELATIVE_PATH = "models/hydrology_model/river_discharge/data_pre"


class WriteCDSAPIKey(ExternalTask):
    """
    Write CDS API key in the file $HOME/.cdsapirc
    For more info see https://cds.climate.copernicus.eu/api-how-to
    """

    def output(self):
        return LocalTarget(os.path.join(os.path.expanduser("~"), ".cdsapirc"))

    def run(self):
        api_key = os.environ["LUIGI_CDS_API_KEY"]
        uid = os.environ["LUIGI_CDS_API_UID"]
        with open(self.output().path, "w") as f:
            f.write("url: https://cds.climate.copernicus.eu/api/v2\n")
            f.write(f"key: {uid}:{api_key}")


@requires(WriteCDSAPIKey, GlobalParameters)
class ScrapeRiverDischarge(Task):
    """
    Scrape river discharge using using CDS API.
    """

    date = luigi.DateParameter(default=datetime.date(2020, 1, 6))

    def output(self):
        dst_src = (
            Path(RELATIVE_PATH)
            / "river_discharge"
            / f"River_discharge_{self.date.strftime('%Y_%m_%d')}.nc"
        )
        return IntermediateTarget(path=str(dst_src), timeout=31536000)

    def run(self):
        c = cdsapi.Client()
        year, month, day = self.date.strftime("%Y-%m-%d").split("-")
        with self.output().open("w") as f:
            temp = tempfile.NamedTemporaryFile(suffix=".zip")
            c.retrieve(
                "cems-glofas-historical",
                {
                    "format": "zip",
                    "variable": "River discharge",
                    "dataset": "Consolidated reanalysis",
                    "year": year,
                    "version": "2.1",
                    "month": month,
                    "day": day,
                },
                temp.name,
            )
            zip_ref = zipfile.ZipFile(temp.name)
            temp_dir = tempfile.TemporaryDirectory()
            zip_ref.extractall(temp_dir.name)
            src_dir = os.path.join(temp_dir.name, os.listdir(temp_dir.name)[0])
            shutil.move(src_dir, f.name)


@requires(WriteCDSAPIKey, GlobalParameters)
class ScrapingUpstreamArea(Task):
    """
    Scrape upstream area using CDS API.
    """

    dataset_name = "cems-glofas-historical"

    def output(self):
        path = Path(RELATIVE_PATH) / "upstream_area.nc"
        return IntermediateTarget(path=str(path), timeout=31536000)

    def run(self):
        c = cdsapi.Client()

        with self.output().open("w") as out:
            temp = tempfile.NamedTemporaryFile(suffix=".zip")
            c.retrieve(
                "cems-glofas-historical",
                {"format": "zip", "variable": "Upstream area"},
                temp.name,
            )
            zip_ref = zipfile.ZipFile(temp.name)
            temp_dir = tempfile.TemporaryDirectory()
            zip_ref.extractall(temp_dir.name)
            src_dir = os.path.join(temp_dir.name, os.listdir(temp_dir.name)[0])
            shutil.move(src_dir, out.name)


class ScrapeDailyRainFallData(ExternalTask):
    """Scrape daily rainfall data.
    """

    date = luigi.DateParameter(default=datetime.date(2020, 1, 1))

    def output(self):
        dst = os.path.join(RELATIVE_PATH, f"chirps_daily/{self.get_filename_f_date()}")
        return IntermediateTarget(path=dst, timeout=5184000)

    def run(self):
        logger.info(f"Downloading rainfall data for {self.date}")
        src_file = self.get_chirps_src(self.date.year) + ".gz"
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
                with self.output().open("w") as out:
                    with rasterio.open(out, "w", **meta) as dst:
                        dst.write(arr)

    def get_chirps_src(self, year):
        src_dir = f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/{year}/"
        return src_dir + self.get_filename_f_date()

    def get_filename_f_date(self):
        return f"chirps-v2.0.{self.date.strftime('%Y.%m.%d')}.tif"


@requires(WriteCDSAPIKey)
class ScrapeERA5LandVaraibles(Task):
    """
    Scrape hourly ERA5 land variables.
    """

    date = luigi.DateParameter(default=datetime.date(2019, 12, 5))
    variables = luigi.ListParameter(
        default=[
            "2m_dewpoint_temperature",
            "2m_temperature",
            "total_evaporation",
            "leaf_area_index_high_vegetation",
            "leaf_area_index_low_vegetation",
            "potential_evaporation",
            "runoff",
            "surface_solar_radiation_downwards",
            "total_precipitation",
            "volumetric_soil_water_layer_1",
            "volumetric_soil_water_layer_2",
            "volumetric_soil_water_layer_3",
            "volumetric_soil_water_layer_4",
        ]
    )
    time = luigi.ListParameter(default=[f"{str(i).zfill(2)}:00" for i in range(24)])

    def output(self):
        file_name = f"{self.date.strftime('%Y%m%d')}.nc"
        path = str(Path(RELATIVE_PATH) / "ERA5LandVariables" / file_name)
        return IntermediateTarget(path=path, timeout=31536000, format=luigi.format.Nop)

    def run(self):
        year, month, day = self.date.strftime("%Y-%m-%d").split("-")
        c = cdsapi.Client()
        with self.output().open("w") as out:
            c.retrieve(
                "reanalysis-era5-land",
                {
                    "area": [15.034878, 23.503092, 3.308814, 48.343304],
                    "format": "netcdf",
                    "variable": self.variables,
                    "year": year,
                    "month": month,
                    "day": day,
                    "time": self.time,
                },
                out.name,
            )


class PullStaticdataFromFromCKAN(ExternalTask):
    """
    Pull DEM and landcover from CKAN.
    """

    def output(self):
        return {
            "dem": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "8cdbc26c-cf9f-4107-a405-d4e4e1777631"},
            ),
            "landcover": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "5587d203-9a18-4538-9da1-7b1005ef0c0d"},
            ),
        }


class PullDEMFromCkan(ExternalTask):
    """
    Pull digital elevation model from CKAN.
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
            resource={"id": "8cdbc26c-cf9f-4107-a405-d4e4e1777631"},
        )


class PullUpstreamArea(ExternalTask):
    """
    Pull upstream area from CKAN.
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
            resource={"id": "0d3ba51a-97ea-4fa4-9b66-b53d80ccfcfc"},
        )


@requires(ScrapeRiverDischarge)
class ModelTrainingPixelSample(Task):
    """
    Select pixels to be used to train the model.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geo_mask = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geo_mask = [geography]

        with self.input().open() as src:
            src_byte = src.read()
        ds = xr.open_dataset(io.BytesIO(src_byte))
        da = ds["dis24"]
        da = da.rename({"lon": "longitude", "lat": "latitude"})
        da = mask_dataset(da, geo_mask)
        da = da.where(da != 1.0000000200408773e20)
        arr = da.values
        arr = arr.flatten()
        df = pd.DataFrame({"discharge": arr})
        df = df.dropna(subset=["discharge"])
        mean, std = df["discharge"].mean(), df["discharge"].std()
        df["discharge"] = (df["discharge"] - mean) / std
        X = df["discharge"].values.reshape(-1, 1)
        dbscan = DBSCAN(eps=0.05, min_samples=5)
        dbscan.fit(X)
        df["clusters"] = dbscan.labels_
        df["index"] = df.index
        train_indices, _ = train_test_split(
            df["index"], test_size=0.9, random_state=42, stratify=df["clusters"]
        )
        train_indices = sorted(train_indices)
        with self.output().open("w") as out:
            out.write(train_indices)


@inherits(GlobalParameters)
class CacheRiverDischargeERA5Rain(Task):
    """
    Pre-process data to be used to train the model or make inference.
    """

    start_date = luigi.DateParameter(default=datetime.date(2016, 11, 1))
    end_date = luigi.DateParameter(default=datetime.date(2018, 12, 31))

    def output(self):
        f = Path(RELATIVE_PATH) / "disharge_era5_rain.nc"
        return IntermediateTarget(path=str(f), task=self, timeout=31536000)

    def requires(self):
        date_list = pd.date_range(self.start_date, self.end_date)
        inputs = {}
        inputs["era5"] = {i: ScrapeERA5LandVaraibles(date=i) for i in date_list}
        inputs["discharge"] = {i: ScrapeRiverDischarge(date=i) for i in date_list}
        inputs["chirps_rain"] = {i: ScrapeDailyRainFallData(date=i) for i in date_list}
        return inputs

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]
        dis_data = self.input()["discharge"]
        dates = pd.date_range(self.start_date, self.end_date)
        dis_da = xr.concat(
            [read_river_discharge(dis_data, i, geom) for i in dates], "time"
        )
        dis_da = dis_da.squeeze(dim="band")
        dis_da.name = "dis"
        era5_data = self.input()["era5"]
        era_ds = xr.concat([read_ERA5_data(era5_data, i, geom) for i in dates], "time")
        era_ds = era_ds.rename({"latitude": "y", "longitude": "x"})
        era_ds = era_ds.rio.reproject_match(dis_da)

        chirps_data = self.input()["chirps_rain"]
        rain_da = xr.concat([read_chirps(chirps_data, i, geom) for i in dates], "time")
        rain_da = rain_da.squeeze(dim="band")
        rain_da.name = "chirps_rain"
        rain_da = rain_da.rio.reproject_match(dis_da)

        for dim in ["x", "y"]:
            era_ds[dim] = dis_da[dim]
            rain_da[dim] = dis_da[dim]

        data = xr.merge([dis_da, era_ds, rain_da], join="exact")
        try:
            data.attrs = {"transform": dis_da.attrs["transform"]}
        except KeyError:
            pass
        with self.output().open("w") as out:
            data.to_netcdf(out.name)


@requires(CacheRiverDischargeERA5Rain)
class UploadCacheData(Task):
    """
    Upload cached data CKAN.
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
            resource={"name": f"Disharge ERA5 Rain {self.country_level}"},
        )

    def run(self):
        self.output().put(self.input().path)


@inherits(GlobalParameters)
class PullPredictors(ExternalTask):
    """
    Pull pre-process predictors from CKAN.
    """

    def output(self):
        if self.country_level == "Ethiopia":
            return CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "90688c52-000b-49cc-a30c-01ac2602f7f1"},
            )
        elif self.country_level == "South Sudan":
            return CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "924c9ce8-54b1-48d0-ae86-bbc811fac31d"},
            )
        else:
            raise NotImplementedError


@requires(PullDEMFromCkan, PullUpstreamArea, PullPredictors)
@inherits(GlobalParameters)
class GetPredictors(Task):
    """
    Add static predictors to time series predictors.
    """

    def output(self):
        dst = Path(RELATIVE_PATH) / "predictors.nc"
        return IntermediateTarget(path=str(dst), task=self, timeout=31536000)

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]

        data_start = self.time.date_a - datetime.timedelta(60)
        data_end = self.time.date_b - datetime.timedelta(1)
        data = xr.open_dataset(self.input()[2].path)
        data = data.sel(time=slice(data_start, data_end))
        dis_da = data["dis"]

        dem_src = self.input()[0].path
        dem_da = read_dem(dem_src, data_start, data_end, geom, dis_da, True)

        area_src = self.input()[1].path
        area_da = read_up_area(area_src, data_start, data_end, geom, dis_da)
        data = xr.merge([data, dem_da, area_da], join="exact")
        with self.output().open("w") as out:
            data.to_netcdf(out.name)


@requires(ModelTrainingPixelSample, PullDEMFromCkan, PullUpstreamArea, PullPredictors)
@inherits(GlobalParameters)
class DischargeTrainingDataset(Task):
    """
    Prepare data to used to train the model.
    """

    start_date = luigi.DateParameter(default=datetime.date(2017, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date(2018, 12, 31))

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    @staticmethod
    def keep_indices(X, y, indices):
        X = X.stack(z=("y", "x", "date"))
        X = X.isel(z=indices)
        X = X[["dis", "t2m", "chirps_rain", "up_area", "dem"]].to_array()
        X = X.transpose("z", "time", "variable")
        X = X.values
        y = y.stack(z=("y", "x", "date"))
        y = y.isel(z=indices)
        y = y.values
        return X, y

    def run(self):
        with self.input()[0].open() as src:
            indices = src.read()
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]

        data = xr.open_dataset(self.input()[3].path)
        start = self.start_date - datetime.timedelta(60)
        data = data.sel(time=slice(start, self.end_date))
        dis_da = data["dis"]

        dem_src = self.input()[1].path
        dem_da = read_dem(dem_src, start, self.end_date, geom, dis_da, True)

        area_src = self.input()[2].path
        area_da = read_up_area(area_src, start, self.end_date, geom, dis_da)

        data = xr.merge([data, dem_da, area_da], join="exact")
        for index, date in enumerate(pd.date_range(self.start_date, self.end_date)):
            temp_x, temp_y = data_t_lstm_format(data, date, 60, True)
            temp_x, temp_y = self.keep_indices(temp_x, temp_y, indices)
            if index == 0:
                X, y = temp_x, temp_y
            else:
                X = np.concatenate((X, temp_x))
                y = np.concatenate((y, temp_y))

        with self.output().open("w") as out:
            out.write((X, y))


class ExtractRiverDischargeAnnualMaxima(Task):
    """
    Extract river discharge annual maxima to be used to calculate flood threshold
    """

    start_date = luigi.DateParameter(default=datetime.date(1981, 1, 1))
    end_date = luigi.DateParameter(default=datetime.date(2017, 1, 1))

    def requires(self):
        time_period = pd.date_range(self.start_date, self.end_date)
        return [ScrapeRiverDischarge(date=date) for date in time_period]

    def output(self):
        return CkanTarget(
            dataset={"id": "7c2bb6b6-a9e8-4417-bb69-a1816e3d2421"},
            resource={
                "name": f"River Discharge Annual Maxima_{self.start_date.year} to {self.end_date.year - 1}"
            },
        )

    def run(self):
        file_path = [i.path for i in self.input()]
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            for year in range(self.start_date.year, self.end_date.year):
                year_files = [i for i in file_path if str(year) in i]
                for index, src_file in enumerate(year_files):
                    arr, transform, ysize, xsize, nodata = calculate_stat_f_netcdf(
                        src_file, None
                    )
                    meta = raster_metadata(transform, ysize, xsize, nodata)
                    meta.update(compress="lzw")
                    arr[arr == nodata] = np.nan
                    if index == 0:
                        year_arr = arr.copy()
                    else:
                        year_arr = np.nanmax([year_arr, arr], axis=0)
                year_arr[np.isnan(year_arr)] = meta["nodata"]
                dst_file = os.path.join(tmpdir, f"river_discharge_maxima_{year}.tif")
                with rasterio.open(dst_file, "w", **meta) as dst:
                    dst.write(year_arr.astype(meta["dtype"]), 1)
            temp_zip = tempfile.NamedTemporaryFile(prefix="river_discharge_")
            shutil.make_archive(temp_zip.name, "zip", tmpdir)
            self.output().put(temp_zip.name + ".zip")


@requires(ExtractRiverDischargeAnnualMaxima, GlobalParameters)
class FloodThreshold(Task):
    """
    Calculate flood threshold by fitting river discharge annumal maxima in gumbel distribution
    """

    return_period_threshold = luigi.IntParameter(default=10)
    param_cal_method = luigi.ChoiceParameter(choices=["mle", "mom"], default="mom")

    def output(self):
        dst_file = f"{RELATIVE_PATH}/flood_threshold_{self.return_period_threshold}.tif"
        return IntermediateTarget(path=dst_file, timeout=31536000)

    def run(self):
        zip_file = self.input()[0].path
        zip_ref = zipfile.ZipFile(zip_file)
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_ref.extractall(tmpdir)
            annual_max = []
            for i in os.listdir(tmpdir):
                with rasterio.open(os.path.join(tmpdir, i)) as src:
                    src_arr = src.read(1)
                    meta = src.meta.copy()
                src_arr[src_arr == meta["nodata"]] = np.nan
                annual_max.append(src_arr)
            annual_max = np.array(annual_max)
            arr = np.apply_along_axis(self.flood_threshold, 0, annual_max)
            arr[np.isnan(arr)] = meta["nodata"]
            arr = np.squeeze(arr)
        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write(arr.astype(meta["dtype"]), 1)

    @staticmethod
    def gumbel_r_mom(x):
        """
        Estimate params using moments of method

        Parameters
        ----------
        x : numpy.array
            Annual maximum numpy array.

        Returns
        -------
        loc : float
            loc.
        scale : float
            sclae.

        """
        scale = np.sqrt(6) / np.pi * np.std(x)
        loc = np.mean(x) - np.euler_gamma * scale
        return loc, scale

    def flood_threshold(self, annual_max):
        """
        Calculate flood threshold.

        Parameters
        ----------
        annual_max : numpy.array
            River discharge annual max.

        Returns
        -------
        thres : numpy.array
            Flood threshold.

        """
        try:
            # parameters not calculated using L-moments
            if self.param_cal_method == "mle":
                loc, scale = stats.gumbel_r.fit(annual_max)
            else:
                loc, scale = self.gumbel_r_mom(annual_max)

            proba = 1 - 1 / self.return_period_threshold
            thres = stats.gumbel_r.ppf([proba], loc=loc, scale=scale)
        except (ValueError, RuntimeError):
            thres = np.nan
        return thres


@requires(FloodThreshold, GlobalParameters)
class UploadFloodThresholdToCkan(Task):
    """
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
            resource={
                "name": f"River Discharge Threshold return period v1 {self.return_period_threshold}"
            },
        )

    def run(self):
        self.output().put(file_path=self.input()[0].path)
