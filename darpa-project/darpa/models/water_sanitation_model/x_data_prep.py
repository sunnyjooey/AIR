import datetime
import gzip
import shutil
import tempfile
from pathlib import Path

import geopandas as gpd
import luigi
import pandas as pd
import rasterio
import requests
from bs4 import BeautifulSoup
from kiluigi.targets import CkanTarget, IntermediateTarget
from luigi.task import ExternalTask, Task
from luigi.util import inherits, requires
from osgeo import gdal
from rasterio.io import MemoryFile
from rasterio.merge import merge
from rasterio.warp import Resampling, calculate_default_transform, reproject
from rasterstats import zonal_stats
from scipy import stats

from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from .utils import SessionWithHeaderRedirection, get_src_file
from .y_data_prep import DHSWASHData, JPMWASHData, LSMSWASHData

RELATIVE_PATH = "models/water_sanitation_model/x_data_prep"


@inherits(GlobalParameters)
class ClusterGPS(Task):
    data_source = luigi.ChoiceParameter(default="LSMS", choices=["LSMS", "JMP"])

    def requires(self):
        lsms = self.clone(LSMSWASHData)
        jpm = self.clone(JPMWASHData)
        dhs = self.clone(DHSWASHData)
        if self.country_level == "Kenya":
            return dhs
        elif self.data_source == "LSMS":
            return lsms
        else:
            return jpm

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        with self.input().open() as src:
            df = src.read()
        if self.country_level in ["Ethiopia", "South Sudan", "Kenya"]:
            if self.country_level == "Kenya":
                df = df.rename(columns={"LATNUM": "latitude", "LONGNUM": "longitude"})
                df = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            df_geom = df.drop_duplicates(subset=["latitude", "longitude"])
            try:
                df_geom = df_geom[["geometry", "latitude", "longitude"]]
            except KeyError:
                df_geom = gpd.GeoDataFrame(
                    df_geom,
                    geometry=gpd.points_from_xy(
                        df_geom["longitude"], df_geom["latitude"]
                    ),
                )
                df_geom = df_geom[["geometry", "latitude", "longitude"]]
            df_geom = df_geom.set_crs(epsg=4326)
            df_geom["geometry"] = df_geom["geometry"].buffer(0.04167086806497172)
        elif self.country_level in ["Djibouti", "Uganda"]:
            df = df.drop_duplicates(subset=["geometry"])
            df = df.loc[~pd.isnull(df["geometry"])]
            df_geom = gpd.GeoDataFrame(df, geometry="geometry")
        else:
            raise NotImplementedError
        with self.output().open("w") as out:
            out.write(df_geom)


class ScrapeRainFall(ExternalTask):
    """Scrape daily rainfall data.
    """

    month = luigi.DateParameter(default=datetime.date(2010, 1, 1))

    def output(self):
        dst = Path(RELATIVE_PATH) / f"chirps_monthly/{self.get_filename_f_date()}"
        return IntermediateTarget(path=str(dst), timeout=31536000)

    def run(self):
        # logger.info(f"Downloading rainfall data for {self.month}")
        src_file = self.get_chirps_src(self.month.year) + ".gz"
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
        return f"chirps-v2.0.{self.month.strftime('%Y.%m.%d')}.tif"


@requires(ScrapeRainFall, ClusterGPS)
class RainfallStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        with self.input()[1].open() as src:
            admin_df = src.read()
        src_file = self.input()[0].path
        temp = zonal_stats(admin_df, src_file, geojson_out=True, all_touched=True)
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(
            columns={
                "min": "rainfall_min",
                "max": "rainfall_max",
                "mean": "rainfall_mean",
                "count": "rainfall_count",
            }
        )
        with self.output().open("w") as out:
            out.write(gdf)


class LandSurfaceTemperature(ExternalTask):
    month = luigi.DateParameter(default=datetime.date(2010, 1, 1))

    def output(self):
        dst = (
            Path(RELATIVE_PATH)
            / "LSD"
            / f"MOD11C3_006_{self.month.strftime('%Y_%m')}.hdf"
        )
        return IntermediateTarget(path=str(dst), timeout=31536000)

    def run(self):
        session = SessionWithHeaderRedirection(
            "petermburu254", "Cncj3d05zZnXDXEl1xhvGyvwbgyMz97c"
        )
        base_url = f"https://e4ftl01.cr.usgs.gov/MOLT/MOD11C3.006/{self.month.strftime('%Y.%m')}.01/"
        req = session.get(base_url)
        assert req.status_code == 200, "Download Failed"
        soup = BeautifulSoup(req.content, "html.parser")

        links = [i.get("href") for i in soup.find_all("a")]
        link = [i for i in links if i.endswith(".hdf")]
        assert len(link) == 1
        url = base_url + link[0]
        req = session.get(url)
        assert req.status_code == 200, "Download Failed"
        with self.output().open("w") as out:
            with open(out.name, "wb") as dst:
                for chunk in req.iter_content(chunk_size=1024):
                    dst.write(chunk)


@requires(LandSurfaceTemperature)
class LandSurfaceTemperatureDay(Task):
    def output(self):
        dst = (
            Path(RELATIVE_PATH)
            / "LSD_Day"
            / f"lst_day_cmg_{self.month.strftime('%Y_%m')}.tif"
        )
        return IntermediateTarget(path=str(dst), timeout=31536000)

    def run(self):
        with self.input().open() as src:
            ds = gdal.Open(src.name)
        lst_day_src = ds.GetSubDatasets()[0][0]
        ds = None
        assert lst_day_src.endswith("LST_Day_CMG")
        lst_day_src = gdal.Open(lst_day_src)
        with tempfile.NamedTemporaryFile(suffix=".tif") as temp:
            ds = gdal.Translate(temp.name, lst_day_src, bandList=[1])
            ds = None
            with rasterio.open(temp.name) as src:
                transform, width, height = calculate_default_transform(
                    src.crs, "EPSG:4326", src.width, src.height, *src.bounds
                )
                dst_meta = src.profile.copy()
                dst_meta.update(
                    transform=transform,
                    width=width,
                    height=height,
                    crs="EPSG:4326",
                    compress="lzw",
                )
                with self.output().open("w") as out:
                    with rasterio.open(out.name, "w", **dst_meta) as dst:
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=rasterio.band(dst, 1),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform,
                            dst_crs=dst_meta["crs"],
                            resampling=Resampling.nearest,
                        )


@requires(LandSurfaceTemperature)
class LandSurfaceTemperatureNight(Task):
    def output(self):
        dst = (
            Path(RELATIVE_PATH)
            / "LSD_Night"
            / f"lst_night_cmg_{self.month.strftime('%Y_%m')}.tif"
        )
        return IntermediateTarget(path=str(dst), timeout=31536000)

    def run(self):
        with self.input().open() as src:
            ds = gdal.Open(src.name)
        lst_day_src = ds.GetSubDatasets()[5][0]
        ds = None
        assert lst_day_src.endswith("LST_Night_CMG")
        lst_day_src = gdal.Open(lst_day_src)
        with tempfile.NamedTemporaryFile(suffix=".tif") as temp:
            ds = gdal.Translate(temp.name, lst_day_src, bandList=[1])
            ds = None
            with rasterio.open(temp.name) as src:
                transform, width, height = calculate_default_transform(
                    src.crs, "EPSG:4326", src.width, src.height, *src.bounds
                )
                dst_meta = src.profile.copy()
                dst_meta.update(
                    transform=transform,
                    width=width,
                    height=height,
                    crs="EPSG:4326",
                    compress="lzw",
                )
                with self.output().open("w") as out:
                    with rasterio.open(out.name, "w", **dst_meta) as dst:
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=rasterio.band(dst, 1),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform,
                            dst_crs=dst_meta["crs"],
                            resampling=Resampling.nearest,
                        )


class ScrapeVegetationData(ExternalTask):
    month = luigi.DateParameter(default=datetime.date(2010, 1, 1))
    box = luigi.Parameter(default="h22v08")

    def output(self):
        dst = (
            Path(RELATIVE_PATH)
            / "vegetation"
            / f"MOD13A3_006_{self.box}_{self.month.strftime('%Y_%m')}.hdf"
        )
        return IntermediateTarget(path=str(dst), timeout=31536000)

    def run(self):
        base_url = f"https://e4ftl01.cr.usgs.gov/MOLT/MOD13A3.006/{self.month.strftime('%Y.%m')}.01/"
        session = SessionWithHeaderRedirection(
            "petermburu254", "Cncj3d05zZnXDXEl1xhvGyvwbgyMz97c"
        )
        req = session.get(base_url)
        assert req.status_code == 200, "Download Failed"
        soup = BeautifulSoup(req.content, "html.parser")
        links = [i.get("href") for i in soup.find_all("a")]
        links = [i for i in links if i.endswith(".hdf")]
        link = [i for i in links if self.box in i]
        assert len(link) == 1

        url = base_url + link[0]
        req = session.get(url)
        assert req.status_code == 200, "Download Failed"
        with self.output().open("w") as out:
            with open(out.name, "wb") as dst:
                for chunk in req.iter_content(chunk_size=1024):
                    dst.write(chunk)


@inherits(ScrapeVegetationData)
class ExtractVariableFromVegetationData(Task):
    variable_index = luigi.IntParameter(default=0)
    variable = luigi.Parameter(default="NDVI")

    def requires(self):
        evi_task = self.clone(ScrapeVegetationData)
        box_list = [
            "h22v08",
            "h22v07",
            "h21v08",
            "h21v07",
            "h20v07",
            "h20v08",
            "h22v09",
            "h21v09",
            "h20v09",
        ]
        return {i: evi_task.clone(box=i) for i in box_list}

    def output(self):
        return NotImplementedError

    @staticmethod
    def close_ds(ds):
        ds = None
        return ds

    def run(self):
        temp_list = []
        temp_map = {}
        for k, v in self.input().items():
            src_file = get_src_file(v.path, self.variable_index, self.variable)
            src = gdal.Open(src_file)
            temp_map[k] = tempfile.NamedTemporaryFile(suffix=".tif")
            gdal.Translate(temp_map[k].name, src, bandList=[1])
            temp_list.append(temp_map[k].name)

        temp_list_src = [rasterio.open(i) for i in temp_list]
        arr, transform = merge(temp_list_src)

        src_meta = temp_list_src[0].profile.copy()
        src_meta.update(transform=transform, height=arr.shape[1], width=arr.shape[2])
        with MemoryFile() as memfile:
            with memfile.open(**src_meta) as src:
                src.write(arr)
                transform, width, height = calculate_default_transform(
                    src.crs, "EPSG:4326", src.width, src.height, *src.bounds
                )
                dst_meta = src.profile.copy()
                dst_meta.update(
                    transform=transform,
                    width=width,
                    height=height,
                    crs="EPSG:4326",
                    compress="lzw",
                )
                with self.output().open("w") as out:
                    with rasterio.open(out.name, "w", **dst_meta) as dst:
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=rasterio.band(dst, 1),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform,
                            dst_crs=dst_meta["crs"],
                            resampling=Resampling.nearest,
                        )


@inherits(ScrapeVegetationData)
class RedReflectance(ExtractVariableFromVegetationData):
    variable_index = 3
    variable = "red reflectance"

    def output(self):
        filename = f"MOD13A3_006_red_{self.month.strftime('%Y_%m')}.tif"
        dst = Path(RELATIVE_PATH) / "red_band" / filename
        return IntermediateTarget(path=str(dst), timeout=31536000)


@inherits(ScrapeVegetationData)
class BlueReflectance(ExtractVariableFromVegetationData):
    variable_index = 5
    variable = "blue reflectance"

    def output(self):
        filename = f"MOD13A3_006_blue_{self.month.strftime('%Y_%m')}.tif"
        dst = Path(RELATIVE_PATH) / "blue_band" / filename
        return IntermediateTarget(path=str(dst), timeout=31536000)


@inherits(ScrapeVegetationData)
class NormalizedDifferenceVegetationIndex(ExtractVariableFromVegetationData):
    variable_index = 0
    variable = "NDVI"

    def output(self):
        filename = f"MOD13A3_006_ndvi_{self.month.strftime('%Y_%m')}.tif"
        dst = Path(RELATIVE_PATH) / "NDVI" / filename
        return IntermediateTarget(path=str(dst), timeout=31536000)


@inherits(ScrapeVegetationData)
class EnhancedVegetationIndex(ExtractVariableFromVegetationData):
    variable_index = 1
    variable = "EVI"

    def output(self):
        filename = f"MOD13A3_006_evi_{self.month.strftime('%Y_%m')}.tif"
        dst = Path(RELATIVE_PATH) / "EVI" / filename
        return IntermediateTarget(path=str(dst), timeout=31536000)


@inherits(ScrapeVegetationData)
class NearInfraredReflectance(ExtractVariableFromVegetationData):
    variable_index = 4
    variable = "NIR reflectance"

    def output(self):
        filename = f"MOD13A3_006_nir_{self.month.strftime('%Y_%m')}.tif"
        dst = Path(RELATIVE_PATH) / "NIR" / filename
        return IntermediateTarget(path=str(dst), timeout=31536000)


@inherits(ScrapeVegetationData)
class MiddleInfraredReflectance(ExtractVariableFromVegetationData):
    variable_index = 6
    variable = "MIR reflectance"

    def output(self):
        filename = f"MOD13A3_006_mir_{self.month.strftime('%Y_%m')}.tif"
        dst = Path(RELATIVE_PATH) / "MIR" / filename
        return IntermediateTarget(path=str(dst), timeout=31536000)


class PullLandCover(ExternalTask):
    year = luigi.IntParameter(default=2010)

    def output(self):
        return CkanTarget(
            dataset={"id": "cdf55c86-5508-4ac0-b212-6907b50a7642"},
            resource={"name": f"land_cover{self.year}"},
        )


@requires(PullLandCover, ClusterGPS)
class LandCoverStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    @staticmethod
    def get_majority_class(arr):
        return stats.mode(arr.data, axis=None)[0][0]

    def run(self):
        with self.input()[1].open() as src:
            admin_df = src.read()

        src_file = self.input()[0].path
        temp = zonal_stats(
            admin_df,
            src_file,
            stats="count",
            add_stats={"lc": self.get_majority_class},
            geojson_out=True,
            all_touched=True,
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["year"] = self.year
        with self.output().open("w") as out:
            out.write(gdf)


class PullNightTimeLight(ExternalTask):
    month = luigi.DateParameter(default=datetime.date(2012, 4, 1))

    def output(self):
        return CkanTarget(
            dataset={"id": "19aa8884-ed48-4c25-88e9-c1f96d5877bc"},
            resource={"name": f"night_time_light_{self.month.strftime('%Y_%m')}"},
        )


@requires(PullNightTimeLight)
class NightTimeLight(Task):
    def output(self):
        dst = (
            Path(RELATIVE_PATH)
            / "night_time"
            / f"night_time_light_{self.month.strftime('%Y_%m')}.tif"
        )
        return IntermediateTarget(path=str(dst), timeout=31536000)

    def run(self):
        src = self.input().path
        with self.output().open("w") as out:
            shutil.copyfile(src, out.name)


@requires(PullNightTimeLight, ClusterGPS)
class NightTimeLightStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        with self.input()[1].open() as src:
            admin_df = src.read()

        src_file = self.input()[0].path
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "ntl_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


# Dependency on popultion model incorporating Uganda
@inherits(GlobalParameters)
class PullPopulationData(ExternalTask):
    year = luigi.IntParameter(default=2010)

    def output(self):
        if self.country_level == "Ethiopia":
            return CkanTarget(
                dataset={"id": "d54a02b2-9df5-4bea-9575-775f9e3ff4d8"},
                resource={"name": f"{self.year}_Ethiopia_hires_pop"},
            )
        elif self.country_level == "South Sudan":
            return CkanTarget(
                dataset={"id": "06447ee6-6761-477d-b0a1-1a9464400f9d"},
                resource={"name": f"{self.year}_South Sudan_hires_pop"},
            )
        else:
            raise NotImplementedError


@requires(PullPopulationData, ClusterGPS)
class PopulationStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        with self.input()[1].open() as src:
            admin_df = src.read()
        src_file = self.input()[0].path
        temp = zonal_stats(
            admin_df, src_file, stats="mean", band=1, geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["year"] = self.year
        gdf = gdf.rename(columns={"mean": "pop_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


class PullDEM(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
            resource={"id": "8cdbc26c-cf9f-4107-a405-d4e4e1777631"},
        )


@requires(PullDEM, ClusterGPS)
class DEMStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        with self.input()[1].open() as src:
            admin_df = src.read()
        src_file = self.input()[0].path
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "dem_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(PullDEM)
class CalculateSlope(Task):
    def output(self):
        dst = Path(RELATIVE_PATH) / "slope.tif"
        return IntermediateTarget(path=str(dst), timeout=31536000, task=self)

    def run(self):
        dem_src = self.input().path
        with self.output().open("w") as out:
            gdal.DEMProcessing(out.name, dem_src, "slope", scale=111120)


@requires(CalculateSlope, ClusterGPS)
class SlopeStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        with self.input()[1].open() as src:
            admin_df = src.read()
        dem_src = self.input()[0].path

        temp = zonal_stats(
            admin_df, dem_src, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "slope_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(PullDEM)
class Slope(Task):
    def output(self):
        dst = Path(RELATIVE_PATH) / "slope.tif"
        return IntermediateTarget(path=str(dst), timeout=31536000)

    def run(self):
        dem_src = self.input().path
        with self.output().open("w") as dst:
            gdal.DEMProcessing(dst.name, dem_src, "slope", scale=111120)


class PullAccessibility(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "e4709f8e-be6b-4a9a-9ce5-5c9b15189690"},
        )


class PullHousing(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "d10f34ed-e42a-43c5-a9de-ef1a3b433d2a"},
        )


@requires(PullHousing, ClusterGPS)
class HousingStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        with self.input()[1].open() as src:
            admin_df = src.read()

        src_file = self.input()[0].path
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "housing_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


class PullFrictionSurface(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "436cba8c-50cd-475b-9b22-3394344a6cf0"},
        )


@requires(PullFrictionSurface, ClusterGPS)
class FrictionSurfaceStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "friction_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(PullAccessibility, ClusterGPS)
class AccessibilityStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "travel_time_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


class PullAridityIndex(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "f99988fb-6c38-447f-8322-edcf07110682"},
        )


@requires(PullAridityIndex, ClusterGPS)
class AridityIndexStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "aridity_index_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


class PullDiarrheaPrevalence(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "df2e67a0-d93b-4199-9a17-8e2ec3509875"},
        )


@requires(PullDiarrheaPrevalence, ClusterGPS)
class DiarrheaPrevalenceStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()

        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "dia_prev_2015_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


class TemperatureSuitability(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "e2dc327a-4bc3-419e-9f27-e0ef187fea02"},
        )


@requires(TemperatureSuitability, ClusterGPS)
class TemperatureSuitabilityStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()

        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "temp_suit_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


class PullGHSettlementLayers(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "b97594cb-48bc-469e-b9a9-9fb771010652"},
        )


@requires(PullGHSettlementLayers, ClusterGPS)
class GHSettlementLayerStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    @staticmethod
    def get_majority_class(arr):
        return stats.mode(arr.data, axis=None)[0][0]

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df,
            src_file,
            stats="count",
            add_stats={"ghs_smod": self.get_majority_class},
            geojson_out=True,
            all_touched=True,
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        with self.output().open("w") as out:
            out.write(gdf)


class PullGHSBuilt(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "b04b3957-d887-4869-974b-f2a427c0518e"},
        )


@requires(PullGHSBuilt, ClusterGPS)
class GHSBuiltStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "ghs_built_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


class PullGHSPop(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "6e482472-d4b4-4804-96c4-81f2a5bedb9d"},
        )


@requires(PullGHSPop, ClusterGPS)
class GHSPopStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf = gdf.rename(columns={"mean": "ghs_pop_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(MiddleInfraredReflectance, ClusterGPS)
class MiddleInfraredReflectanceStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "mir_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(NearInfraredReflectance, ClusterGPS)
class NearInfraredReflectanceStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "nir_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(EnhancedVegetationIndex, ClusterGPS)
class EnhancedVegetationIndexStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "evi_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(NormalizedDifferenceVegetationIndex, ClusterGPS)
class NormalizedDifferenceVegetationIndexStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "ndvi_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(BlueReflectance, ClusterGPS)
class BlueReflectanceStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "blue_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(RedReflectance, ClusterGPS)
class RedReflectanceStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()

        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "red_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(LandSurfaceTemperatureNight, ClusterGPS)
class LandSurfaceTemperatureNightStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()
        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "lst_night_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@requires(LandSurfaceTemperatureDay, ClusterGPS)
class LandSurfaceTemperatureDayStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_file = self.input()[0].path
        with self.input()[1].open() as src:
            admin_df = src.read()

        temp = zonal_stats(
            admin_df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "lst_day_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


@inherits(ClusterGPS)
class GetPredictors(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def requires(self):
        ndvi_task = self.clone(NormalizedDifferenceVegetationIndexStats)
        evi_task = self.clone(EnhancedVegetationIndexStats)
        lst_day = self.clone(LandSurfaceTemperatureDayStats)
        last_night = self.clone(LandSurfaceTemperatureNightStats)
        red_task = self.clone(RedReflectanceStats)
        blue_task = self.clone(BlueReflectanceStats)
        nir_task = self.clone(NearInfraredReflectanceStats)
        mir_task = self.clone(MiddleInfraredReflectanceStats)
        land_task = self.clone(LandCoverStats)
        ntl_task = self.clone(NightTimeLightStats)
        pop_task = self.clone(PopulationStats)
        travel_time_task = self.clone(AccessibilityStats)
        slope_task = self.clone(SlopeStats)
        dem_task = self.clone(DEMStats)
        aridity_index_task = self.clone(AridityIndexStats)
        housing_task = self.clone(HousingStats)
        friction_task = self.clone(FrictionSurfaceStats)
        dia_task = self.clone(DiarrheaPrevalenceStats)
        temp_suit = self.clone(TemperatureSuitabilityStats)
        ghs_smod_task = self.clone(GHSettlementLayerStats)
        ghs_built_task = self.clone(GHSBuiltStats)
        ghs_pop_task = self.clone(GHSPopStats)
        date_list = pd.date_range(
            datetime.date(2012, 4, 1), datetime.date(2020, 3, 1), freq="M"
        )
        input_map = {
            "ndvi": {i: ndvi_task.clone(month=i) for i in date_list},
            "evi": {i: evi_task.clone(month=i) for i in date_list},
            "lst_day": {i: lst_day.clone(month=i) for i in date_list},
            "last_night": {i: last_night.clone(month=i) for i in date_list},
            "red": {i: red_task.clone(month=i) for i in date_list},
            "blue": {i: blue_task.clone(month=i) for i in date_list},
            "nir": {i: nir_task.clone(month=i) for i in date_list},
            "mir": {i: mir_task.clone(month=i) for i in date_list},
            "ntl": {i: ntl_task.clone(month=i) for i in date_list},
            "land": {i: land_task.clone(year=i) for i in range(2014, 2016)},
            # "pop": {i: pop_task.clone(year=i) for i in range(2010, 2020)},
            "travel_time": travel_time_task,
            "slope_task": slope_task,
            "dem_task": dem_task,
            "aridity_index": aridity_index_task,
            "housing": housing_task,
            "friction": friction_task,
            "dia_prev": dia_task,
            "temp_suit": temp_suit,
            "ghs_smod": ghs_smod_task,
            "ghs_built": ghs_built_task,
            "ghs_pop": ghs_pop_task,
        }
        if self.country_level in ["Ethiopia", "South Sudan"]:
            input_map.update(pop={i: pop_task.clone(year=i) for i in range(2010, 2020)})
        return input_map

    def run(self):
        data_map = {}
        for k, v in self.input().items():
            if isinstance(v, dict):
                data_map[k] = pd.concat([i.open().read() for _, i in v.items()])
            else:
                data_map[k] = v.open().read()

        static_variables = [
            "travel_time",
            "slope_task",
            "dem_task",
            "aridity_index",
            "housing",
            "friction",
            "dia_prev",
            "temp_suit",
            "ghs_smod",
            "ghs_built",
            "ghs_pop",
        ]

        months_df = data_map["ndvi"][["month"]]
        months_df = months_df.drop_duplicates(subset=["month"])
        months_df["month"] = pd.to_datetime(months_df["month"])
        months_df["year"] = months_df["month"].dt.year

        for variable in ["land", "pop"]:
            try:
                data_map[variable] = data_map[variable].merge(
                    months_df, on="year", how="right"
                )
                data_map[variable] = data_map[variable].drop("year", axis=1)
            except KeyError:
                pass
        months_df = months_df.drop("year", axis=1)
        months_df["merge_id"] = 0
        for variable in static_variables:
            data_map[variable]["merge_id"] = 0
            data_map[variable] = data_map[variable].merge(
                months_df, on="merge_id", how="right"
            )
            data_map[variable] = data_map[variable].drop("merge_id", axis=1)
        df = pd.DataFrame()
        if self.country_level in ["Ethiopia", "South Sudan", "Kenya"]:
            merge_vars = ["longitude", "latitude", "month"]
            drop_vars = ["geometry"]
        elif self.country_level in ["Djibouti", "Uganda"]:
            merge_vars = ["region", "month", "Country"]
            drop_vars = [
                "geometry",
                "unimproved_sanitation",
                "year",
                "month_num",
                "unimproved_drinking_water",
                "ADM0_EN",
                "ADM0_PCODE",
                "Year",
            ]
        else:
            raise NotImplementedError
        for k in data_map:
            data_map[k]["month"] = data_map[k]["month"].astype(str)
            data_map[k] = data_map[k].drop(
                [i for i in drop_vars if i in data_map[k].columns], axis=1
            )

            if df.empty:
                df = data_map[k].copy()
            else:
                merge_vars = [i for i in merge_vars if i in df.columns]
                df = df.merge(data_map[k], on=merge_vars, how="outer")

        with self.output().open("w") as out:
            out.write(df)
