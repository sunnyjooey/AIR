import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import luigi
import pandas as pd
import xarray as xr
from dateutil import relativedelta
from fiona.crs import from_epsg
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from luigi import ExternalTask, Task
from luigi.util import inherits, requires
from shapely.geometry import Polygon

from utils.geospatial_tasks.functions.geospatial import geography_f_country
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from .data_pre import (
    MaskMaxTemperature,
    MaskMinTemperature,
    MaskNDVIData,
    MaskPFTData,
    MaskRainfallData,
    MaskSoilData,
    PastureLegumeFraction,
    SiteIndex,
)
from .forage import execute

RELATIVEPATH = "models/livestock_model/tasks"


class PullCSVFile(ExternalTask):
    def output(self):
        return {
            "animal_trait_path": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "bbc419c0-086b-45a5-af68-894564a34e1a"},
            ),
            "veg_trait_path": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "5e3e9404-f717-439e-8418-2364c9ad9911"},
            ),
            "pft_initial_table": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "a062a077-70e8-47c9-be0b-9c9518611b06"},
            ),
            "site_param_table": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "b7f40bc9-c14e-4807-ae3f-3bd5bcb85a05"},
            ),
            "site_initial_table": CkanTarget(
                dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
                resource={"id": "e129b5f2-88db-4cbf-afda-10957141d1ca"},
            ),
        }


class PullGrazingArea(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "90383c4c-0763-4d30-9563-abc3643d3de6"},
            resource={"id": "272e9296-4211-4a98-9bb4-930569e2cadc"},
        )


@requires(PullGrazingArea)
class ExtractGrazingArea(Task):
    def output(self):
        dst = Path(RELATIVEPATH) / f"{self.task_id}/"
        return IntermediateTarget(path=str(dst), timeout=5184000)

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            zip_ref = zipfile.ZipFile(self.input().path)
            zip_ref.extractall(tmpdir)


@inherits(GlobalParameters)
class GeographyToShape(ExternalTask):
    def output(self):
        dst = Path(RELATIVEPATH) / f"{self.task_id}/"
        return IntermediateTarget(path=str(dst), timeout=5184000)

    @staticmethod
    def parse_dict(df):
        df["geometry"] = df["geometry"].apply(lambda x: Polygon(x["coordinates"][0]))
        df = gpd.GeoDataFrame(df, geometry="geometry")
        return df

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]
        gdf = gpd.GeoDataFrame()
        gdf["geometry"] = geom
        gdf.crs = from_epsg(4326)
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            try:
                gdf.to_file(tmpdir)
            except AttributeError:
                gdf = self.parse_dict(gdf)
                gdf.to_file(tmpdir)


@requires(
    PastureLegumeFraction,
    MaskPFTData,
    MaskSoilData,
    MaskMinTemperature,
    MaskMaxTemperature,
    MaskNDVIData,
    MaskRainfallData,
    PullCSVFile,
    ExtractGrazingArea,
    GeographyToShape,
    SiteIndex,
)
class RunRangeLandProductionModel(Task):
    management_threshold = luigi.FloatParameter(default=0)

    def output(self):
        dst = Path(RELATIVEPATH) / f"{self.task_id}/"
        return IntermediateTarget(path=str(dst), timeout=5184000)

    def run(self):
        legume_src = self.input()[0].path
        pft_src = self.input()[1].path
        soil_map = self.input()[2]
        soil_src_map = {k: v.path for k, v in soil_map.items()}
        min_temp = self.input()[3].path
        max_temp = self.input()[4].path
        ndvi_src = self.input()[5].path
        rain_src = self.input()[6].path
        csv_map = self.input()[7]
        csv_src_map = {k: v.path for k, v in csv_map.items()}
        graz_dir = self.input()[8].path
        graz_src = [
            os.path.join(graz_dir, i)
            for i in os.listdir(graz_dir)
            if i.endswith(".shp")
        ][0]
        geo = self.input()[9].path
        geo_src = [os.path.join(geo, i) for i in os.listdir(geo) if i.endswith(".shp")][
            0
        ]
        site_index = self.input()[10].path

        r = relativedelta.relativedelta(self.time.date_b, self.time.date_a)
        n_months = r.months + (12 * r.years)
        temp_dir = tempfile.mkdtemp()
        args = {
            "workspace_dir": temp_dir,
            "starting_month": self.time.date_a.month,
            "starting_year": self.time.date_a.year,
            "n_months": n_months,
            "aoi_path": geo_src,
            "management_threshold": self.management_threshold,
            "proportion_legume_path": legume_src,
            "bulk_density_path": soil_src_map["bulk"],
            "ph_path": soil_src_map["ph"],
            "clay_proportion_path": soil_src_map["clay"],
            "silt_proportion_path": soil_src_map["silt"],
            "sand_proportion_path": soil_src_map["sand"],
            "monthly_precip_path_pattern": f"{rain_src}/chirps-v2.0.<year>.<month>.tif",
            "min_temp_path_pattern": f"{min_temp}/min_temperature_<month>.tif",
            "max_temp_path_pattern": f"{max_temp}/max_temperature_<month>.tif",
            "monthly_vi_path_pattern": f"{ndvi_src}/ndvi_<year>_<month>.tif",
            "site_param_table": csv_src_map["site_param_table"],
            "site_param_spatial_index_path": site_index,
            "veg_trait_path": csv_src_map["veg_trait_path"],
            "veg_spatial_composition_path_pattern": f"{os.path.dirname(pft_src)}/pft<PFT>.tif",
            "animal_trait_path": csv_src_map["animal_trait_path"],
            "animal_grazing_areas_path": graz_src,
            "site_initial_table": csv_src_map["site_initial_table"],
            "pft_initial_table": csv_src_map["pft_initial_table"],
        }
        execute(args)
        with self.output().temporary_path() as tmpdir:
            shutil.copytree(temp_dir, tmpdir)


@requires(RunRangeLandProductionModel)
class AnimalDensity(Task):
    def output(self):
        return FinalTarget(path="livestock_model/animal_density.nc", task=self)

    def run(self):
        data_dir = os.path.join(self.input().path, "output")
        start_date = self.time.date_a
        end_date = self.time.date_b
        for index, month in enumerate(pd.date_range(start_date, end_date, freq="M")):
            file_name = f"animal_density_{month.year}_{month.month}.tif"
            tmp = xr.open_rasterio(os.path.join(data_dir, file_name))
            tmp = tmp.squeeze(dim="band")
            tmp = tmp.expand_dims({"date": [month]})
            if index == 0:
                da = tmp.copy()
            else:
                da = xr.concat([da, tmp], "date")

        da = da.to_dataset(name="animal density")
        with self.output().open("w") as out:
            da.to_netcdf(out.name)


@requires(RunRangeLandProductionModel)
class DietSufficiency(Task):
    def output(self):
        return FinalTarget(path="livestock_model/diet_sufficiency.nc", task=self)

    def run(self):
        data_dir = data_dir = os.path.join(self.input().path, "output")
        start_date = self.time.date_a
        end_date = self.time.date_b
        for index, month in enumerate(pd.date_range(start_date, end_date, freq="M")):
            file_name = f"diet_sufficiency_{month.year}_{month.month}.tif"
            tmp = xr.open_rasterio(os.path.join(data_dir, file_name))
            tmp = tmp.squeeze(dim="band")
            tmp = tmp.expand_dims({"date": [month]})
            if index == 0:
                da = tmp.copy()
            else:
                da = xr.concat([da, tmp], "date")

        da = da.to_dataset(name="diet sufficiency")
        with self.output().open("w") as out:
            da.to_netcdf(out.name)


@requires(RunRangeLandProductionModel)
class PotentialBiomass(Task):
    def output(self):
        return FinalTarget(path="livestock_model/potential_biomass.nc", task=self)

    def run(self):
        data_dir = data_dir = os.path.join(self.input().path, "output")
        start_date = self.time.date_a
        end_date = self.time.date_b
        for index, month in enumerate(pd.date_range(start_date, end_date, freq="M")):
            file_name = f"potential_biomass_{month.year}_{month.month}.tif"
            tmp = xr.open_rasterio(os.path.join(data_dir, file_name))
            tmp = tmp.squeeze(dim="band")
            tmp = tmp.expand_dims({"date": [month]})
            if index == 0:
                da = tmp.copy()
            else:
                da = xr.concat([da, tmp], "date")

        da = da.to_dataset(name="potential biomass")
        with self.output().open("w") as out:
            da.to_netcdf(out.name)


@requires(RunRangeLandProductionModel)
class StandingBiomass(Task):
    def output(self):
        return FinalTarget(path="livestock_model/standing_biomass.nc", task=self)

    def run(self):
        data_dir = data_dir = os.path.join(self.input().path, "output")
        start_date = self.time.date_a
        end_date = self.time.date_b
        for index, month in enumerate(pd.date_range(start_date, end_date, freq="M")):
            file_name = f"standing_biomass_{month.year}_{month.month}.tif"
            tmp = xr.open_rasterio(os.path.join(data_dir, file_name))
            tmp = tmp.squeeze(dim="band")
            tmp = tmp.expand_dims({"date": [month]})
            if index == 0:
                da = tmp.copy()
            else:
                da = xr.concat([da, tmp], "date")

        da = da.to_dataset(name="standing biomass")
        with self.output().open("w") as out:
            da.to_netcdf(out.name)
