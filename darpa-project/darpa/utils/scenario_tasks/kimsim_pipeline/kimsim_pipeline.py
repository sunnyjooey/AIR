import os
from datetime import timedelta
from ftplib import FTP  # noqa: S402

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from luigi.configuration import get_config
from luigi.parameter import DateIntervalParameter
from luigi.util import requires
from rasterio.mask import mask
from rasterio.transform import xy
from shapely.geometry import box, shape

import elevation
import osmnx as ox
from kiluigi.parameter import GeoParameter
from kiluigi.targets import CkanTarget, IntermediateTarget
from kiluigi.tasks import ExternalTask, Task
from models.crop_detection_model.tasks import IdentifyCropLand
from utils.defaults import DEFAULT_GEOGRAPHY, DEFAULT_TIME

CONFIG = get_config()


class CkanData(ExternalTask):
    def output(self):
        return {
            "population_density": CkanTarget(
                dataset={"id": "ba3fe2a4-1476-4ccb-8856-382891881ed3"},
                resource={"id": "afd570eb-80b7-4136-88ea-b132ada91a13"},
            ),
            "birth_death_rates": CkanTarget(
                dataset={"id": "79e631b6-86f2-4fc8-8054-31c17eac1e3f"},
                resource={"id": "6ac5956d-7d1f-4f08-aaea-669153b290c8"},
            ),
            "age_sex": CkanTarget(
                dataset={"id": "79e631b6-86f2-4fc8-8054-31c17eac1e3f"},
                resource={"id": "deb7987d-dc53-4374-a33f-51bb29be49ba"},
            ),
            "admin2_boundaries": CkanTarget(
                dataset={"id": "79e631b6-86f2-4fc8-8054-31c17eac1e3f"},
                resource={"id": "913ec657-3184-4bb1-8f13-614dc1341e16"},
            ),
            "poverty": CkanTarget(
                dataset={"id": "8399cfb4-fb5f-4e71-93a9-db77ce3b74a4"},
                resource={"id": "afe3aab8-5610-46af-9e49-9e1d623e2307"},
            ),
            "consumption_expenditure": CkanTarget(
                dataset={"id": "8399cfb4-fb5f-4e71-93a9-db77ce3b74a4"},
                resource={"id": "4c3c22f4-0c6e-4034-bee2-81c89d754dd5"},
            ),
        }


@requires(CkanData)
class GetPopulationRasterBounds(Task):
    """
    This task finds the extent of each tile of population density and saves the file name and its bounds as a vector
    file, so only the appropriate tile(s) will be loaded.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=100_000_000_000)

    def run(self):
        # unzip directory of population density tiles
        zip_path = self.input()["population_density"].path
        cache_dir = CONFIG.get("core", "cache_dir")
        out_folder = os.path.join(cache_dir, "population_density")
        if not os.path.exists(out_folder):
            os.makedirs(out_folder)
            os.system(f"unzip -o -d {out_folder} {zip_path}")  # noqa: S605

        # get bounds of rasters, store in GeoDataFrame with file names
        bounds_dict = {}
        for f in os.listdir(out_folder):
            if f.endswith(".tif"):
                with rasterio.open(os.path.join(out_folder, f)) as src:
                    bounds = src.bounds
                    bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
                    bounds_dict[f] = bbox
        bounds_gdf = pd.DataFrame.from_dict(
            bounds_dict, orient="index", columns=["geometry"]
        )

        with self.output().open("w") as dst:
            dst.write(bounds_gdf)


@requires(GetPopulationRasterBounds)
class GetPopulation(Task):
    """
    This task masks the population density raster to the desired geography and sums up the total population.
    """

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        with self.input().open("r") as src:
            bounds = gpd.GeoDataFrame(src.read())
        aoi = shape(self.geography)
        download_files = bounds.index[bounds.intersects(aoi)]
        n_people = 0
        for f in download_files:
            path = os.path.join(
                CONFIG.get("core", "cache_dir"), "population_density", f
            )
            with rasterio.open(path) as src:
                masked_img, _ = mask(src, [self.geography], crop=True)
                masked_img = np.squeeze(masked_img)
                n_people += int(np.nansum(masked_img))
        with self.output().open("w") as dst:
            dst.write({"population_total_initial": n_people})


@requires(CkanData, GetPopulation)
class GetDemographics(Task):
    """
    This task uses the age-sex breakdowns from the South Sudan census data and the total population of the scenario
    geography to determine the number of adult males, adult females, and children in the scenario.
    """

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)
    time = DateIntervalParameter(default=DEFAULT_TIME)

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        age_sex = pd.read_excel(self.input()[0]["age_sex"].path)
        adm2 = gpd.read_file(self.input()[0]["admin2_boundaries"].path)

        # only keep data for scenario geography
        geo = adm2[adm2.intersects(shape(self.geography))]

        merged = geo.merge(age_sex, left_on="ADMIN2NAME", right_on="County")
        total_cols = [col for col in age_sex.columns if col.endswith("T")]
        male_cols = [col for col in age_sex.columns if col.endswith("M")]
        female_cols = [col for col in age_sex.columns if col.endswith("F")]

        adult_male_cols = [
            col
            for col in male_cols
            if col.startswith("95") or int(col.split("_")[0]) >= 18
        ]
        adult_female_cols = [
            col
            for col in female_cols
            if col.startswith("95") or int(col.split("_")[0]) >= 18
        ]
        child_cols = [
            col
            for col in total_cols
            if not col.startswith("95") and int(col.split("_")[0]) < 18
        ]

        # get total numbers for counties that contain scenario geography
        total_pop = merged[total_cols].sum().sum()
        n_adult_males = merged[adult_male_cols].sum().sum()
        n_adult_females = merged[adult_female_cols].sum().sum()
        n_children = merged[child_cols].sum().sum()

        # calculate demographic proportions for scenario geography
        adult_male_pct = n_adult_males / total_pop
        adult_female_pct = n_adult_females / total_pop
        child_pct = n_children / total_pop

        # multiply demographic proportions by total scenario population to determine demographic breakdown
        with self.input()[1].open("r") as src:
            aoi_pop = src.read()["population_total_initial"]

        aoi_adult_males = round(aoi_pop * adult_male_pct)
        aoi_adult_females = round(aoi_pop * adult_female_pct)
        aoi_children = round(aoi_pop * child_pct)

        with self.output().open("w") as dst:
            dst.write(
                {
                    "VillageAdultMalePopulation": aoi_adult_males,
                    "VillageAdultFemalePopulation": aoi_adult_females,
                    "VillageChildrenPopulation": aoi_children,
                }
            )


@requires(CkanData)
class GetBirthDeathRates(Task):
    """
    This task returns South Sudan crude birth and death rate data.
    """

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)
    time = DateIntervalParameter(default=DEFAULT_TIME)

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        birth_death_rates = pd.read_csv(self.input()["birth_death_rates"].path)
        adm2 = gpd.read_file(self.input()["admin2_boundaries"].path)

        # only keep data for scenario geography
        geo = adm2[adm2.intersects(shape(self.geography))]

        merged = geo.merge(birth_death_rates, on="ADMIN2PCOD")
        year = self.time[0].year
        cbr_col = f"CBR_{year}"
        cdr_col = f"CDR_{year}"

        birth_rate = merged[cbr_col].mean()
        death_rate = merged[cdr_col].mean()

        with self.output().open("w") as dst:
            dst.write({"birth_rate_p_y": birth_rate, "death_rate_p_y": death_rate})


class DownloadRoadNetwork(Task):

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        roads = ox.graph_from_polygon(
            shape(self.geography), network_type="all", retain_all=True
        )
        roads_gdf = ox.graph_to_gdfs(roads)[1]
        geojson = roads_gdf.to_json()

        with self.output().open("w") as dst:
            dst.write({"roads": geojson})


class GetBuildingLocations(Task):
    """
    This task returns the locations of buildings within the scenario geography using OpenStreetMaps data. Rather than
    return the entire building footprint, it returns the (lat, lon) pair of the centroid of the building, as requested
    by Bolder Games.
    """

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        footprints = ox.footprints_from_polygon(shape(self.geography))
        footprints["geometry"] = footprints["geometry"].apply(lambda g: g.centroid)
        coords = [
            (g.coords.xy[1][0], g.coords.xy[0][0]) for g in footprints["geometry"]
        ]

        with self.output().open("w") as dst:
            dst.write({"buildings": coords})


class GetTerrain(Task):

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)
    dem_path = os.path.join(CONFIG.get("core", "cache_dir"), "dem.tif")

    def output(self):
        return IntermediateTarget(task=self, timeout=10)

    def run(self):
        bounds = shape(DEFAULT_GEOGRAPHY).bounds
        elevation.clip(bounds, output=self.dem_path)


class GetRainfall(Task):
    """
    Downloads daily rainfall data for scenario duration, takes mean value over scenario geography, returns series of
    daily rainfall values.
    """

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)
    time = DateIntervalParameter(default=DEFAULT_TIME)
    outdir = os.path.join(CONFIG.get("core", "cache_dir"), "rainfall")

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=24 * 60 * 60)

    def run(self):
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

        # connect to FTP
        ftp = FTP("ftp.chg.ucsb.edu")  # noqa: S321 - ignore secuirty check
        ftp.sendcmd("USER anonymous")
        ftp.sendcmd("PASS anonymous@")

        base_dir = "pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/"
        ftp.cwd(base_dir)

        # find all years in scenario, since we'll have to switch folders for each year
        year_delta = self.time[1].year - self.time[0].year
        years = [self.time[0].year + i for i in range(year_delta + 1)]
        rainfall_vals = []
        for year in years:
            # switch to year directory
            ftp.cwd(f"{str(year)}")

            # get file name for each day in scenario
            delta = self.time[1] - self.time[0]
            dates = [self.time[0] + timedelta(i) for i in range(delta.days + 1)]
            download_files = [
                f"chirps-v2.0.{d.year}.{str(d.month).zfill(2)}.{str(d.day).zfill(2)}.tif.gz"
                for d in dates
                if d.year == year
            ]

            for f in download_files:
                zip_path = os.path.join(self.outdir, f)
                tif_path = zip_path.replace(".gz", "")
                # download file from FTP
                if not os.path.isfile(tif_path):
                    ftp.retrbinary("RETR " + f, open(zip_path, "wb").write)
                    os.system(f"gunzip {zip_path}")  # noqa: S605

                # mask rainfall raster to AOI, update metadata
                with rasterio.open(tif_path) as src:
                    masked_img, masked_transform = mask(
                        src, [self.geography], crop=True
                    )
                    masked_img = np.squeeze(masked_img)
                    rainfall_vals.append(np.nanmean(masked_img))

            ftp.cwd(f"/{base_dir}")

        with self.output().open("w") as dst:
            dst.write({"rainfall": rainfall_vals})


@requires(CkanData)
class GetEconomicData(Task):

    """
    This task masks the outputs of the household economic model to the scenario geography to determine poverty levels,
    min, max, median, and standard deviation of household consumption expenditure.
    """

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)
    time = DateIntervalParameter(default=DEFAULT_TIME)

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        with rasterio.open(self.input()["poverty"].path) as src:
            masked_img, _ = mask(src, [self.geography], crop=True)
            masked_img = np.squeeze(masked_img)
            poverty_pct = np.nanmean(masked_img)

        with rasterio.open(self.input()["consumption_expenditure"].path) as src:
            masked_img, _ = mask(src, [self.geography], crop=True)
            masked_img = np.squeeze(masked_img)
            masked_img[masked_img <= 0] = np.nan

            min_ce = np.nanmin(masked_img)
            max_ce = np.nanmax(masked_img)
            med_ce = np.nanmedian(masked_img)
            std_ce = np.nanstd(masked_img)

        with self.output().open("w") as dst:
            dst.write(
                {
                    "HouseholdIncomeMin": min_ce,
                    "HouseholdIncomeMax": max_ce,
                    "HouseholdIncomeMedian": med_ce,
                    "HouseholdIncomeStdDev": std_ce,
                    "poverty_line_percent_below": poverty_pct,
                }
            )


class GetCropLocations(Task):

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)
    time = DateIntervalParameter(default=DEFAULT_TIME)

    def requires(self):
        return IdentifyCropLand(geography=self.geography, time=self.time)

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60)

    def run(self):
        with self.input().open("r") as src:
            crop_raster_path = src.read()

        with rasterio.open(crop_raster_path) as src:
            data = src.read(1)
            transform = src.transform

        xs, ys = np.where(data == 0)
        lons, lats = xy(transform, rows=ys, cols=xs)

        with self.output().open("w") as dst:
            dst.write(list(zip(lats, lons)))


@requires(
    GetPopulation,
    GetDemographics,
    GetBirthDeathRates,
    GetEconomicData,
    GetRainfall,
    GetBuildingLocations,
    GetCropLocations,
)
class GetKimSimBaseData(Task):
    """
    This task combines the outputs of all data gathering tasks into a single JSON-like dictionary, as this is the
    preferred format for Bolder Games.
    """

    geography = GeoParameter(default=DEFAULT_GEOGRAPHY)
    time = DateIntervalParameter(default=DEFAULT_TIME)

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        kimsim_data = {}
        for upstream_target in self.input():
            with upstream_target.open("r") as src:
                data_dict = src.read()
                for k, v in data_dict.items():
                    kimsim_data[k] = v

        with self.output().open("w") as dst:
            dst.write(kimsim_data)
