import glob
import os
import pickle

import geojson
import geopandas as gpd
import luigi
import numpy as np
import numpy.ma as ma
import pandas as pd
import rasterio
import rasterio.features
from geojson import FeatureCollection
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget  # noqa: F401
from kiluigi.tasks import ExternalTask, Task
from luigi.configuration import get_config
from luigi.util import requires
from rasterio.features import shapes
from rasterio.mask import mask
from rasterio.transform import Affine

from models.malnutrition_model.functions.helper_func import retrieve_file
from models.malnutrition_model.functions.maln_utility_func import column_standardization
from utils.geospatial_tasks.functions.geospatial import geography_f_country
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters
from utils.visualization_tasks.push_csv_to_database import PushCSVToDatabase

CONFIG = get_config()
POP_OUTPUT_PATH = os.path.join(CONFIG.get("paths", "output_path"), "population_model")

# set country and admin
# Standard administrative levels
ADMIN_LEVEL_CHOICES = {
    "ADMIN_0": "admin0",
    "ADMIN_1": "admin1",
    "ADMIN_2": "admin2",
    "ADMIN_3": "admin3",
    "ADMIN_4": "admin4",
}

COUNTRY_CHOICES = {"SS": "South Sudan", "ETH": "Ethiopia"}

REF_PROJ_CONFIG_SS = {
    "srs": 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]',  # noqa
    "wkp": "EPSG:4326",
    "transform": Affine(
        0.008_332_824_858_757_061,
        0.0,
        24.12156,
        0.0,
        -0.008_334_240_687_679_084,
        12.21615,
    ),
    "ncolsrows": (1416, 1051),
}

REF_PROJ_CONFIG_ETH = {
    "srs": 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]',  # noqa
    "wkp": "EPSG:4326",
    "transform": Affine(
        0.008_332_056_698_165_648,
        0.0,
        32.9898,
        0.0,
        -0.008_334_204_793_028_323,
        14.87953,
    ),
    "ncolsrows": (1800, 1379),
}


class AgeGenderFile(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "79e631b6-86f2-4fc8-8054-31c17eac1e3f"},
            resource={"id": "9ad617a9-cafb-4e8c-a40a-0a2467fef07d"},
        )


class AdminShpfiles(ExternalTask):

    """
    Task for pulling shape files for county and payam level, and landscan population density grids
    """

    def output(self):

        return {
            "county_shape": CkanTarget(
                dataset={"id": "79e631b6-86f2-4fc8-8054-31c17eac1e3f"},
                resource={"id": "210558e2-d70f-4762-b24b-6be2992d1e41"},
            ),
            "payam_shape": CkanTarget(
                dataset={"id": "79e631b6-86f2-4fc8-8054-31c17eac1e3f"},
                resource={"id": "39a7af45-d929-4475-851d-8b0d763b7f7c"},
            ),
            "pop_density": CkanTarget(
                dataset={"id": "83a07464-6aa1-4611-8e51-d2e04c4a7b97"},
                resource={"id": "146eafe7-28fe-4ccd-8daa-dc95e5252389"},
            ),
            "ethiopia_county_shape": CkanTarget(
                dataset={"id": "d07b30c6-4909-43fa-914b-b3b435bef314"},
                resource={"id": "d4804e8a-5146-48cd-8a36-557f981b073c"},
            ),
            "eth_pop_density": CkanTarget(
                dataset={"id": "11bd6b1e-6939-4f00-839d-fdff1a470dd1"},
                resource={"id": "fef4be94-0f26-4861-bc75-7e7c1927af24"},
            ),
            "ethiopia_woreda_shape": CkanTarget(
                dataset={"id": "d8292b7b-c937-4e81-bc96-796f2d78f339"},
                resource={"id": "2c8a0376-7d93-48d8-b2a5-3c10f1d3eb1a"},
            ),
        }


class censusfiles(ExternalTask):

    """
    Task for pulling csv files for county and payam population estimateself.
    Note: ssd_norm_ds and eth_norm_ds are the normalized datasets which aggregates from different SID sources
    """

    def output(self):

        return {
            "ssd_census_county": CkanTarget(
                dataset={"id": "79e631b6-86f2-4fc8-8054-31c17eac1e3f"},
                resource={"id": "81df7a5f-1b24-41d1-8f9c-0e67623d58a7"},
            ),
            "ssd_census_payam": CkanTarget(
                dataset={"id": "ba4c5a30-1575-4049-a901-db7c756c052d"},
                resource={"id": "39a7af45-d929-4475-851d-8b0d763b7f7c"},
            ),
            "ethiopia_census_county": CkanTarget(
                dataset={"id": "d85818ef-773b-4385-b116-6f172b6f2477"},
                resource={"id": "b3955342-6c41-4b6b-9995-4f015490a3e3"},
            ),
            "ssd_norm_ds": CkanTarget(
                dataset={"id": "f6544ffc-3f7f-4170-8520-e1b75c657f6e"},
                resource={"id": "73aeb9fb-ae0f-4a1d-88e0-4299f6eb9637"},
            ),
            "eth_norm_ds": CkanTarget(
                dataset={"id": "f6544ffc-3f7f-4170-8520-e1b75c657f6e"},
                resource={"id": "b3a49113-780c-481c-8bed-eae7e2bf7e80"},
            ),
        }


@requires(AdminShpfiles, censusfiles)
class PopCkanData(Task):
    """
    retrieves data from CKAN and returns the dir path dictionary
    """

    outfolder = POP_OUTPUT_PATH

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        shp_data_dict = self.input()[0]  # returns dictionary
        census_data_dict = self.input()[1]

        county_shp_dir = shp_data_dict[list(shp_data_dict.keys())[0]].path
        payam_shp_dir = shp_data_dict[list(shp_data_dict.keys())[1]].path
        eth_shp_admin2_dir = shp_data_dict[list(shp_data_dict.keys())[3]].path
        eth_shp_admin3_dir = shp_data_dict[list(shp_data_dict.keys())[5]].path

        pop_density_dir = shp_data_dict[list(shp_data_dict.keys())[2]].path
        eth_pop_density_dir = shp_data_dict[list(shp_data_dict.keys())[4]].path
        # unzip the shapefiles
        retrieve_file(county_shp_dir, ".zip", self.outfolder)
        retrieve_file(payam_shp_dir, ".zip", self.outfolder)
        retrieve_file(eth_shp_admin2_dir, ".zip", self.outfolder)
        retrieve_file(eth_shp_admin3_dir, ".zip", self.outfolder)

        county_shp_path = (
            os.path.join(self.outfolder, county_shp_dir.split("/")[-2]) + "/"
        )
        payam_shp_path = (
            os.path.join(self.outfolder, payam_shp_dir.split("/")[-2]) + "/"
        )
        eth_county_shp_path = (
            os.path.join(self.outfolder, eth_shp_admin2_dir.split("/")[-2]) + "/"
        )
        eth_woreda_shp_path = (
            os.path.join(self.outfolder, eth_shp_admin3_dir.split("/")[-2]) + "/"
        )

        county_census_dir = census_data_dict[list(census_data_dict.keys())[0]].path
        payam_census_dir = census_data_dict[list(census_data_dict.keys())[1]].path
        eth_census_dir = census_data_dict[list(census_data_dict.keys())[2]].path
        ssd_norm_dir = census_data_dict[list(census_data_dict.keys())[3]].path
        eth_norm_dir = census_data_dict[list(census_data_dict.keys())[4]].path

        path_names = {
            "county_shp_dir": county_shp_path,
            "payam_shp_dir": payam_shp_path,
            "ethiopia_county_shp_dir": eth_county_shp_path,
            "ethiopia_woreda_shp_dir": eth_woreda_shp_path,
            "pop_density_dir": pop_density_dir,
            "eth_pop_density": eth_pop_density_dir,
            "county_census_dir": county_census_dir,
            "payam_census_dir": payam_census_dir,
            "ethiopia_census_dir": eth_census_dir,
            "ssd_norm_dir": ssd_norm_dir,
            "eth_norm_dir": eth_norm_dir,
        }
        with self.output().open("w") as output:  # IntermediateTarget requires .open()
            output.write(path_names)


@requires(PopCkanData, GlobalParameters)
class NormalizePopData(Task):
    """
    Normalize the Population data.

    Selects normalized census data matching a particular search and/or
    filter criteria.

    At the moment only selects census data at the requested admin level,
    but in the future can be extended to select by country, year, etc.
    This is the basis of being able to scale our pipelines to other
    countries and time periods.
    """

    def output(self):
        return IntermediateTarget(
            f"population_model/census_data_{self.country_level}_{self.admin_level}-norm.pickle",
            task=self,
            timeout=3600,
        )

    def standardize_column(self, df, geoid):
        # Remove leading & trailing whitespace from column names
        df.rename(str.strip, axis="columns", inplace=True)
        # Make column names lowercase
        df.rename(str.lower, axis="columns", inplace=True)

        # Standardize the geographic unit unique identifier column and make it lowercase
        df["GEO_ID"] = df[geoid].str.lower()

        # Split the census data
        # @TODO: Validate that there is only one census year in
        # the data (either here or in the EstimatePopulation task)
        # @TODO: Validate that there are no duplicate rows
        # census_data = df[["GEO_ID", "POP"]].copy()
        # census_data["YEAR"] = self.CENSUS_YEAR
        return df

    def run(self):
        ckan_path_names = self.input()[0].open().read()
        if self.country_level == COUNTRY_CHOICES["SS"]:
            if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
                # Normalize County data (data set calls it admin3)
                admin_level = "admin3"
                df = pd.read_csv(ckan_path_names["ssd_norm_dir"])
                census_data = self.standardize_column(df, admin_level)
                census_data["year"] = census_data["year"].astype(int)
                pop_proj_data = census_data[census_data["datasourceid"] == "SID20"]
            elif self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_3"]:
                # use admin2 county data FOR NOW
                admin_level = "admin3"
                df = pd.read_csv(ckan_path_names["ssd_norm_dir"])
                census_data = self.standardize_column(df, admin_level)
                census_data["year"] = census_data["year"].astype(int)
                pop_proj_data = census_data[census_data["datasourceid"] == "SID20"]

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            # get census for ethiopia:
            if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
                df = pd.read_csv(ckan_path_names["eth_norm_dir"])
                # fix the column typo
                typo_file = open("models/malnutrition_model/eth_typo.pkl", "rb")
                eth_dict = pickle.load(typo_file)  # noqa: S301
                typo_file.close()

                census_data = column_standardization(df, self.admin_level, eth_dict)
                census_data["GEO_ID"] = census_data[self.admin_level]
                census_data["year"] = census_data["year"].astype(int)
                pop_proj_data = census_data[
                    (census_data["year"] > 1999)
                    & (census_data["btotl"].notnull())
                    & (census_data["mtotl"].notnull())
                ]
                pop_proj_data = (
                    pop_proj_data.groupby(
                        [
                            "GEO_ID",
                            "datasourceid",
                            "census_or_projected",
                            "admin0",
                            "admin1",
                            "admin2",
                            "year",
                        ]
                    )
                    .sum()
                    .reset_index()
                )

                select_cols = [
                    "GEO_ID",
                    "admin0",
                    "admin1",
                    "admin2",
                    "datasourceid",
                    "year",
                    "census_or_projected",
                    "growth_rate",
                    "cdr",
                    "death_rate",
                    "cbr",
                    "birth_rate",
                    "in_migration",
                    "out_migration",
                    "btotl",
                    "mtotl",
                    "ftotl",
                    "b0004",
                    "b0509",
                    "b1014",
                    "b1519",
                    "b2024",
                    "b2529",
                    "b3034",
                    "b3539",
                    "b4044",
                    "b4549",
                    "b5054",
                    "b5559",
                    "b6064",
                    "b6569",
                    "b7074",
                    "b7579",
                    "b8084",
                    "b8589",
                    "b9094",
                    "b95pl",
                    "m0004",
                    "m0509",
                    "m1014",
                    "m1519",
                    "m2024",
                    "m2529",
                    "m3034",
                    "m3539",
                    "m4044",
                    "m4549",
                    "m5054",
                    "m5559",
                    "m6064",
                    "m6569",
                    "m7074",
                    "m7579",
                    "m8084",
                    "m8589",
                    "m9094",
                    "m95pl",
                    "f0004",
                    "f0509",
                    "f1014",
                    "f1519",
                    "f2024",
                    "f2529",
                    "f3034",
                    "f3539",
                    "f4044",
                    "f4549",
                    "f5054",
                    "f5559",
                    "f6064",
                    "f6569",
                    "f7074",
                    "f7579",
                    "f8084",
                    "f8589",
                    "f9094",
                    "f95pl",
                ]

            elif self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_3"]:
                df = pd.read_csv(ckan_path_names["eth_norm_dir"])
                census_data = df.copy()
                census_data[self.admin_level] = census_data[
                    self.admin_level
                ].str.lower()
                census_data["GEO_ID"] = census_data[self.admin_level]
                census_data["year"] = census_data["year"].astype(int)
                pop_proj_data = census_data[
                    (census_data["year"] > 1999)
                    & (census_data["btotl"].notnull())
                    & (census_data["mtotl"].notnull())
                ]

                select_cols = [
                    "GEO_ID",
                    "admin0",
                    "admin1",
                    "admin2",
                    "admin3",
                    "datasourceid",
                    "year",
                    "census_or_projected",
                    "growth_rate",
                    "cdr",
                    "death_rate",
                    "cbr",
                    "birth_rate",
                    "in_migration",
                    "out_migration",
                    "btotl",
                    "mtotl",
                    "ftotl",
                    "b0004",
                    "b0509",
                    "b1014",
                    "b1519",
                    "b2024",
                    "b2529",
                    "b3034",
                    "b3539",
                    "b4044",
                    "b4549",
                    "b5054",
                    "b5559",
                    "b6064",
                    "b6569",
                    "b7074",
                    "b7579",
                    "b8084",
                    "b8589",
                    "b9094",
                    "b95pl",
                    "m0004",
                    "m0509",
                    "m1014",
                    "m1519",
                    "m2024",
                    "m2529",
                    "m3034",
                    "m3539",
                    "m4044",
                    "m4549",
                    "m5054",
                    "m5559",
                    "m6064",
                    "m6569",
                    "m7074",
                    "m7579",
                    "m8084",
                    "m8589",
                    "m9094",
                    "m95pl",
                    "f0004",
                    "f0509",
                    "f1014",
                    "f1519",
                    "f2024",
                    "f2529",
                    "f3034",
                    "f3539",
                    "f4044",
                    "f4549",
                    "f5054",
                    "f5559",
                    "f6064",
                    "f6569",
                    "f7074",
                    "f7579",
                    "f8084",
                    "f8589",
                    "f9094",
                    "f95pl",
                ]

            pop_proj_data = pop_proj_data[select_cols]

        else:
            # @TODO: Raise appropriate exception
            raise FileNotFoundError

        with self.output().open("w") as f:
            f.write(pop_proj_data)


@requires(PopCkanData, GlobalParameters)
class NormalizeShapeFiles(Task):

    """
    Normalize shapefiles.

    Selects normalized shapefiles matching a particular search and/or
    filter criteria.

    At the moment only selects shapes at the requested admin level,
    but in the future can be extended to select by country, year, etc.
    This is the basis of being able to scale our pipelines to other
    countries and time periods.
    """

    def output(self):
        return IntermediateTarget(
            f"population_model/shapefiles_{self.country_level}_{self.admin_level}_-norm.pickle",
            task=self,
            timeout=3600,
        )

    def normalize_county_shapefile(self, df):
        # Standardize column header names
        df.rename(str.strip, axis="columns", inplace=True)
        df.rename(str.lower, axis="columns", inplace=True)
        # Standardize the Geographic unit unique identifier column
        df["GEO_ID"] = df["admin2name"].str.lower()

        return df

    def normalize_payam_shapefile(self, df):
        # Standardize column header names
        df.rename(str.strip, axis="columns", inplace=True)
        df.rename(str.lower, axis="columns", inplace=True)
        # Standardize the Geographic unit unique identifier column
        df["GEO_ID"] = df["c_payam"].str.lower()

        return df

    def run(self):
        ckan_path_names = self.input()[0].open().read()

        if self.country_level == COUNTRY_CHOICES["SS"]:
            if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
                # Normalize County shapefile
                geoshape_fname = glob.glob(
                    ckan_path_names["county_shp_dir"] + "/*.shp"
                )[0]
                df = gpd.read_file(geoshape_fname, encoding="utf-8")

                shp_data = self.normalize_county_shapefile(df)
            elif self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_3"]:
                # Normalize Payam shapefile
                geoshape_fname = glob.glob(ckan_path_names["payam_shp_dir"] + "/*.shp")[
                    0
                ]
                df = gpd.read_file(geoshape_fname, encoding="utf-8")
                shp_data = self.normalize_payam_shapefile(df)

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            # normalize shape file for ethiopia:
            if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:

                geoshape_fname = glob.glob(
                    ckan_path_names["ethiopia_county_shp_dir"] + "/*.shp"
                )[0]

                shp_data = gpd.read_file(geoshape_fname, encoding="utf-8")
                typo_file = open("models/malnutrition_model/eth_typo.pkl", "rb")
                eth_dict = pickle.load(typo_file)  # noqa: S301
                typo_file.close()

                shp_data = column_standardization(shp_data, "admin2", eth_dict)
                shp_data.rename(
                    columns={"admin2": "GEO_ID"}, inplace=True,
                )

            elif self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_3"]:

                geoshape_fname = glob.glob(
                    ckan_path_names["ethiopia_woreda_shp_dir"] + "/*.shp"
                )[0]

                shp_data = gpd.read_file(geoshape_fname, encoding="utf-8")

                shp_data[self.admin_level] = shp_data["WOREDANAME"].str.strip()
                shp_data[self.admin_level] = shp_data["WOREDANAME"].str.lower()
                shp_data.rename(
                    columns={"admin3": "GEO_ID"}, inplace=True,
                )

        else:
            # @TODO: Raise appropriate exception
            raise FileNotFoundError

        with self.output().open("w") as f:
            f.write(shp_data)


@requires(NormalizePopData, GlobalParameters)
class EstimatePopulation(Task):
    """
    Filter the population based on year from time parameter and returns a csv file
    """

    def output(self):
        return FinalTarget(
            f"population_estimate_{self.country_level}_{self.admin_level}.csv",
            task=self,
        )

    def run(self):
        # determine years from user input time parameters
        dt_list = list(
            set(range(self.time.date_a.year, self.time.dates()[-1].year + 1))
        )

        print("list of unique years from UI parameter", dt_list)

        population_data = self.input()[0].open().read()
        pop_total_estimates = population_data[
            [
                "GEO_ID",
                "year",
                "btotl",
                "mtotl",
                "ftotl",
                "b0004",
                "b0509",
                "b1014",
                "b1519",
                "b2024",
                "b2529",
                "b3034",
                "b3539",
                "b4044",
                "b4549",
                "b5054",
                "b5559",
                "b6064",
                "b6569",
                "b7074",
                "b7579",
                "b8084",
                "b8589",
                "b9094",
                "b95pl",
            ]
        ]
        pop_total_estimates.drop_duplicates(inplace=True)
        # filter for years present in Time parameters!
        pop_yr_estimates = pop_total_estimates.loc[
            pop_total_estimates["year"].isin(dt_list)
        ]

        if self.country_level == COUNTRY_CHOICES["SS"]:
            pop_yr_estimates.insert(loc=0, column="Country", value="South Sudan")

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            pop_yr_estimates.insert(loc=0, column="Country", value="Ethiopia")

        pop_yr_estimates = pop_yr_estimates.rename(columns={"GEO_ID": self.admin_level})

        with self.output().open("w") as f:
            pop_yr_estimates.to_csv(f, index=False)


@requires(EstimatePopulation, NormalizeShapeFiles, GlobalParameters)
class RasterizePopulationEstimates(Task):
    """
    Convert population estimate array to raster on the admin level and save as .tiff file.
    """

    # Tags for raster bands in order
    RASTER_BANDS_NAMES = [
        "btotl",
        "mtotl",
        "ftotl",
        "b0004",
        "b0509",
        "b1014",
        "b1519",
        "b2024",
        "b2529",
        "b3034",
        "b3539",
        "b4044",
        "b4549",
        "b5054",
        "b5559",
        "b6064",
        "b6569",
        "b7074",
        "b7579",
        "b8084",
        "b8589",
        "b9094",
        "b95pl",
    ]

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=3600, task=self)

    def run(self):
        pop_estimate = pd.read_csv(self.input()[0].path)
        shpfile = self.input()[1].open().read()

        pop_estimate = pop_estimate.merge(shpfile, on="GEO_ID", how="inner")

        # ----------------------------------------------------------------------
        # Rasterize estimated total population and the various demographic groups
        # ----------------------------------------------------------------------
        # @TODO: Remove when the cohorts are rasterized along with the population values
        pop_band = self.RASTER_BANDS_NAMES
        year_range = pop_estimate["year"].unique().tolist()  # noqa:F841

        if self.country_level == COUNTRY_CHOICES["SS"]:

            ncols, nrows = REF_PROJ_CONFIG_SS["ncolsrows"]
            ##### profile based on country choice #########
            profile = {
                "driver": "GTiff",
                "height": nrows,
                "width": ncols,
                "count": len(pop_band),
                "dtype": rasterio.float32,
                "crs": REF_PROJ_CONFIG_SS["srs"],
                "transform": REF_PROJ_CONFIG_SS["transform"],
                "nodata": -9999,
            }

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            ncols, nrows = REF_PROJ_CONFIG_ETH["ncolsrows"]
            profile = {
                "driver": "GTiff",
                "height": nrows,
                "width": ncols,
                "count": len(pop_band),
                "dtype": rasterio.float32,
                "crs": REF_PROJ_CONFIG_ETH["srs"],
                "transform": REF_PROJ_CONFIG_ETH["transform"],
                "nodata": -9999,
            }

        with self.output().temporary_path() as temp_output_path:
            os.makedirs(temp_output_path)
            for yr in year_range:
                raster_outname = os.path.join(
                    temp_output_path,
                    f"{yr}_{self.country_level}_{self.admin_level}_pop.tiff",
                )
                # fetch data in that year
                pop_estimate_yr = pop_estimate[pop_estimate["year"] == yr]

                # define timestamp obj for raster
                time_obj = pd.Timestamp(yr, 1, 1)
                with rasterio.open(raster_outname, "w", **profile) as out_raster:

                    # Rasterize geopandas geometries with population values for the given year
                    for i, column in enumerate(pop_band, 1):
                        out_raster.update_tags(Time=time_obj)
                        shapes = (
                            (geom, pop_value)
                            for geom, pop_value in zip(
                                pop_estimate_yr["geometry"], pop_estimate_yr[column]
                            )
                        )

                        burned_data = rasterio.features.rasterize(
                            shapes=shapes,
                            fill=np.nan,
                            out_shape=out_raster.shape,
                            transform=out_raster.transform,
                        )
                        # Write band. GDAL doesn't support float64 so cast to float32.
                        out_raster.write_band(i, burned_data.astype(rasterio.float32))

                        # Tag raster band
                        out_raster.set_band_description(i, column)
                        out_raster.update_tags(**{"Band_{}".format(i): column})


@requires(RasterizePopulationEstimates)
class RasterizedPopGeojson(Task):
    """
    takes the rasterized population tiff from previous task and converts it to geojson format
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=360 * 20, task=self)

    def run(self):
        # Open and read raster files
        raster_ls = os.listdir(self.input().path)
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)

            for raster_file in raster_ls:
                raster_fname = os.path.join(self.input().path, raster_file)
                split_name = raster_fname.split("/")[-1].split(".tif")[0]

                with rasterio.open(raster_fname) as pop_est_raster:
                    # make a list that includes the other band

                    json_ls = []
                    for i in range(pop_est_raster.count):

                        pop_est_arr = pop_est_raster.read(i + 1)
                        raster_meta = pop_est_raster.tags()
                        band_name = "population_" + raster_meta["Band_" + str(i + 1)]
                        results = (
                            {
                                "type": "Feature",
                                "geometry": s,
                                "properties": {band_name: v},
                            }
                            for i, (s, v) in enumerate(
                                shapes(
                                    pop_est_arr,
                                    mask=None,
                                    transform=pop_est_raster.transform,
                                )
                            )
                        )
                        json_ls.append(list(results))

                out_fname = os.path.join(tmpdir, f"{split_name}.geojson")
                with open(out_fname, "w") as outfile:
                    geojson.dump(FeatureCollection(json_ls), outfile, allow_nan=True)


@requires(RasterizePopulationEstimates, PopCkanData, GlobalParameters)
class CreateHighResPopulationEstimateRaster(Task):
    """
    Distribute admin-level population estimates to a higher resolution
    grid (1km^2 grid), by using a county population density grid. The output
    also has the same number of bands (band_1: total population, band_2, male_total,
    band_3, femal_total)

    @TODO: At the moment, uses a static population density grid. This should
    be improved to use a population density grid of the same year as the
    population estimate i.e a dynamic population density grid.
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=3600, task=self)

    def run(self):
        pop_raster_ls = os.listdir(self.input()[0].path)

        if self.country_level == COUNTRY_CHOICES["SS"]:
            pop_density_fname = self.input()[1].open().read()["pop_density_dir"]

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            pop_density_fname = self.input()[1].open().read()["eth_pop_density"]

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            for raster_file in pop_raster_ls:
                raster_fname = os.path.join(self.input()[0].path, raster_file)
                split_name = raster_fname.split("/")[-1].split("_")

                with rasterio.open(pop_density_fname) as pop_dist_raster:

                    pop_dist = pop_dist_raster.read(
                        1
                    )  # don't use masked=True, or borders will contain max value
                    nodata_pop_dist = pop_dist_raster.nodata
                    profile = pop_dist_raster.profile

                with rasterio.open(raster_fname) as pop_est_raster:

                    time_str = pop_est_raster.tags()["Time"]
                    nodata_pop_est = pop_est_raster.nodata
                    band_counts = pop_est_raster.count
                    raster_meta = pop_est_raster.tags()

                    assert pop_est_raster.crs == pop_dist_raster.crs
                    assert pop_est_raster.width == pop_dist_raster.width
                    assert pop_est_raster.height == pop_dist_raster.height

                    band_ls = []
                    for i in range(band_counts):
                        band_ls.append(i + 1)

                    pop_est_bands = pop_est_raster.read(
                        tuple(band_ls)
                    )  # don't use masked=True, or borders will contain max value
                    # shpae of pop_dist is (1051, 1416)

                    # loop through the 3 bands and store output in high_res_arry
                    high_res_arr = np.zeros(pop_est_bands.shape)

                    for i in range(band_counts):
                        pop_est = np.squeeze(pop_est_bands[i])
                        pop_dist = np.squeeze(pop_dist)
                        high_res_pop_est = np.ones(pop_dist.shape) * -9999
                        ind1 = np.where(pop_est.flatten() != nodata_pop_est)[0]
                        ind2 = np.where(pop_dist.flatten() != nodata_pop_dist)[0]
                        ind = np.intersect1d(ind1, ind2)
                        ind2d = np.unravel_index(ind, pop_dist.shape)

                        # take county malnutrition cases X the county population percentage
                        high_res_pop_est[ind2d] = pop_est[ind2d] * pop_dist[ind2d]
                        high_res_pop_est[ind2d] = np.round(high_res_pop_est[ind2d])
                        high_res_pop_est = ma.filled(high_res_pop_est, fill_value=-9999)

                        high_res_arr[i] = high_res_pop_est
                        # Use the profile of the higher resolution grid since the
                        # population values have been redistributed to that grid.

                    profile.update(nodata=-9999, count=pop_est_bands.shape[0])

                    hires_pop_outname = os.path.join(
                        tmpdir, f"{split_name[0]}_{self.country_level}_hires_pop.tiff",
                    )
                    # Write out spatially distributed population estimate to raster band

                    with rasterio.open(hires_pop_outname, "w", **profile) as out_raster:
                        out_raster.update_tags(Time=time_str)
                        out_raster.write(
                            high_res_arr.astype(rasterio.float32), tuple(band_ls)
                        )
                        # tag the bands!
                        for i in range(pop_est_bands.shape[0]):
                            band_name = (
                                "population_" + raster_meta["Band_" + str(i + 1)]
                            )
                            out_raster.set_band_description(i + 1, band_name)
                            out_raster.update_tags(
                                **{"Band_{}".format(i + 1): band_name}
                            )


@requires(CreateHighResPopulationEstimateRaster, GlobalParameters)
class HiResPopRasterMasked(Task):

    """
    applies masking to hires(hr) raster using geography json file from UI front end.
    The geography GeoParameter is inherite from HighResMalnRaster
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=31536000, task=self)

    def run(self):
        hr_pop_raster_ls = os.listdir(self.input()[0].path)

        if self.country_level == COUNTRY_CHOICES["SS"]:
            crs_ref = REF_PROJ_CONFIG_SS["srs"]
        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            crs_ref = REF_PROJ_CONFIG_ETH["srs"]

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)

            for hr_raster_file in hr_pop_raster_ls:
                hr_raster_fname = os.path.join(self.input()[0].path, hr_raster_file)
                split_name = hr_raster_fname.split("/")[-1].split("_")
                geography = geography_f_country(self.country_level)
                try:
                    geo_mask = geography["features"][0]["geometry"]
                except KeyError:
                    geo_mask = geography

                with rasterio.open(hr_raster_fname) as src:
                    masked_image, masked_transform = mask(src, [geo_mask], crop=True)
                    band_counts = src.count
                    raster_meta = src.tags()
                    out_meta = src.meta.copy()
                    out_meta.update(
                        {
                            "driver": "GTiff",
                            "crs": crs_ref,
                            "height": masked_image.shape[1],
                            "width": masked_image.shape[2],
                            "transform": masked_transform,
                            "dtype": rasterio.int32,
                            "nodata": -9999,
                        }
                    )

                hr_raster_maskname = os.path.join(
                    tmpdir,
                    f"{split_name[0]}_{self.country_level}_hires_masked_pop.tiff",
                )
                with rasterio.open(hr_raster_maskname, "w", **out_meta) as dest:
                    dest.write(masked_image.astype(rasterio.int32))

                    # tag the bands!
                    for i in range(band_counts):
                        band_name = raster_meta["Band_" + str(i + 1)]
                        dest.set_band_description(i + 1, band_name)
                        dest.update_tags(**{"Band_{}".format(i + 1): band_name})


@requires(EstimatePopulation)
class CSVToDatabase_PopulationEstimate(PushCSVToDatabase):
    temporal_fields = luigi.ListParameter(default=["YEAR"])


# 'luigi --module models.population_model.tasks models.population_model.tasks.CSVToDatabase_PopulationEstimate --admin-level admin2 --country-level Ethiopia --local-scheduler '
