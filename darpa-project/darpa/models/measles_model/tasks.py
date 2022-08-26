import os
import pickle

import geopandas as gpd
import luigi
import numpy as np
import pandas as pd

# import statsmodels.api as sm
from affine import Affine
from kiluigi.targets import FinalTarget, IntermediateTarget
from kiluigi.targets.ckan import CkanTarget
from kiluigi.tasks import ExternalTask, Task
from luigi.configuration import get_config
from luigi.util import inherits, requires
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler

from models.malnutrition_model.functions.helper_func import retrieve_file
from models.measles_model.functions.helper_func import (
    column_standardization,
    proj_val,
    timelagged_val,
)
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

# from utils.scenario_tasks.functions.geography import MaskDataToGeography


CONFIG = get_config()
MEASLES_OUTPUT_PATH = os.path.join(CONFIG.get("paths", "output_path"), "measles_model")

# Check if output path exists, if not create it
if not os.path.exists(MEASLES_OUTPUT_PATH):
    os.makedirs(MEASLES_OUTPUT_PATH, exist_ok=True)


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
    "ncolsrows": (1416, 1047),
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
    "ncolsrows": (1799, 1377),
}


class InfectionData(ExternalTask):

    """
    Task for pulling infectious disease data sets for South Sudan and Ethiopia
    """

    def output(self):

        return {
            "eth_infection": CkanTarget(
                dataset={"id": "28ed1647-dd84-42c8-89e9-5af8f4771112"},
                resource={"id": "63465e2d-0936-4dbd-92b3-060cebd65157"},
            ),
            "ssd_infection": CkanTarget(
                dataset={"id": "28ed1647-dd84-42c8-89e9-5af8f4771112"},
                resource={"id": "58c9ecde-ee63-4284-9221-c7422ed99a46"},
            ),
        }


class CkanMeaslVars(ExternalTask):
    """
    Input variables to the model, population and number of clinics
    """

    def output(self):

        return {
            "pop_census_eth": CkanTarget(
                dataset={"id": "f6544ffc-3f7f-4170-8520-e1b75c657f6e"},
                resource={"id": "b3a49113-780c-481c-8bed-eae7e2bf7e80"},
            ),
            "pop_census_ssd": CkanTarget(
                dataset={"id": "f6544ffc-3f7f-4170-8520-e1b75c657f6e"},
                resource={"id": "73aeb9fb-ae0f-4a1d-88e0-4299f6eb9637"},
            ),
            "subhara_clinic": CkanTarget(
                dataset={"id": "663d3707-5512-4640-ae1f-fdeda4e61077"},
                resource={"id": "779f6740-4973-47a7-90b9-fb5b127aa1a2"},
            ),
        }


class CkanShpfiles_eth(ExternalTask):

    """
    Task for pulling EThiopia shape files and population density tiff file (landscan)
    """

    def output(self):
        return {
            "eth_county_shape": CkanTarget(
                dataset={"id": "d07b30c6-4909-43fa-914b-b3b435bef314"},
                resource={"id": "d4804e8a-5146-48cd-8a36-557f981b073c"},
            ),
            "eth_county_pop_density": CkanTarget(
                dataset={"id": "11bd6b1e-6939-4f00-839d-fdff1a470dd1"},
                resource={"id": "fef4be94-0f26-4861-bc75-7e7c1927af24"},
            ),
        }


class CkanShpfiles_ssd(ExternalTask):

    """
    Task for pulling SSD shape files and population density tiff file (landscan)
    """

    def output(self):

        return {
            "county_shape": CkanTarget(
                dataset={"id": "79e631b6-86f2-4fc8-8054-31c17eac1e3f"},
                resource={"id": "4c110606-e7c7-45b3-8f8b-9f8032b9c3fe"},
            ),
            "ssd_county_pop_density": CkanTarget(
                dataset={"id": "83a07464-6aa1-4611-8e51-d2e04c4a7b97"},
                resource={"id": "146eafe7-28fe-4ccd-8daa-dc95e5252389"},
            ),
        }


@requires(CkanMeaslVars, InfectionData, CkanShpfiles_eth, CkanShpfiles_ssd)
class RetrvCkanData(Task):

    """
    retrieves data from CKAN and returns the dir path name dictionary
    """

    outfolder = MEASLES_OUTPUT_PATH

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        ckan_data_dict = self.input()[0]  # returns dictionary
        ckan_infect_dict = self.input()[1]
        ckan_shape_eth_dict = self.input()[2]
        ckan_shape_ssd_dict = self.input()[3]

        eth_infect_dir = ckan_infect_dict[list(ckan_infect_dict.keys())[0]].path
        ssd_infect_dir = ckan_infect_dict[list(ckan_infect_dict.keys())[1]].path
        eth_pop_dir = ckan_data_dict[list(ckan_data_dict.keys())[0]].path
        ssd_pop_dir = ckan_data_dict[list(ckan_data_dict.keys())[1]].path
        subhara_clinic_dir = ckan_data_dict[list(ckan_data_dict.keys())[2]].path

        shp_eth_dir = ckan_shape_eth_dict[list(ckan_shape_eth_dict.keys())[0]].path
        shp_ssd_dir = ckan_shape_ssd_dict[list(ckan_shape_ssd_dict.keys())[0]].path

        retrieve_file(shp_eth_dir, ".zip", self.outfolder)
        retrieve_file(shp_ssd_dir, ".zip", self.outfolder)

        shp_eth_path = os.path.join(self.outfolder, shp_eth_dir.split("/")[-2]) + "/"

        shp_ssd_path = os.path.join(self.outfolder, shp_ssd_dir.split("/")[-2]) + "/"
        path_names = {
            "eth_infect_dir": eth_infect_dir,
            "ssd_infect_dir": ssd_infect_dir,
            "eth_pop_dir": eth_pop_dir,
            "ssd_pop_dir": ssd_pop_dir,
            "subhara_clinic_dir": subhara_clinic_dir,
            "shp_eth_dir": shp_eth_path,
            "shp_ssd_dir": shp_ssd_path,
        }
        with self.output().open("w") as output:
            output.write(path_names)


@requires(RetrvCkanData)
class MergeDF_eth(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        path_names = self.input().open().read()
        eth_infection = pd.read_csv(path_names["eth_infect_dir"])
        clinic_df = pd.read_excel(path_names["subhara_clinic_dir"])
        eth_census = pd.read_csv(path_names["eth_pop_dir"])

        typo_file = open("models/malnutrition_model/eth_typo.pkl", "rb")
        eth_dict = pickle.load(typo_file)  # noqa: S301
        typo_file.close()

        eth_census = column_standardization(eth_census, "admin2", eth_dict)
        eth_pop_proj = proj_val(eth_census)
        eth_pop_proj.drop_duplicates(
            subset=[
                "admin0",
                "admin1",
                "admin2",
                "admin3",
                "year",
                "datasourceid",
                "census_or_projected",
            ],
            inplace=True,
        )

        # last census in ETH was in 2007, Remote duplicate projection values
        drop_idx = eth_pop_proj[
            (eth_pop_proj["year"] == 2007)
            & (eth_pop_proj["census_or_projected"] == "Projected")
            | (eth_pop_proj["admin3"].isnull())
        ].index
        eth_pop_proj.drop(drop_idx, inplace=True)
        eth_pop_admin1 = (
            eth_pop_proj.groupby(["admin1", "year"])["btotl"].sum().reset_index()
        )

        # map typo from infection dataset
        eth_infection.replace(
            {
                "Benshangul Gumuz": "Benishangul Gumuz",
                "Gambela": "Gambella",
                "Oromia": "Oromiya",
                "SNNPR": "SNNP",
            },
            inplace=True,
        )

        eth_pop_admin1.rename(
            columns={"year": "Year", "admin1": "Admin 1"}, inplace=True
        )
        eth_infection_merged = eth_infection.merge(
            eth_pop_admin1, how="left", on=["Year", "Admin 1"]
        )
        # merge data with clinic df
        eth_clinic_df = clinic_df.loc[clinic_df["Country"] == "Ethiopia"]
        eth_clinic_df["Admin1"].replace(
            {
                "Benishangul-Gumuz": "Benishangul Gumuz",
                "Southern Nations, Nationalities, and Peoples' Region": "SNNP",
            },
            inplace=True,
        )
        eth_clinic_admin1 = (
            eth_clinic_df.groupby(["Admin1"])["Facility type"].count().reset_index()
        )
        eth_infection_merged2 = eth_infection_merged.merge(
            eth_clinic_admin1, how="left", left_on=["Admin 1"], right_on=["Admin1"]
        )

        Months = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]
        mapping = {month: i + 1 for i, month in enumerate(Months)}
        key = eth_infection_merged2["Month"].map(mapping)

        eth_infection_merged2["month"] = key
        eth_infection_merged2["Day"] = 1

        # eth_infection_merged
        eth_infection_merged2["timestamp"] = pd.to_datetime(
            eth_infection_merged2[["Year", "month", "Day"]]
        )
        eth_infection_merged2.sort_values(by=["timestamp"], inplace=True)

        with self.output().open("w") as output:
            output.write(eth_infection_merged2)


@requires(MergeDF_eth)
class lagDF_eth(Task):
    """
    lag time for cases of measles from one month ago (if available)
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        eth_infection_merged2 = self.input().open().read()

        eth_infection_merged2["log_clinics"] = np.log(
            eth_infection_merged2["Facility type"] + 1
        )
        eth_infection_merged2["log_cases"] = np.log(
            eth_infection_merged2["Total Cases"] + 1
        )
        eth_infection_merged2["log_pop"] = np.log(eth_infection_merged2["btotl"] + 1)
        eth_measles_merged = eth_infection_merged2.loc[
            eth_infection_merged2["Disease"] == "Measles"
        ]
        # print('before time lag',eth_measles_merged['Total Cases'])
        eth_measles_df = timelagged_val(eth_measles_merged)
        # print(eth_measles_df.head())
        eth_measles_df2 = eth_measles_df.dropna(subset=["Total Cases_lag1"])

        select_cols = [
            "Admin 1",
            "Month",
            "Year",
            "timestamp",
            "Total Cases",
            "log_pop",
            "btotl",
            "Facility type",
            "log_clinics",
            "log_cases",
            "log_cases_lag1",
            "Total Cases_lag1",
        ]
        measles_select_df = eth_measles_df2[select_cols]
        measles_select_df.rename(
            columns={
                "Admin 1": "Admin_1",
                "btotl": "pop",
                "Total Cases": "Total_Cases",
                "Facility type": "num_clinics",
                "Total Cases_lag1": "total_cases_lag1",
            },
            inplace=True,
        )

        with self.output().open("w") as output:
            output.write(measles_select_df)


@requires(RetrvCkanData)
class MergeDF_ssd(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        path_names = self.input().open().read()
        ssd_infection = pd.read_csv(path_names["ssd_infect_dir"])
        clinic_df = pd.read_excel(path_names["subhara_clinic_dir"])
        ssd_census = pd.read_csv(path_names["ssd_pop_dir"])

        ssd_census["admin2"].replace(
            {"Western Equtoria": "Western Equatoria"}, inplace=True
        )
        ssd_pop_proj = proj_val(ssd_census, yr_thresh=2015)
        # get rid of weird duplicate values on ssd_pop_proj
        ssd_pop_proj.drop_duplicates(
            subset=[
                "admin0",
                "admin1",
                "admin2",
                "admin3",
                "year",
                "datasourceid",
                "census_or_projected",
            ],
            inplace=True,
        )
        ssd_pop_admin1 = (
            ssd_pop_proj.groupby(["admin2", "year"])["btotl"].sum().reset_index()
        )
        ssd_pop_admin1.rename(
            columns={"year": "Year", "admin2": "Admin 2"}, inplace=True
        )
        ssd_infection_merged = ssd_infection.merge(
            ssd_pop_admin1, how="left", on=["Year", "Admin 2"]
        )
        ssd_clinic_df = clinic_df.loc[clinic_df["Country"] == "South Sudan"]
        ssd_clinic_admin1 = (
            ssd_clinic_df.groupby(["Admin1"])["Facility type"].count().reset_index()
        )
        ssd_infection_merged2 = ssd_infection_merged.merge(
            ssd_clinic_admin1, how="left", left_on=["Admin 2"], right_on=["Admin1"]
        )
        Months = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]
        mapping = {month: i + 1 for i, month in enumerate(Months)}
        key2 = ssd_infection_merged2["Month"].map(mapping)
        ssd_infection_merged2["month"] = key2
        ssd_infection_merged2["Day"] = 1

        # ssd_infection_merged
        ssd_infection_merged2["timestamp"] = pd.to_datetime(
            ssd_infection_merged2[["Year", "month", "Day"]]
        )
        ssd_infection_merged2.sort_values(by=["timestamp"], inplace=True)

        with self.output().open("w") as output:
            output.write(ssd_infection_merged2)


@requires(MergeDF_ssd)
@inherits(lagDF_eth)
class lagDF_ssd(Task):
    """
    lag time for cases of measles from one month ago (if available)
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        ssd_infection_merged2 = self.input().open().read()
        # ssd_infection_merged2["Facility type"] = ssd_infection_merged2[
        #     "Facility type"
        # ] * (ssd_infection_merged2["Facility type"] + self.num_clinics_percent_change)
        ssd_infection_merged2["log_clinics"] = np.log(
            ssd_infection_merged2["Facility type"] + 1
        )
        ssd_infection_merged2["log_cases"] = np.log(
            ssd_infection_merged2["Total cases"] + 1
        )
        ssd_infection_merged2["log_pop"] = np.log(ssd_infection_merged2["btotl"] + 1)
        ssd_infection_merged2.rename(
            columns={"Admin 2": "Admin 1", "Total cases": "Total Cases"}, inplace=True
        )
        ssd_measles_merged = ssd_infection_merged2.loc[
            ssd_infection_merged2["Disease"] == "Measles"
        ]
        ssd_measles_df = timelagged_val(ssd_measles_merged)
        ssd_measles_df2 = ssd_measles_df.dropna(subset=["Total Cases_lag1"])

        select_cols = [
            "Admin 1",
            "Month",
            "Year",
            "timestamp",
            "Total Cases",
            "log_pop",
            "btotl",
            "Facility type",
            "log_clinics",
            "log_cases",
            "log_cases_lag1",
            "Total Cases_lag1",
        ]
        measles_select_df = ssd_measles_df2[select_cols]
        measles_select_df.rename(
            columns={
                "Admin 1": "Admin_1",
                "btotl": "pop",
                "Total Cases": "Total_Cases",
                "Facility type": "num_clinics",
                "Total Cases_lag1": "total_cases_lag1",
            },
            inplace=True,
        )

        with self.output().open("w") as output:
            output.write(measles_select_df)


@requires(lagDF_eth, lagDF_ssd, GlobalParameters)
class MeaslesInferTime(Task):
    """
    takes the dataset from ethiopia and ssd and combine it together
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        measles_eth_lag = self.input()[0].open().read()
        measles_ssd_lag = self.input()[1].open().read()

        measles_eth_lag["Admin_0"] = "Ethiopia"
        measles_ssd_lag["Admin_0"] = "South Sudan"
        if self.country_level == "Ethiopia":
            measles_lag_df = measles_eth_lag.copy()
        elif self.country_level == "South Sudan":
            measles_lag_df = measles_ssd_lag.copy()
        else:
            raise NotImplementedError
        with self.output().open("w") as df_output:
            df_output.write(measles_lag_df)


@requires(lagDF_eth)
class mixedlinear_model(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        measles_select_df = self.input().open().read()
        measles_select_df["num_clinics_per_population"] = (
            measles_select_df["num_clinics"] / measles_select_df["pop"]
        )
        measles_select_df["month"] = measles_select_df["timestamp"].dt.strftime("%m")
        measles_select_df["month"] = measles_select_df["month"].astype(int)
        train_df = measles_select_df[:-10]
        val_df = measles_select_df[-10:]
        y = train_df["Total_Cases"]
        X = train_df[["total_cases_lag1", "num_clinics_per_population", "month"]]
        preprocessor = ColumnTransformer(
            [
                (
                    "numeric",
                    MinMaxScaler(),
                    ["total_cases_lag1", "num_clinics_per_population"],
                ),
            ]
        )
        estimator = LinearRegression()
        pipeline = Pipeline([("preprocessor", preprocessor), ("estimator", estimator)])
        pipeline.fit(X, y)
        X_test = val_df[["total_cases_lag1", "num_clinics_per_population", "month"]]
        eval_prediction = pipeline.predict(X_test)
        target = val_df["Total_Cases"]
        print("rmse: ", np.sqrt(mean_squared_error(target, eval_prediction)))
        print("r2 score: ", r2_score(target, eval_prediction))
        with self.output().open("w") as model_output:
            model_output.write(pipeline)


@requires(mixedlinear_model, MeaslesInferTime)
class MeaslesInference(Task):
    """
    Runs inference on selected dataframe filtered by user parameter (e.g.time, country_level)
    """

    num_clinics_percent_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0],
        var_type=np.float,
        description="Percentage increase in the number of clinics",
    )

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        mdf1 = self.input()[0].open().read()
        eval_df = self.input()[1].open().read()

        eval_df["num_clinics"] = eval_df["num_clinics"] * (
            1 + self.num_clinics_percent_change
        )
        eval_df["num_clinics_per_population"] = eval_df["num_clinics"] / eval_df["pop"]
        eval_df["month"] = eval_df["timestamp"].dt.strftime("%m")
        eval_df["month"] = eval_df["month"].astype(int)
        eval_df.drop(["Total_Cases", "log_cases"], axis=1, inplace=True)
        # select only the independent variables
        feat_cols = ["total_cases_lag1", "num_clinics_per_population", "month"]
        X_new = eval_df[feat_cols]
        prediction = mdf1.predict(X_new)
        eval_df["measles_number"] = prediction
        print(eval_df[["measles_number"]].head())
        with self.output().open("w") as model_output:
            model_output.write(eval_df)


@requires(MeaslesInference, RetrvCkanData)
class PredictionGeojson(Task):
    """
    output the data in geojson after merging with shape files
    """

    def output(self):
        return FinalTarget(path="measles.geojson", task=self, ACL="public-read")
        # return FinalTarget(path="measles.geojson", task=self)

    def run(self):

        measles_pred = self.input()[0].open().read()

        if self.country_level == COUNTRY_CHOICES["SS"]:
            with self.input()[1].open("r") as src:
                geo_dir = src.read()["shp_ssd_dir"]
                # geo_dir.rename(columns={"ADMIN_1": "admin1"}, inplace=True)

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            with self.input()[1].open("r") as src:
                geo_dir = src.read()["shp_eth_dir"]

        shp_file = [x for x in os.listdir(geo_dir) if x.endswith(".shp")][0]
        shp_file = os.path.join(geo_dir, shp_file)
        if self.country_level == COUNTRY_CHOICES["SS"]:

            gdf = gpd.read_file(shp_file)
            gdf.rename(columns={"ADMIN_1": "admin1"}, inplace=True)

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            gdf = (
                gpd.read_file(shp_file).explode().reset_index().sort_index()
            )  # .rename(columns={0: "geometry"})

        gdf = gdf[["admin1", "geometry"]]
        gdf_admin1 = gdf.dissolve(by="admin1")
        gdf_admin1["admin1"] = gdf_admin1.index.values

        gdf_admin1 = gdf_admin1.reset_index(drop=True).explode().reset_index()
        # remove duplicate admin1s
        gdf_admin1.drop_duplicates(subset=["admin1"], keep="first", inplace=True)
        measles_merge_df = measles_pred.merge(
            gdf_admin1[["admin1", "geometry"]], left_on="Admin_1", right_on="admin1"
        )

        measles_merge_df.drop(columns=["log_clinics", "log_cases_lag1"], inplace=True)
        measles_geo_df = gpd.GeoDataFrame(measles_merge_df)
        measles_geo_df["measles_number"] = measles_geo_df["measles_number"].astype(
            "int"
        )
        measles_geo_df["measles_number"][measles_geo_df["measles_number"] < 0] = 0

        with self.output().open("w") as out:
            measles_geo_df.to_file(out.name, driver="GeoJSON")

    # measles_geo_df.to_file(self.output().path, driver="GeoJSON")


@requires(MeaslesInference)
class MeaslesPrediction(Task):
    def output(self):
        return FinalTarget(
            path=f"measles_model/measles_{self.num_clinics_percent_change}.csv",
            task=self,
        )

    def run(self):
        with self.input().open() as src:
            df = src.read()
        df.loc[df["measles_number"] < 0, "measles_number"] = 0
        df["measles_number"] = df["measles_number"].astype(int)
        with self.output().open("w") as out:
            df.to_csv(out.name, index=False)
