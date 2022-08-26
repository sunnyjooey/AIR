import datetime
import os
import pickle

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from affine import Affine

# from kiluigi.parameter import GeoParameter
from kiluigi.targets import FinalTarget, IntermediateTarget
from kiluigi.targets.ckan import CkanTarget
from kiluigi.tasks import ExternalTask, Task, VariableInfluenceMixin
from luigi.configuration import get_config
from luigi.util import requires
from rasterio import features
from rasterio.features import shapes
from rasterio.mask import mask
from scipy.stats import randint as sp_randint
from shapely.geometry import shape
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import r2_score
from sklearn.model_selection import RandomizedSearchCV

from models.malnutrition_model.data_cleaning import (
    RetrvCkanData,
    RetrvDataEth,
    RetrvDataFrames,
    VarsStandardized_train,
)
from models.malnutrition_model.functions.helper_func import retrieve_file
from models.malnutrition_model.functions.maln_utility_func import (
    add_missing_dummy_columns,
    column_standardization,
    merge_rf_scenario,
    county_expend,
    df_aggregate,
    estimate_pop,
    eth_df_aggregate,
    malnutrition_mapping,
)

from models.market_price_model.utils import datetime_schema, merge_with_shapefile
from utils.geospatial_tasks.functions.geospatial import geography_f_country
from utils.scenario_tasks.functions.geography import MaskDataToGeography
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

CONFIG = get_config()
MALNUTRITION_OUTPUT_PATH = os.path.join(
    CONFIG.get("paths", "output_path"), "malnutrition_model"
)

# Check if output path exists, if not create it
if not os.path.exists(MALNUTRITION_OUTPUT_PATH):
    os.makedirs(MALNUTRITION_OUTPUT_PATH, exist_ok=True)

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


# --------------
# Model Training
# --------------


@requires(VarsStandardized_train, RetrvDataFrames, GlobalParameters)
class GAM_gbm_model(VariableInfluenceMixin, Task):

    """
    train the data using GBM(gradient boosting machine) models and GAM and then save the models
    """

    # NOTE: in breaking the 'rules' and having multiple outputs in one task, it's best practice to use ExpiringLocalTarget()
    # because it writes to each target atomically (https://luigi.readthedocs.io/en/stable/tasks.html#task-output)
    # if atomicity is not a concern, stick to SerializerTarget()
    def output(self):
        return {
            "gb_gam": IntermediateTarget(task=self, timeout=31536000),
            "gam_grid": self.get_influence_grid_target(),
        }

    def run(self):
        select_train_df = self.input()[0]["standard_vars"].open().read()
        scaler = self.input()[0]["scaler"].open().read()
        path_names = self.input()[1].open().read()

        eth_select_df = pd.read_csv(path_names["eth_select_df_v3"])
        ss_select_df = pd.read_csv(path_names["ssd_select_df_v3"])

        ssd_train_df = ss_select_df[ss_select_df["Year"] < 2018]
        eth_train_df = eth_select_df[(eth_select_df["Year"] < 2014)]

        ssd_validation_df = ss_select_df[ss_select_df["Year"] >= 2018]
        eth_validation_df = eth_select_df[eth_select_df["Year"] >= 2014]

        select_vars = [
            "gam_rate",
            "sam_rate",
            "Year",
            "NDVI_lag1",
            "Population",
            "CPI",
            "crop_per_capita",
            "CHIRPS(mm)_lag3",
            "med_exp",
            "Apr",
            "Aug",
            "Dec",
            "Feb",
            "Jan",
            "Jul",
            "Jun",
            "Mar",
            "May",
            "Nov",
            "Oct",
            "Sep",
        ]
        select_val_df = pd.concat([ssd_validation_df, eth_validation_df], axis=0)[
            select_vars[3:]
        ]

        target_gam_train = pd.concat([ssd_train_df, eth_train_df], axis=0)[
            select_vars[0]
        ]

        target_gam_val = pd.concat([ssd_validation_df, eth_validation_df], axis=0)[
            select_vars[0]
        ]

        select_val_df[
            [
                "NDVI_lag1",
                "Population",
                "CPI",
                "crop_per_capita",
                "CHIRPS(mm)_lag3",
                "med_exp",
            ]
        ] = scaler.transform(select_val_df[select_vars[3:9]])

        # GridSearch for model
        param_gb = {
            "max_depth": sp_randint(2, 8),
            "min_samples_leaf": sp_randint(2, 10),
            "n_estimators": sp_randint(2000, 4000),
            "min_samples_split": sp_randint(2, 10),
        }

        gb_gam_gs = GradientBoostingRegressor(
            learning_rate=0.001, criterion="mse", random_state=42
        )
        gb_gam_search = RandomizedSearchCV(
            gb_gam_gs,
            param_distributions=param_gb,
            n_iter=20,
            cv=3,
            verbose=0,
            random_state=42,
        )
        gb_gam_search.fit(select_train_df, target_gam_train)

        # This is NOT redundant: retrain in order to derive feature importance
        gb_gam = gb_gam_search.best_estimator_

        predicted_gb_gam = gb_gam.predict(select_val_df)
        # @TODO: Consider replacing the print statement below with logging if log
        # info is still needed.
        print(
            "r2 for GAM (gb) prediction on validation: {}".format(
                r2_score(target_gam_val, predicted_gb_gam)
            )
        )

        with self.output()["gb_gam"].open("w") as model_output:
            model_output.write(gb_gam)

        gam_grid = pd.DataFrame(
            {
                "source_variable": select_vars[3:],
                "influence_percent": gb_gam.feature_importances_,
            }
        )
        self.store_influence_grid("gam_influence_grid", gam_grid)


@requires(VarsStandardized_train, RetrvDataFrames, GlobalParameters)
class SAM_gbm_model(VariableInfluenceMixin, Task):

    """
    Train the data using gbm(gradient boosting machine) models and SAM and then save the models
    """

    def output(self):
        return {
            "gb_sam": IntermediateTarget(task=self, timeout=31536000),
            "sam_grid": self.get_influence_grid_target(),
        }

    def run(self):
        select_train_df = self.input()[0]["standard_vars"].open().read()
        scaler = self.input()[0]["scaler"].open().read()
        path_names = self.input()[1].open().read()

        eth_select_df = pd.read_csv(path_names["eth_select_df_v3"])
        ss_select_df = pd.read_csv(path_names["ssd_select_df_v3"])

        ssd_train_df = ss_select_df[ss_select_df["Year"] < 2018]
        eth_train_df = eth_select_df[(eth_select_df["Year"] < 2014)]

        ssd_validation_df = ss_select_df[ss_select_df["Year"] >= 2018]
        eth_validation_df = eth_select_df[eth_select_df["Year"] >= 2014]

        select_vars = [
            "gam_rate",
            "sam_rate",
            "Year",
            "NDVI_lag1",
            "Population",
            "CPI",
            "crop_per_capita",
            "CHIRPS(mm)_lag3",
            "med_exp",
            "Apr",
            "Aug",
            "Dec",
            "Feb",
            "Jan",
            "Jul",
            "Jun",
            "Mar",
            "May",
            "Nov",
            "Oct",
            "Sep",
        ]
        select_val_df = pd.concat([ssd_validation_df, eth_validation_df], axis=0)[
            select_vars[3:]
        ]

        target_sam_train = pd.concat([ssd_train_df, eth_train_df], axis=0)[
            select_vars[1]
        ]

        target_sam_val = pd.concat([ssd_validation_df, eth_validation_df], axis=0)[
            select_vars[1]
        ]

        select_val_df[
            [
                "NDVI_lag1",
                "Population",
                "CPI",
                "crop_per_capita",
                "CHIRPS(mm)_lag3",
                "med_exp",
            ]
        ] = scaler.transform(select_val_df[select_vars[3:9]])

        # GridSearch for model
        param_gb = {
            "max_depth": sp_randint(2, 8),
            "min_samples_leaf": sp_randint(2, 10),
            "n_estimators": sp_randint(2000, 4000),
            "min_samples_split": sp_randint(2, 10),
        }

        gb_sam_gs = GradientBoostingRegressor(
            learning_rate=0.001, criterion="mse", random_state=42
        )
        gb_sam_search = RandomizedSearchCV(
            gb_sam_gs,
            param_distributions=param_gb,
            n_iter=20,
            cv=3,
            verbose=0,
            random_state=42,
        )
        gb_sam_search.fit(select_train_df, target_sam_train)

        # This is NOT redundant: retrain in order to derive feature importance
        gb_sam = gb_sam_search.best_estimator_

        predicted_gb_sam = gb_sam.predict(select_val_df)
        # @TODO: Consider replacing the print statement below with logging if log
        # info is still needed.
        print(
            "r2 for SAM (gb) prediction on validation: {}".format(
                r2_score(target_sam_val, predicted_gb_sam)
            )
        )

        with self.output()["gb_sam"].open("w") as model_output:
            model_output.write(gb_sam)

        sam_grid = pd.DataFrame(
            {
                "source_variable": select_vars[3:],
                "influence_percent": gb_sam.feature_importances_,
            }
        )
        self.store_influence_grid("sam_influence_grid", sam_grid)


# ---------------
# Model Inference
# ---------------


class InferenceVars(ExternalTask):

    """
    Task for importing template for model inference.
    """

    def output(self):

        return {
            "ssd_infer_vars": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "43c766f0-1c76-4f94-8e58-7bd2ebaef3fe"},
            ),
            "eth_infer_vars": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "40b515b5-d185-4594-965a-1d821fb378da"},
            ),
        }


class PullEconomicModel(ExternalTask):

    """
    Task for importing template for model inference. For ethiopia, use class CkanMalnVars_eth output "expenditure"
    """

    def output(self):

        return CkanTarget(
            dataset={"id": "8399cfb4-fb5f-4e71-93a9-db77ce3b74a4"},
            resource={"id": "962b32b9-59a4-4907-b698-fd3b2d4a80af"},
        )


class RainfallEstimate(ExternalTask):

    """
    Task for importing csv files of the rainfall estimate for high/low/mean rainfall"
    """

    def output(self):

        return {
            "ssd_rainfall": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "dbb8738a-3be7-4f56-8cf8-54ae27d2d976"},
            ),
            "eth_rainfall": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "3bbcafcb-6f6f-4678-9915-48f1081c3fcb"},
            ),
        }


@requires(
    RetrvCkanData,
    InferenceVars,
    GlobalParameters,
    PullEconomicModel,
    RetrvDataEth,
    RainfallEstimate,
)
class MalnutDFInferTime(Task):
    """
    This fetches the DateIntervalParameter, which passes user defined time interval from the front end UI.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        path_names = self.input()[0].open().read()
        econ_out_raster = self.input()[3].get()

        #  open the inference gdf
        if self.country_level == COUNTRY_CHOICES["SS"]:
            infer_vars_path = self.input()[1][
                "ssd_infer_vars"
            ]  # when it's directly ckantarget, use self.input()
            path_names = self.input()[0].open().read()
            # for consumption expenditure, some counties have NaN values.
            expend_df = county_expend(path_names["county_dir"], econ_out_raster)
            ndvi_df = pd.read_csv(path_names["ndvi_dir"])
            chirps_df = pd.read_csv(path_names["chirps_dir"])
            cpi_df = retrieve_file(path_names["cpi_dir"], ".csv")
            crop_df = pd.read_csv(path_names["crop_dir"])
            pop_est_df = estimate_pop(path_names["ss_rates_dir"])
            infer_var_df = pd.read_csv(infer_vars_path.path)

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            infer_vars_path = self.input()[1]["eth_infer_vars"]
            # infer_vars_dir = infer_vars_path[list(infer_vars_path.keys())[0]].path
            # path_names already has the file directory paths
            path_names = self.input()[4].open().read()

            expend_df = pd.read_csv(path_names["expend_dir"])
            ndvi_df = pd.read_csv(path_names["ndvi_dir"])
            chirps_df = pd.read_csv(path_names["chirps_dir"])
            cpi_df = pd.read_csv(path_names["cpi_dir"])
            crop_df = pd.read_csv(path_names["crop_dir"])
            pop_est_df = pd.read_csv(path_names["pop_dir"])
            infer_var_df = pd.read_csv(infer_vars_path.path)

        # create a range of time based on DateIntervalParameter value

        dt_range = pd.date_range(
            start=self.time.date_a,
            end=self.time.date_b - datetime.timedelta(days=1),
            freq="M",
        ).tolist()
        dt_append = []

        for dt_point in dt_range:

            dt_str = dt_point.strftime("%Y-%m-%d")
            dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d")
            infer_scn_df = infer_var_df.copy()
            infer_scn_df["Year"] = dt.year
            infer_scn_df["Month"] = dt.strftime("%b")
            infer_scn_df["month_int"] = dt.month
            dt_append.append(infer_scn_df)

        infer_scn_df = pd.concat(dt_append, axis=0)
        infer_scn_df["GAM_rate"] = 0.00001  # place holder values for column
        infer_scn_df["SAM_rate"] = 0.00001  # place holder values for column

        if self.country_level == COUNTRY_CHOICES["SS"]:
            # fix typo for south sudan data. This might change later with data warehouse
            infer_scn_df["county_lower"] = infer_scn_df["County"]

            infer_scn_df["Country"] = "South Sudan"
            # load the typo dictionary, specify path relative to PYTHONPATH env variable
            pkl_file = open("models/malnutrition_model/typo_dict.pkl", "rb")
            ssd_typo = pickle.load(pkl_file)  # noqa: S301
            pkl_file.close()
            typo_list = list(ssd_typo.keys())

            rev_cname = []
            for _i, row in infer_scn_df.iterrows():
                if row["county_lower"] in typo_list:
                    rev_cname.append(ssd_typo[row["county_lower"]])
                else:
                    rev_cname.append(row["county_lower"].strip())

            # merge chirps with the rainfall estimate
            rain_ssd_fname = self.input()[5]["ssd_rainfall"]
            rain_ssd_df = pd.read_csv(rain_ssd_fname.path)
            rain_ssd_std = column_standardization(rain_ssd_df, "admin2", ssd_typo)
            # chirps_merged_df = merge_rf_scenario(rain_ssd_std, chirps_df)

            smart_df = df_aggregate(
                infer_scn_df,
                crop_df,
                pop_est_df,
                ndvi_df,
                chirps_df,
                expend_df,
                cpi_df,
                rev_cname,
            )

            smart_df["County"] = infer_scn_df[
                "County"
            ]  # add this for down stream processing
            smart_df["Day"] = 1
            smart_df = merge_rf_scenario(rain_ssd_std, smart_df)

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            typo_file = open("models/malnutrition_model/eth_typo.pkl", "rb")
            eth_dict = pickle.load(typo_file)  # noqa: S301
            typo_file.close()

            pop_df_std = column_standardization(pop_est_df, "admin2", eth_dict)
            ndvi_df_std = column_standardization(ndvi_df, "admin2", eth_dict)
            chirps_df_std = column_standardization(chirps_df, "admin2", eth_dict)
            crop_df_std = column_standardization(crop_df, "Admin2", eth_dict)
            rain_eth_fname = self.input()[5]["eth_rainfall"]
            rain_eth_df = pd.read_csv(rain_eth_fname.path)
            rain_eth_std = column_standardization(rain_eth_df, "admin2", eth_dict)
            # chirps_merged_df = merge_rf_scenario(rain_eth_std, chirps_df_std)

            inf_df_std = column_standardization(infer_scn_df, "admin2", eth_dict)
            inf_df_std["admin0"] = "Ethiopia"

            smart_df = eth_df_aggregate(
                inf_df_std,
                pop_df_std,
                ndvi_df_std,
                chirps_df_std,
                crop_df_std,
                cpi_df,
                expend_df,
            )

            smart_df["County"] = smart_df["admin2"]
            smart_df["Day"] = 1
            smart_df = merge_rf_scenario(rain_eth_std, smart_df)

        with self.output().open("w") as output:
            output.write(smart_df)


@requires(MalnutDFInferTime, RetrvCkanData, RetrvDataEth, GlobalParameters)
class MalnutDFInferRain(Task):
    """
    This fetches the DateIntervalParameter, which passes user defined time interval from the front end UI.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        smart_df = self.input()[0].open().read()
        smart_df.rename(columns={"mean": "mean_rainfall"}, inplace=True)

        # -------------------
        # Scenario Parameters
        # -------------------
        # adjust rainfall for scenario! baseline is always "CHIRPS(mm)_lag3" (which is PercentOfNormalRainfall=1)

        # Get the time range to adjust rainfall based on the rainfall DateIntervalParameter

        # Get a list of months when the date range contains at least one month
        dt_rain = pd.date_range(
            start=self.rainfall_scenario_time.date_a,
            end=self.rainfall_scenario_time.date_b - datetime.timedelta(days=1),
            freq="M",
        ).tolist()
        # if date range didn't cover a month, we use a pd.Timestamp
        if not dt_rain:
            dt_rain = [pd.Timestamp(self.rainfall_scenario_time.date_a, freq="M")]

        try:
            rainfall_scenario_geography = geography_f_country(self.country_level)
            rainfall_geo_mask = rainfall_scenario_geography["features"][0]["geometry"]
        except KeyError:
            rainfall_geo_mask = rainfall_scenario_geography
        # Get the counties to adjust rainfall based on the rainfall GeoParameter
        if self.country_level == COUNTRY_CHOICES["SS"]:
            with self.input()[1].open("r") as src:
                geo_dir = src.read()["county_dir"]
            shp_file = [x for x in os.listdir(geo_dir) if x.endswith(".shp")][0]
            shp_file = os.path.join(geo_dir, shp_file)
            gdf = gpd.read_file(shp_file)

            gdf["geometry"] = gdf.intersection(shape(rainfall_geo_mask))
            gdf = gdf[gdf["geometry"].notnull()]
            geo_rain = [
                x for x in smart_df["County"] if x in list(np.unique(gdf["ADMIN2NAME"]))
            ]

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            with self.input()[2].open("r") as src:
                geo_dir = src.read()["county_dir"]

            shp_file = [x for x in os.listdir(geo_dir) if x.endswith(".shp")][0]
            shp_file = os.path.join(geo_dir, shp_file)
            gdf = gpd.read_file(shp_file)
            # normalize admin name
            typo_file = open("models/malnutrition_model/eth_typo.pkl", "rb")
            typo_dict = pickle.load(typo_file)  # noqa: S301
            typo_file.close()

            gdf_std = column_standardization(gdf, "admin2", typo_dict)
            gdf_std["geometry"] = gdf_std.intersection(shape(rainfall_geo_mask))
            gdf_std = gdf_std[gdf_std["geometry"].notnull()]
            geo_rain = [
                x
                for x in smart_df["County"]
                if x in gdf_std["admin2"].unique().tolist()
            ]

        # For each date in the range, change the rainfall variables based on selected criteria
        if self.rainfall_scenario == "normal":
            smart_df["CHIRPS(mm)_lag3_scenario"] = smart_df["CHIRPS(mm)_lag3"].copy()
        else:
            scn_name = self.rainfall_scenario + "_rainfall"
            smart_df["CHIRPS(mm)_lag3_scenario"] = smart_df[scn_name].copy()

        for dt in dt_rain:
            smart_df.loc[
                (smart_df["Year"] == dt.year)
                & (
                    smart_df["Month"] == dt.strftime("%b")
                )  # DO NOT use dt.month because it returns an integer for month
                & (smart_df["County"].isin(geo_rain)),
                "CHIRPS(mm)_lag3_scenario",
            ]

        # -------------------

        # NOTE: for panda 0.18 and above, use dictionary for to_datetime, because it expects column names to be year, month, day
        smart_df["Time"] = pd.to_datetime(
            {
                "year": smart_df["Year"],
                "month": smart_df["month_int"],
                "day": smart_df["Day"],
            }
        )
        smart_df.drop(["Day", "month_int"], axis=1, inplace=True)
        month_dummy = pd.get_dummies(smart_df.Month)
        month_dummy = add_missing_dummy_columns(month_dummy)
        smart_df2 = pd.concat([smart_df, month_dummy], axis=1)

        with self.output().open("w") as output:
            output.write(smart_df2)


@requires(RetrvCkanData, RetrvDataEth, MalnutDFInferRain, GlobalParameters)
class Inference_geo_df(Task):
    """
    This will merge inference variable dataframe with shapefiles to include geometry column for the given admins (i.e,counties)
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        infer_df = self.input()[2].open().read()
        if self.country_level == COUNTRY_CHOICES["SS"]:
            path_names = self.input()[0].open().read()
            admin_shp = gpd.read_file(path_names["county_dir"])
            shp_select = admin_shp[["ADMIN2NAME", "geometry"]]
            shp_select["County"] = shp_select["ADMIN2NAME"]
            infer_df["Geography"] = infer_df["County"]

            # merge geometry coordinates
            infer_df_merged = merge_with_shapefile(
                infer_df, shp_select, gdf_on="County"
            )

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            path_names = self.input()[1].open().read()
            admin_shp = gpd.read_file(path_names["county_dir"])
            # normalize teh column for shapefile
            typo_file = open("models/malnutrition_model/eth_typo.pkl", "rb")
            typo_dict = pickle.load(typo_file)  # noqa: S301
            typo_file.close()

            admin_shp_std = column_standardization(admin_shp, "admin2", typo_dict)

            shp_select = admin_shp_std[["admin2", "geometry"]]
            shp_select["County"] = shp_select["admin2"]
            infer_df["Geography"] = infer_df["County"]

            # Admin shape file contains multipolygon for geometry
            # explode the geometry coordinates, to use explode make sure geopanda is 0.4.1
            shp_select2 = (
                shp_select.explode().reset_index().rename(columns={0: "geometry"})
            )
            infer_df_merged = merge_with_shapefile(
                infer_df, shp_select2, gdf_on="County"
            )
            infer_df_merged.rename(columns={"admin2_x": "admin2"}, inplace=True)

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            if isinstance(infer_df_merged, gpd.GeoDataFrame):
                print("this is in geopandaframe")

                schema = datetime_schema(infer_df_merged)

                infer_df_merged.to_file(
                    os.path.join(
                        tmpdir, f"maln_infer_masked_{self.country_level}.geojson"
                    ),
                    driver="GeoJSON",
                    schema=schema,
                )


@requires(Inference_geo_df)
class MaskMalnutInferToGeography(GlobalParameters, MaskDataToGeography):
    """
    Mask the Inference_geo_df to geography passed from GlobalParameters
    """

    def complete(self):
        return super(MaskDataToGeography, self).complete()


@requires(MaskMalnutInferToGeography)
class MaskedGeojson(Task):
    """read the masked geojson into dataframe"""

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        input_infer_file = self.input().path
        infer_gdf = gpd.read_file(
            os.path.join(
                input_infer_file, f"maln_infer_masked_{self.country_level}.geojson"
            )
        )
        infer_df = pd.DataFrame(infer_gdf)

        with self.output().open("w") as infer_out:
            infer_out.write(infer_df)


@requires(MaskedGeojson, VarsStandardized_train)
class VarsStandardized_infer(Task):

    """
    standardize the numeric variables, model was trained on standardscaler variables
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        smart_df2 = self.input()[0].open().read()

        if self.country_level == COUNTRY_CHOICES["SS"]:
            admin_col = ["ADMIN2NAME"]
        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            admin_col = ["admin2"]

        select_vars = (
            [
                "NDVI_lag1",
                "Population",
                "CPI",
                "crop_per_capita",
                "CHIRPS(mm)_lag3",
                "med_exp",
                "Apr",
                "Aug",
                "Dec",
                "Feb",
                "Jan",
                "Jul",
                "Jun",
                "Mar",
                "May",
                "Nov",
                "Oct",
                "Sep",
                "Month",
                "Year",
                "Time",
            ]
            + admin_col
            + ["County"]
        )
        select_vars_scn = (
            [
                "NDVI_lag1",
                "Population",
                "CPI",
                "crop_per_capita",
                "CHIRPS(mm)_lag3_scenario",
                "med_exp",
                "Apr",
                "Aug",
                "Dec",
                "Feb",
                "Jan",
                "Jul",
                "Jun",
                "Mar",
                "May",
                "Nov",
                "Oct",
                "Sep",
                "Month",
                "Year",
                "Time",
            ]
            + admin_col
            + ["County"]
        )

        select_df = smart_df2[select_vars].dropna()
        scaler = self.input()[1]["scaler"].open().read()

        select_transform = scaler.transform(select_df[select_vars[:6]])
        select_df[
            [
                "NDVI_lag1",
                "Population",
                "CPI",
                "crop_per_capita",
                "CHIRPS(mm)_lag3",
                "med_exp",
            ]
        ] = select_transform
        # repeat variable standardization for scenario

        select_df_scn = smart_df2[select_vars_scn].dropna()
        select_transform_scn = scaler.transform(select_df_scn[select_vars_scn[:6]])
        select_df_scn[
            [
                "NDVI_lag1",
                "Population",
                "CPI",
                "crop_per_capita",
                "CHIRPS(mm)_lag3_scenario",
                "med_exp",
            ]
        ] = select_transform_scn
        # add the normalized scenario rainfall to main df
        select_df["CHIRPS(mm)_lag3_scenario"] = select_df_scn[
            "CHIRPS(mm)_lag3_scenario"
        ]

        with self.output().open("w") as output:
            output.write(select_df)


@requires(
    VarsStandardized_infer,
    RetrvCkanData,
    RetrvDataEth,
    GAM_gbm_model,
    SAM_gbm_model,
    VarsStandardized_train,
    GlobalParameters,
)
class MalnutInference(Task):

    """
    Gradient Boosting models make inference and returns a csv dataframe file
    Final output can be found at https://darpa-output-dev.s3.amazonaws.com/
    """

    def output(self):
        try:
            return FinalTarget(path="maln_inference.csv", task=self, ACL="public-read")
        except TypeError:
            return FinalTarget(path="maln_inference.csv", task=self)

    def run(self):
        if self.country_level == COUNTRY_CHOICES["SS"]:
            path_names = self.input()[1].open().read()
            admin_col = ["ADMIN2NAME"]

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            path_names = self.input()[2].open().read()
            admin_col = ["admin2"]

        baseline_vars = (
            [
                "NDVI_lag1",
                "Population",
                "CPI",
                "crop_per_capita",
                "CHIRPS(mm)_lag3",
                "med_exp",
                "Apr",
                "Aug",
                "Dec",
                "Feb",
                "Jan",
                "Jul",
                "Jun",
                "Mar",
                "May",
                "Nov",
                "Oct",
                "Sep",
                "Month",
                "Year",
                "Time",
            ]
            + admin_col
            + ["County"]
        )
        scn_vars = (
            [
                "NDVI_lag1",
                "Population",
                "CPI",
                "crop_per_capita",
                "CHIRPS(mm)_lag3_scenario",
                "med_exp",
                "Apr",
                "Aug",
                "Dec",
                "Feb",
                "Jan",
                "Jul",
                "Jun",
                "Mar",
                "May",
                "Nov",
                "Oct",
                "Sep",
                "Month",
                "Year",
                "Time",
            ]
            + admin_col
            + ["County"]
        )

        norm_df = self.input()[0].open().read()[baseline_vars]

        scaler = self.input()[5]["scaler"].open().read()
        gb_gam = self.input()[3]["gb_gam"].open().read()
        gb_sam = self.input()[4]["gb_sam"].open().read()

        # add this after model.predict()
        norm_df["gam_rate"] = gb_gam.predict(norm_df[baseline_vars[:-5]])
        norm_df["sam_rate"] = gb_sam.predict(norm_df[baseline_vars[:-5]])

        # inverse transform !
        inverse_trf_base = scaler.inverse_transform(norm_df[baseline_vars[:6]])
        norm_df[baseline_vars[:6]] = inverse_trf_base

        norm_df["gam_cases"] = (
            norm_df["gam_rate"] * 0.16 * norm_df["Population"]
        ).astype("int32")
        norm_df["sam_cases"] = (
            norm_df["sam_rate"] * 0.16 * norm_df["Population"]
        ).astype("int32")

        # predict for scenario then insert prediction into 'scenario' column in norm_df
        norm_df_scn = self.input()[0].open().read()[scn_vars]
        norm_df_scn["gam_rate"] = gb_gam.predict(norm_df_scn[scn_vars[:-5]])
        norm_df_scn["sam_rate"] = gb_sam.predict(norm_df_scn[scn_vars[:-5]])

        # inverse transform the scenario variabls!
        inverse_trf_scn = scaler.inverse_transform(norm_df_scn[scn_vars[:6]])
        norm_df_scn[scn_vars[:6]] = inverse_trf_scn

        norm_df_scn["gam_cases"] = (
            norm_df_scn["gam_rate"] * 0.16 * norm_df_scn["Population"]
        ).astype("int32")
        norm_df_scn["sam_cases"] = (
            norm_df_scn["sam_rate"] * 0.16 * norm_df_scn["Population"]
        ).astype("int32")

        if self.country_level == COUNTRY_CHOICES["SS"]:
            ss_pop = retrieve_file(path_names["ss_rates_dir"], ".csv")

            census_malnutrition = malnutrition_mapping(ss_pop, norm_df)
            maln_copy = census_malnutrition.copy()

            # do this for malnutrition_mapping() to work
            norm_df_scn.rename(
                columns={"CHIRPS(mm)_lag3_scenario": "CHIRPS(mm)_lag3"}, inplace=True,
            )
            census_malnutrition_scn = malnutrition_mapping(ss_pop, norm_df_scn)
            census_malnutrition_scn.rename(
                columns={
                    "gam_cases": "gam_scenario",
                    "sam_cases": "sam_scenario",
                    "precipitation(mm)": "precipitation(mm)_scenario",
                },
                inplace=True,
            )

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            eth_pop = pd.read_csv(path_names["pop_dir"])

            # merge with eth_pop to obtain admin1 info
            typo_file = open("models/malnutrition_model/eth_typo.pkl", "rb")
            typo_dict = pickle.load(typo_file)  # noqa: S301
            typo_file.close()
            eth_pop_std = column_standardization(eth_pop, "admin2", typo_dict)
            census_malnutrition = norm_df.merge(
                eth_pop_std[["admin1", "admin2"]], on="admin2", how="left"
            )
            census_malnutrition.rename(
                columns={"CHIRPS(mm)_lag3": "precipitation(mm)"}, inplace=True
            )

            maln_copy = census_malnutrition.copy()
            census_malnutrition_scn = norm_df_scn.merge(
                eth_pop_std[["admin1", "admin2"]], on="admin2", how="left"
            )
            census_malnutrition_scn.rename(
                columns={
                    "gam_cases": "gam_scenario",
                    "sam_cases": "sam_scenario",
                    "CHIRPS(mm)_lag3_scenario": "precipitation(mm)_scenario",
                },
                inplace=True,
            )

        # insert scenario malnutrition cases and precipitation values to the main prediction dataframe
        maln_copy["gam_scenario"] = census_malnutrition_scn["gam_scenario"]
        maln_copy["sam_scenario"] = census_malnutrition_scn["sam_scenario"]
        maln_copy["gam_rate_scenario"] = census_malnutrition_scn["gam_rate"]
        maln_copy["sam_rate_scenario"] = census_malnutrition_scn["sam_rate"]
        maln_copy["precipitation(mm)_scenario"] = census_malnutrition_scn[
            "precipitation(mm)_scenario"
        ]
        maln_copy["rainfall_scenario"] = self.rainfall_scenario

        maln_copy.rename(
            columns={"gam_cases": "gam_baseline", "sam_cases": "sam_baseline"},
            inplace=True,
        )

        maln_copy.drop_duplicates(keep="first", inplace=True)

        if self.country_level == COUNTRY_CHOICES["SS"]:
            if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_1"]:
                admin1_col = "ADMIN1NAME"

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_1"]:
                admin1_col = "admin1"

        # aggregate to admin 1 level if specified by user
        if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_1"]:
            admin1_sum = (
                maln_copy.groupby(
                    by=["Month", "Year", "Time", "rainfall_scenario", admin1_col]
                )[
                    [
                        "Population",
                        "gam_baseline",
                        "sam_baseline",
                        "gam_scenario",
                        "sam_scenario",
                    ]
                ]
                .sum()
                .reset_index()
            )
            admin1_mean = (
                maln_copy.groupby(
                    by=["Month", "Year", "Time", "rainfall_scenario", admin1_col]
                )[
                    [
                        "NDVI_lag1",
                        "CPI",
                        "crop_per_capita",
                        "med_exp",
                        "gam_rate",
                        "sam_rate",
                        "gam_rate_scenario",
                        "sam_rate_scenario",
                        "precipitation(mm)",
                        "precipitation(mm)_scenario",
                    ]
                ]
                .mean()
                .reset_index()
            )
            gdf_sum_mean = admin1_sum.merge(
                admin1_mean,
                on=["Month", "Year", "Time", "rainfall_scenario", admin1_col],
                how="left",
            )

            df_output = gdf_sum_mean

        elif self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
            if self.country_level == COUNTRY_CHOICES["ETH"]:
                df_output = maln_copy.drop(
                    [
                        "Apr",
                        "Aug",
                        "Dec",
                        "Feb",
                        "Jan",
                        "Jul",
                        "Jun",
                        "Mar",
                        "May",
                        "Nov",
                        "Oct",
                        "Sep",
                    ],
                    axis=1,
                )
            else:
                df_output = maln_copy

        if self.country_level == COUNTRY_CHOICES["SS"]:
            df_output.insert(loc=0, column="Country", value="South Sudan")
            df_output = df_output.rename(
                columns={"ADMIN2NAME": "admin2", "ADMIN1NAME": "admin1"}
            )
            df_output.drop(columns=["ADMIN1PCOD", "ADMIN2PCOD"], inplace=True)
            df_output.drop(columns=["County"], inplace=True)

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            df_output.insert(loc=0, column="Country", value="Ethiopia")
            df_output.drop(
                columns=["County", "NDVI_lag1", "CPI", "med_exp"], inplace=True
            )

        with self.output().open("w") as f:
            df_output.to_csv(f, index=False)


@requires(MalnutInference, RetrvCkanData, RetrvDataEth)
class MalnutritionInferenceGeoJSON(Task):
    """
    Output MalnutInference results as a GeoJSON.These results are masked by geography and rainfall geography.
    """

    def output(self):
        try:
            return FinalTarget(
                path="malnutrition.geojson", task=self, ACL="public-read"
            )
        except TypeError:
            return FinalTarget(path="malnutrition.geojson", task=self)

    def run(self):
        df = pd.read_csv(self.input()[0].path)

        df["start"] = pd.to_datetime(df["Time"])
        df["end"] = df["start"] + pd.DateOffset(months=1)
        df["start"] = df["start"].astype(str)
        df["end"] = df["end"].astype(str)

        if self.country_level == COUNTRY_CHOICES["SS"]:
            df.rename(
                columns={"admin2": "ADMIN2NAME", "admin1": "ADMIN1NAME"}, inplace=True
            )

        if self.country_level == COUNTRY_CHOICES["SS"]:
            with self.input()[1].open("r") as src:
                geo_dir = src.read()["county_dir"]

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            with self.input()[2].open("r") as src:
                geo_dir = src.read()["county_dir"]

        shp_file = [x for x in os.listdir(geo_dir) if x.endswith(".shp")][0]
        shp_file = os.path.join(geo_dir, shp_file)
        gdf = gpd.read_file(shp_file)

        # Calculate gam and sam number
        df["gam_number"] = df["gam_baseline"]
        df["sam_number"] = df["sam_baseline"]

        # Merge the data with the shapefile
        if self.country_level == COUNTRY_CHOICES["SS"]:
            if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
                data = df[
                    [
                        "gam_rate",
                        "sam_rate",
                        "gam_number",
                        "sam_number",
                        "gam_scenario",
                        "sam_scenario",
                        "gam_rate_scenario",
                        "sam_rate_scenario",
                        "ADMIN1NAME",
                        "ADMIN2NAME",
                        "Month",
                        "Year",
                        "start",
                        "end",
                        "precipitation(mm)",
                        "precipitation(mm)_scenario",
                        "rainfall_scenario",
                    ]
                ].merge(
                    gdf[["ADMIN2NAME", "geometry"]],
                    left_on="ADMIN2NAME",
                    right_on="ADMIN2NAME",
                )
            elif self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_1"]:
                gdf_admin1 = (
                    gdf[["ADMIN1NAME", "geometry"]]
                    .dissolve(by="ADMIN1NAME")
                    .reset_index()
                )
                data = df[
                    [
                        "gam_rate",
                        "sam_rate",
                        "gam_number",
                        "sam_number",
                        "gam_scenario",
                        "sam_scenario",
                        "gam_rate_scenario",
                        "sam_rate_scenario",
                        "ADMIN1NAME",
                        "Month",
                        "Year",
                        "start",
                        "end",
                        "precipitation(mm)",
                        "precipitation(mm)_scenario",
                        "rainfall_scenario",
                    ]
                ].merge(
                    gdf_admin1[["ADMIN1NAME", "geometry"]],
                    left_on="ADMIN1NAME",
                    right_on="ADMIN1NAME",
                )

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            typo_file = open("models/malnutrition_model/eth_typo.pkl", "rb")
            typo_dict = pickle.load(typo_file)  # noqa: S301
            typo_file.close()
            gdf_std = column_standardization(gdf, "admin2", typo_dict)
            # gdf_std might contain mixed geometry type
            gdf_std_explode = (
                gdf_std.explode().reset_index().rename(columns={0: "geometry"})
            )
            gdf_std_explode.drop(columns=["level_0", "level_1"], inplace=True)
            if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
                data = df[
                    [
                        "gam_rate",
                        "sam_rate",
                        "gam_number",
                        "sam_number",
                        "gam_scenario",
                        "sam_scenario",
                        "gam_rate_scenario",
                        "sam_rate_scenario",
                        "admin1",
                        "admin2",
                        "Month",
                        "Year",
                        "start",
                        "end",
                        "precipitation(mm)",
                        "precipitation(mm)_scenario",
                        "rainfall_scenario",
                    ]
                ].merge(
                    gdf_std_explode[["admin2", "geometry"]],
                    left_on="admin2",
                    right_on="admin2",
                )
                data.drop_duplicates(
                    subset=[
                        "Month",
                        "Year",
                        "start",
                        "end",
                        "admin1",
                        "admin2",
                        "gam_number",
                    ],
                    keep="first",
                    inplace=True,
                )
            elif self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_1"]:
                gdf_admin1 = (
                    gdf_std_explode[["admin1", "geometry"]]
                    .dissolve(by="admin1")
                    .reset_index()
                )

                data = df[
                    [
                        "gam_rate",
                        "sam_rate",
                        "gam_number",
                        "sam_number",
                        "gam_scenario",
                        "sam_scenario",
                        "gam_rate_scenario",
                        "sam_rate_scenario",
                        "admin1",
                        "Month",
                        "Year",
                        "start",
                        "end",
                        "precipitation(mm)",
                        "precipitation(mm)_scenario",
                        "rainfall_scenario",
                    ]
                ].merge(
                    gdf_admin1[["admin1", "geometry"]],
                    left_on="admin1",
                    right_on="admin1",
                )
                data.drop_duplicates(
                    subset=["Month", "Year", "start", "end", "admin1", "gam_number"],
                    keep="first",
                    inplace=True,
                )
                data = data.reset_index(drop=True)

        data = gpd.GeoDataFrame(data)

        with self.output().open("w") as out:
            data.to_file(out.name, driver="GeoJSON")


@requires(MalnutInference, RetrvCkanData, RetrvDataEth)
class MalnutritionRaster(Task):
    """Rasterize  malnutriton cases for each county using
    reference projection json file for the relevant country
    NOTE: This is supported for only admin2 level.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
            census_malnutrition = pd.read_csv(self.input()[0].path)

            if self.country_level == COUNTRY_CHOICES["SS"]:
                ref_proj = REF_PROJ_CONFIG_SS
                ncols, nrows = ref_proj["ncolsrows"]
                if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
                    # merge_cols = ["ADMIN1PCOD", "ADMIN2PCOD"]
                    merge_cols = ["admin1", "admin2"]

                    with self.input()[1].open("r") as src:
                        geo_dir = src.read()["county_dir"]
                        shp_file = [
                            x for x in os.listdir(geo_dir) if x.endswith(".shp")
                        ][0]
                        shp_file = os.path.join(geo_dir, shp_file)
                        shp_data = gpd.read_file(shp_file)

                        # These columns were rename and don't exist
                        # census_malnutrition = census_malnutrition.drop(
                        #    ["ADMIN2NAME", "ADMIN1NAME"], axis=1
                        # )

            elif self.country_level == COUNTRY_CHOICES["ETH"]:
                ref_proj = REF_PROJ_CONFIG_ETH
                ncols, nrows = ref_proj["ncolsrows"]
                if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
                    merge_cols = ["admin1", "admin2"]
                    with self.input()[2].open("r") as src:
                        geo_dir = src.read()["county_dir"]
                        shp_file = [
                            x for x in os.listdir(geo_dir) if x.endswith(".shp")
                        ][0]
                        shp_file = os.path.join(geo_dir, shp_file)
                        shp_input = gpd.read_file(shp_file)

                        typo_file = open("models/malnutrition_model/eth_typo.pkl", "rb")
                        typo_dict = pickle.load(typo_file)  # noqa: S301
                        typo_file.close()
                        shp_data_std = column_standardization(
                            shp_input, "admin2", typo_dict
                        )
                        # gdf_std might contain mixed geometry type
                        shp_data = (
                            shp_data_std.explode()
                            .reset_index()
                            .rename(columns={0: "geometry"})
                        )
                        select_cols = [
                            col for col in shp_data.columns if col not in ["geometry"]
                        ]
                        shp_data.drop_duplicates(subset=select_cols, inplace=True)

            # loop through the unique values of year and month in order to generate raster for each
            m_unique = census_malnutrition["Month"].unique().tolist()
            y_unique = census_malnutrition["Year"].unique().tolist()
            with self.output().temporary_path() as tmpdir:
                os.makedirs(tmpdir)
                for y_val in y_unique:
                    for month_val in m_unique:
                        select_pred = census_malnutrition.loc[
                            (census_malnutrition["Month"] == month_val)
                            & (census_malnutrition["Year"] == y_val)
                        ]

                        shp_data.rename(
                            columns={"ADMIN2NAME": "admin2", "ADMIN1NAME": "admin1"},
                            inplace=True,
                        )

                        select_census_malnutrition = shp_data.merge(
                            select_pred, on=merge_cols, how="inner"
                        )

                        rast_meta = {
                            "driver": "GTiff",
                            "height": nrows,
                            "width": ncols,
                            "count": 4,
                            "dtype": rasterio.int32,
                            "crs": ref_proj["srs"],
                            "transform": ref_proj["transform"],
                            "nodata": -9999,
                        }

                        # create file path for writing rasters
                        raster_outname = os.path.join(
                            tmpdir,
                            f"{self.country_level}_{y_val}_{month_val}_malnut.tiff",
                        )

                        with rasterio.open(
                            raster_outname, "w", **rast_meta
                        ) as out_raster:

                            out_raster.update_tags(
                                Time=select_census_malnutrition["Time"][0]
                            )
                            shapes_gam = (
                                (geom, gam_values)
                                for geom, gam_values in zip(
                                    select_census_malnutrition["geometry"],
                                    select_census_malnutrition["gam_baseline"],
                                )
                            )
                            shapes_gam_scn = (
                                (geom, sam_values)
                                for geom, sam_values in zip(
                                    select_census_malnutrition["geometry"],
                                    select_census_malnutrition["gam_scenario"],
                                )
                            )
                            shapes_sam = (
                                (geom, gam_values)
                                for geom, gam_values in zip(
                                    select_census_malnutrition["geometry"],
                                    select_census_malnutrition["sam_baseline"],
                                )
                            )
                            shapes_sam_scn = (
                                (geom, sam_values)
                                for geom, sam_values in zip(
                                    select_census_malnutrition["geometry"],
                                    select_census_malnutrition["sam_scenario"],
                                )
                            )

                            burned_data_gam = features.rasterize(
                                shapes=shapes_gam,
                                fill=0,
                                out_shape=out_raster.shape,
                                transform=out_raster.transform,
                            )

                            burned_data_sam = features.rasterize(
                                shapes=shapes_sam,
                                fill=0,
                                out_shape=out_raster.shape,
                                transform=out_raster.transform,
                            )
                            burned_data_gam_scn = features.rasterize(
                                shapes=shapes_gam_scn,
                                fill=0,
                                out_shape=out_raster.shape,
                                transform=out_raster.transform,
                            )

                            burned_data_sam_scn = features.rasterize(
                                shapes=shapes_sam_scn,
                                fill=0,
                                out_shape=out_raster.shape,
                                transform=out_raster.transform,
                            )

                            out_raster.write_band(1, burned_data_gam.astype(np.int32))
                            out_raster.write_band(
                                2, burned_data_gam_scn.astype(np.int32)
                            )
                            out_raster.write_band(3, burned_data_sam.astype(np.int32))
                            out_raster.write_band(
                                4, burned_data_sam_scn.astype(np.int32)
                            )

                            # Tag raster bands
                            band_tags = {
                                "band_1": "gam_cases_baseline",
                                "band_2": "gam_cases_scenario",
                                "band_3": "sam_cases_baseline",
                                "band_4": "sam_cases_scenario",
                            }
                            out_raster.update_tags(**band_tags)
        else:
            return


@requires(MalnutritionRaster, RetrvCkanData, RetrvDataEth)
class HighResMalnRaster(Task):
    """
    Distribute admin-level malnutrition estimates (GAM - band1) to a higher resolution
    grid (1km^2 grid) using county population density grid.
    NOTE: This is supported for only admin2 level.
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=31536000)

    def run(self):
        if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
            # Open and read raster file with population estimates at admin level
            raster_ls = os.listdir(self.input()[0].path)
            with self.output().temporary_path() as tmpdir:
                os.makedirs(tmpdir)

                for raster_file in raster_ls:
                    raster_fname = os.path.join(self.input()[0].path, raster_file)
                    split_name = raster_fname.split("/")[-1].split("_")

                    with rasterio.open(raster_fname) as pop_estimate_raster:
                        time_str = pop_estimate_raster.tags()["Time"]
                        pop_estimate_baseline = pop_estimate_raster.read(
                            1
                        )  # band 1 is GAM baseline
                        pop_estimate_scn = pop_estimate_raster.read(
                            2
                        )  # band 2 is for GAM scenario
                        nodata_pop_est = pop_estimate_raster.nodata

                        # Open and read the appropriate raster file with spatial population distribution, get first band
                    if self.country_level == COUNTRY_CHOICES["SS"]:
                        pop_den_fname = self.input()[1].open().read()["pop_density_dir"]
                    elif self.country_level == COUNTRY_CHOICES["ETH"]:
                        pop_den_fname = self.input()[2].open().read()["pop_density_dir"]

                    with rasterio.open(pop_den_fname) as pop_dist_raster:
                        pop_dist_array = pop_dist_raster.read(1)
                        nodata_pop_dist = pop_dist_raster.nodata
                        profile = pop_dist_raster.profile

                    pop_estimate_baseline = np.squeeze(pop_estimate_baseline)
                    pop_dist_array = np.squeeze(pop_dist_array)
                    pop_est_bl = np.ones(pop_dist_array.shape) * -9999
                    ind1_bl = np.where(
                        pop_estimate_baseline.flatten() != nodata_pop_est
                    )[0]
                    ind2_bl = np.where(pop_dist_array.flatten() != nodata_pop_dist)[0]
                    ind_bl = np.intersect1d(ind1_bl, ind2_bl)
                    ind2d_bl = np.unravel_index(ind_bl, pop_dist_array.shape)
                    # take county(admin2) malnutrition cases X the county population percentage
                    pop_est_bl[ind2d_bl] = (
                        pop_estimate_baseline[ind2d_bl] * pop_dist_array[ind2d_bl]
                    )
                    pop_est_bl[ind2d_bl] = np.round(pop_est_bl[ind2d_bl])

                    # repeat for scenario array, burn this array into second band of the output raster
                    pop_estimate_scn = np.squeeze(pop_estimate_scn)
                    pop_est_scn = np.ones(pop_dist_array.shape) * -9999
                    ind1_scn = np.where(pop_estimate_scn.flatten() != nodata_pop_est)[0]
                    ind2_scn = np.where(pop_dist_array.flatten() != nodata_pop_dist)[0]
                    ind_scn = np.intersect1d(ind1_scn, ind2_scn)
                    ind2d_scn = np.unravel_index(ind_scn, pop_dist_array.shape)
                    pop_est_scn[ind2d_scn] = (
                        pop_estimate_scn[ind2d_scn] * pop_dist_array[ind2d_scn]
                    )
                    pop_est_scn[ind2d_scn] = np.round(pop_est_scn[ind2d_scn])

                    # Update raster meta-data
                    profile.update(nodata=-9999, count=2)

                    hires_raster_outname = os.path.join(
                        tmpdir,
                        f"{split_name[0]}_{split_name[1]}_{split_name[2]}_hires_malnut.tiff",
                    )
                    with rasterio.open(
                        hires_raster_outname, "w", **profile
                    ) as out_raster:
                        out_raster.update_tags(Time=time_str)
                        out_raster.write(pop_est_bl.astype(rasterio.float32), 1)
                        out_raster.write(pop_est_scn.astype(rasterio.float32), 2)
        else:
            return


@requires(HighResMalnRaster)
class HiResRasterMasked(Task):
    """
    This applies masking to hires(hr) raster using geography json file from UI front end.
    The geography GeoParameter is inherited from HighResMalnRaster
    NOTE: This is supported for only admin2 level.
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", task=self, timeout=31536000)

    def run(self):
        if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:
            hr_raster_ls = os.listdir(self.input().path)
            if self.country_level == COUNTRY_CHOICES["SS"]:
                ref_proj = REF_PROJ_CONFIG_SS
            elif self.country_level == COUNTRY_CHOICES["ETH"]:
                ref_proj = REF_PROJ_CONFIG_ETH

            with self.output().temporary_path() as tmpdir:
                os.makedirs(tmpdir)
                for hr_raster_file in hr_raster_ls:
                    hr_raster_fname = os.path.join(self.input().path, hr_raster_file)
                    split_name = hr_raster_fname.split("/")[-1].split("_")

                    with rasterio.open(hr_raster_fname) as src:
                        time_str = src.tags()["Time"]
                        geography = geography_f_country(self.country_level)
                        try:
                            geo_mask = geography["features"][0]["geometry"]
                        except KeyError:
                            geo_mask = geography
                        masked_image, masked_transform = mask(
                            src, [geo_mask], crop=True
                        )
                        out_meta = src.meta.copy()
                        out_meta.update(
                            {
                                "driver": "GTiff",
                                "crs": ref_proj["srs"],
                                "height": masked_image.shape[1],
                                "width": masked_image.shape[2],
                                "transform": masked_transform,
                                "dtype": rasterio.float32,
                                "nodata": -9999,
                            }
                        )
                    hr_raster_maskname = os.path.join(
                        tmpdir,
                        f"{split_name[0]}_{split_name[1]}_{split_name[2]}_hires_masked_malnut.tiff",
                    )
                    with rasterio.open(hr_raster_maskname, "w", **out_meta) as dest:
                        dest.update_tags(Time=time_str)
                        dest.write(masked_image.astype(rasterio.float32))
        else:
            return


@requires(HiResRasterMasked)
class RasterToGeojson(Task):
    """
    Convert the 1km^2 raster (band_1= baseline, band_2=scenario simulation)
    to a geojson file for each hi-resolution raster file. This results in big output files
    and might not be a scalable solution.
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=31536000, task=self)

    def run(self):
        if self.admin_level == ADMIN_LEVEL_CHOICES["ADMIN_2"]:

            masked_raster_ls = os.listdir(self.input().path)
            with self.output().temporary_path() as tmpdir:
                os.makedirs(tmpdir)
                for masked_raster in masked_raster_ls:
                    masked_raster_fname = os.path.join(self.input().path, masked_raster)
                    split_name = masked_raster_fname.split("/")[-1].split("_")
                    with rasterio.open(masked_raster_fname) as maln_est_raster:
                        mask = maln_est_raster.dataset_mask()
                        maln_est_arr_bl = maln_est_raster.read(1)
                        maln_est_arr_scn = maln_est_raster.read(2)

                        results_bl = (
                            {"properties": {"GAM_cases_baseline": v}, "geometry": s}
                            for i, (s, v) in enumerate(
                                shapes(
                                    maln_est_arr_bl,
                                    mask=mask,
                                    transform=maln_est_raster.transform,
                                )
                            )
                        )
                        results_scn = (
                            {"properties": {"GAM cases scenario": y}, "geometry": s}
                            for i, (s, y) in enumerate(
                                shapes(
                                    maln_est_arr_scn,
                                    mask=mask,
                                    transform=maln_est_raster.transform,
                                )
                            )
                        )

                    gpd_poly_bl = gpd.GeoDataFrame.from_features(list(results_bl))
                    gpd_poly_scn = gpd.GeoDataFrame.from_features(list(results_scn))

                    gpd_poly_bl["GAM_cases_scn"] = gpd_poly_scn["GAM cases scenario"]
                    gpd_poly_bl["year"] = int(split_name[1])
                    gpd_poly_bl["month"] = split_name[2]
                    out_fname = os.path.join(
                        tmpdir,
                        f"{split_name[0]}_{split_name[1]}_{split_name[2]}_hires_masked_malnut.geojson",
                    )
                    gpd_poly_bl.to_file(out_fname, driver="GeoJSON")
        else:
            return
