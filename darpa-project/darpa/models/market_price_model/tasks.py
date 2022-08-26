import os
from filecmp import dircmp

import geopandas as gpd
import luigi
import numpy as np

# import datetime
import pandas as pd
import rasterio
import rasterio.features
import rasterio.warp
import statsmodels.api as sm

# from kiluigi.parameter import GeoParameter
from kiluigi.targets.ckan import CkanTarget
from kiluigi.targets.delegating import FinalTarget, IntermediateTarget
from kiluigi.tasks import ExternalTask, Task
from linearmodels import PanelOLS
from luigi.configuration import get_config
from luigi.util import requires
from rasterio import features
from rasterio.mask import mask
from shapely.geometry import Point

from models.market_price_model.data_cleaning import (
    ExtractBorderShapefile,
    WriteCleanedPriceModelData,
)
from models.market_price_model.mappings import commodity_group_map

# from affine import Affine
from models.market_price_model.utils import (
    datetime_schema,
    merge_independent_vars,
    merge_with_shapefile,
    prepare_model_mats,
)
from utils.geospatial_tasks.functions.geospatial import (
    buffer_points,
    geography_f_country,
)
from utils.scenario_tasks.functions.geography import MaskDataToGeography

# from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters
from models.market_price_model.ScenarioDefinition_mpm import GlobalParametersMPM
from models.market_price_model.Country_Choice import CountryParameters


# from utils.geospatial_tasks.functions.remote_sensing_functs import calc_cluster_data
from utils.scenario_tasks.functions.time import MaskDataToTime

CONFIG = get_config()

COUNTRY_CHOICES = {
    "SS": "South Sudan",
    "ETH": "Ethiopia",
    "DJ": "Djibouti",
    "UG": "Uganda",
    "KE": "Kenya",
    "SD": "Sudan",
    "SO": "Somalia",
    "ER": "Eritrea",
}


class ReferenceRaster(ExternalTask):
    """
    Pull raster of consumption expenditure. Will eventually come from upstream household economic model.
    """

    def output(self):
        return {
            "ss_raster": CkanTarget(
                dataset={"id": "8399cfb4-fb5f-4e71-93a9-db77ce3b74a4"},
                resource={"id": "4c3c22f4-0c6e-4034-bee2-81c89d754dd5"},
            ),
            "eth_raster": CkanTarget(
                dataset={"id": "263ca202-b1af-4ebe-825d-da20624695b2"},
                resource={"id": "673e9fe0-7e2f-4572-967e-979ac81a17be"},
            ),
        }


class RainFallScenarioPath(ExternalTask):
    """
    Pulls rainfall scenarios from CKAN
    """

    def output(self):
        return {
            "ss_rainfall": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "82c51f64-795a-4104-8e16-53d2dc1f3907"},
            ),
            "eth_rainfall": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "3bbcafcb-6f6f-4678-9915-48f1081c3fcb"},
            ),
            "ug_rainfall": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "a8512e80-48d8-4b47-9a57-8ee87180dd37"},
            ),
            "ke_rainfall": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "ddc89718-00fd-456b-98f4-86af4d098d56"},
            ),
            "sd_rainfall": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "4f934fe2-8df1-48ff-8724-2b8ed5cd8776"},
            ),
            "so_rainfall": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "5207959e-f425-4585-85be-f6c784dc613c"},
            ),
        }


class MarketAccessibilityPath(ExternalTask):
    "Path for Travel time raster and market locations"

    def output(self):
        return {
            # Accessibility raster overlayed with popultion raster: in Ahmad folder
            "travel_time_pop_raster": CkanTarget(
                dataset={"id": "d6561514-02d4-4acc-8b8b-82794d3de58e"},
                resource={"id": "99808ec0-c9fa-46d7-aebd-eb2396588f30"},
            ),
            "market_location": CkanTarget(
                dataset={"id": "0ba1f319-1132-405a-826c-6ce70906f1f3"},
                resource={"id": "21a2b36c-c701-45ff-89af-7e60afc76241"},
            ),
            "market_location_json": CkanTarget(
                dataset={"id": "2869de83-7035-4e28-83f1-cf07ef0227e5"},
                resource={"id": "07536938-7632-442c-81cb-e4bacd036f38"},
            ),
            "travel_time": CkanTarget(
                dataset={"id": "2869de83-7035-4e28-83f1-cf07ef0227e5"},
                resource={"id": "c218686e-6c74-4f61-b7cb-1305782fcd34"},
            ),
            "market_id_raster": CkanTarget(
                dataset={"id": "2869de83-7035-4e28-83f1-cf07ef0227e5"},
                resource={"id": "9a21c26c-15c3-4b6f-b528-e8a7423d5a44"},
            ),
        }


# Format of date
@requires(WriteCleanedPriceModelData)
class MaskPriceModelDataToTime(GlobalParametersMPM, MaskDataToTime):
    def complete(self):
        return super(MaskDataToTime, self).complete()


# Parameter
@requires(MaskPriceModelDataToTime)
class MaskPriceModelDataToGeography(GlobalParametersMPM, MaskDataToGeography):
    def complete(self):
        return super(MaskDataToGeography, self).complete()


@requires(WriteCleanedPriceModelData, MaskPriceModelDataToGeography)
class MergeData(Task):
    def gdf_from_dir(self, parent_dir, ind_var_files, dep_var_files):
        iv_gdf = merge_independent_vars(parent_dir, ind_var_files)
        commodity_dfs = {}
        for d in dep_var_files:
            print(d)
            # Some commodity data doesn't get written if there are no data for the given timeframe
            if os.path.exists(os.path.join(parent_dir, d)):
                dv_gdf = gpd.read_file(os.path.join(parent_dir, d))
                merged = dv_gdf.merge(iv_gdf, on=["Geography", "Time"])
                # Adjusting for empty dataframes
                if merged.empty is True:
                    print(f"{d} is empty, moving on ...")
                    pass
                else:
                    print(f"Commodity {d} successfully written")
                    commodity_dfs[d.split(".")[0]] = pd.DataFrame(merged)
            else:
                print(f"The file for {d} does not exist, moving on ...")
        return commodity_dfs

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 7 * 100)

    def run(self):
        input_dir_train = self.input()[0].path
        input_dir_test = self.input()[1].path

        assert dircmp(
            input_dir_train, input_dir_test
        ), "Train and test directories do not contain same variable files"

        files = os.listdir(input_dir_train)

        if self.country_level == COUNTRY_CHOICES["SS"]:
            dvs = [f for f in files if f.startswith("food")]
            ivs = [f for f in files if not f.startswith("food")]

        elif self.country_level in [
            COUNTRY_CHOICES["KE"],
            COUNTRY_CHOICES["UG"],
            COUNTRY_CHOICES["SD"],
            COUNTRY_CHOICES["SO"],
            COUNTRY_CHOICES["ER"],
        ]:
            dvs = [f for f in files if f.startswith("food")]
            ivs = [f for f in files if not f.startswith("food")]

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            dvs = [
                f
                for f in files
                if (f.startswith("food") and not f.startswith("food_vegetables"))
            ]
            ivs = [
                f
                for f in files
                if (not f.startswith("food") and not f.startswith("Crop"))
            ]
        else:
            raise NotImplementedError

        train_dfs = self.gdf_from_dir(input_dir_train, ivs, dvs)
        test_dfs = self.gdf_from_dir(input_dir_test, ivs, dvs)

        dfs = {"train": train_dfs, "test": test_dfs}
        with self.output().open("w") as dst:
            dst.write(dfs)


@requires(MergeData, GlobalParametersMPM, CountryParameters)
class PrepareTrainData(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 365)

    def run(self):

        if self.country_level == COUNTRY_CHOICES["ETH"]:
            self.train_vars = ["fatalities", "rainfall", "p_diesel"]

        elif self.country_level == COUNTRY_CHOICES["SS"]:
            self.train_vars = ["fatalities", "rainfall", "p_petrol"]

        elif self.country_level in [
            COUNTRY_CHOICES["KE"],
            COUNTRY_CHOICES["UG"],
            COUNTRY_CHOICES["SD"],
            COUNTRY_CHOICES["SO"],
            COUNTRY_CHOICES["ER"],
        ]:
            self.train_vars = ["fatalities", "rainfall", "p_Fuel - Crude Oil"]

        else:
            raise NotImplementedError

        with self.input()[0].open("r") as src:
            commodity_dfs = src.read()["train"]

        train_data = prepare_model_mats(commodity_dfs, self.train_vars)

        with self.output().open("w") as dst:
            dst.write(train_data)


@requires(PrepareTrainData, GlobalParametersMPM, CountryParameters)
class TrainPriceModel(Task):
    """
    Trains PanelOLS model for each commodity
    """

    def train(self, model_mat, dep_var, ind_vars):
        exog = sm.add_constant(model_mat[ind_vars])
        try:
            mod = PanelOLS(model_mat[dep_var], exog, entity_effects=True)
            res = mod.fit().model
            return res
        except ValueError:
            pass

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):

        if self.country_level == COUNTRY_CHOICES["ETH"]:
            self.train_vars = ["fatalities", "rainfall", "p_diesel"]

        elif self.country_level == COUNTRY_CHOICES["SS"]:
            self.train_vars = ["fatalities", "rainfall", "p_petrol"]

        elif self.country_level in [
            COUNTRY_CHOICES["KE"],
            COUNTRY_CHOICES["UG"],
            COUNTRY_CHOICES["SD"],
            COUNTRY_CHOICES["SO"],
            COUNTRY_CHOICES["ER"],
        ]:
            self.train_vars = ["fatalities", "rainfall", "p_Fuel - Crude Oil"]

        else:
            raise NotImplementedError

        with self.input()[0].open("r") as src:
            train_data = src.read()

        models = {}
        for k, v in train_data.items():
            dv = f"p_{k}"
            ivs = list(self.train_vars) + [f"p_{k}_t-1"]
            res = self.train(v, dep_var=dv, ind_vars=ivs)
            models[k] = res
        with self.output().open("w") as dst:
            dst.write(models)


@requires(
    TrainPriceModel,
    MergeData,
    GlobalParametersMPM,
    RainFallScenarioPath,
    CountryParameters,
)
class PredictPrices(Task):
    """
    Generates predictions of commodity prices at particular time and geography from trained models
    """

    # PercentOfNormalRainfall = luigi.FloatParameter(default=1.0)

    def test(self, mod, data, ind_vars):
        exog = data[ind_vars]
        exog.insert(0, "const", 1)
        res = mod.fit()
        preds = res.predict(exog=exog, effects=True)
        preds = preds.reset_index()
        preds = preds.drop_duplicates(
            subset=["Geography", "Time"], keep="first"
        ).reset_index()
        preds = preds.set_index(["Geography", "Time"])
        preds["geometry"] = data["geometry"]

        return preds

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 21)

    def run(self):

        if self.country_level == COUNTRY_CHOICES["ETH"]:
            self.train_vars = ["fatalities", "rainfall", "p_diesel"]
            rain_eth_name = self.input()[3]["eth_rainfall"]
            rain_scenario_df = pd.read_csv(rain_eth_name.path)
            rain_scenario_df = rain_scenario_df[
                ["admin1", "admin2", "month", "rainfall", "scenario"]
            ]
            rain_scenario_df = rain_scenario_df.rename(columns={"admin2": "Geography"})

        elif self.country_level == COUNTRY_CHOICES["SS"]:
            self.train_vars = ["fatalities", "rainfall", "p_petrol"]
            rain_ss_name = self.input()[3]["ss_rainfall"]
            rain_scenario_df = pd.read_csv(rain_ss_name.path)
            rain_scenario_df = rain_scenario_df.rename(columns={"County": "Geography"})

        elif self.country_level == COUNTRY_CHOICES["UG"]:
            self.train_vars = ["fatalities", "rainfall", "p_Fuel - Crude Oil"]
            rain_ug_name = self.input()[3]["ug_rainfall"]
            rain_scenario_df = pd.read_csv(rain_ug_name.path)
            rain_scenario_df = rain_scenario_df.rename(columns={"ADM2_EN": "Geography"})

        elif self.country_level == COUNTRY_CHOICES["KE"]:
            self.train_vars = ["fatalities", "rainfall", "p_Fuel - Crude Oil"]
            rain_ke_name = self.input()[3]["ke_rainfall"]
            rain_scenario_df = pd.read_csv(rain_ke_name.path)
            rain_scenario_df = rain_scenario_df.rename(
                columns={"FIRST_DIST": "Geography"}
            )

        elif self.country_level == COUNTRY_CHOICES["SD"]:
            self.train_vars = ["fatalities", "rainfall", "p_Fuel - Crude Oil"]
            rain_sd_name = self.input()[3]["sd_rainfall"]
            rain_scenario_df = pd.read_csv(rain_sd_name.path)
            rain_scenario_df = rain_scenario_df.rename(columns={"County": "Geography"})

        elif self.country_level == COUNTRY_CHOICES["SO"]:
            self.train_vars = ["fatalities", "rainfall", "p_Fuel - Crude Oil"]
            rain_so_name = self.input()[3]["so_rainfall"]
            rain_scenario_df = pd.read_csv(rain_so_name.path)
            rain_scenario_df = rain_scenario_df.rename(columns={"NAME_2": "Geography"})

        else:
            raise NotImplementedError

        with self.input()[0].open("r") as src:
            models = src.read()

        with self.input()[1].open("r") as src:
            test_data = src.read()["test"]

        test_data = prepare_model_mats(test_data, self.train_vars)

        pred_dict = {}
        for k, v in models.items():
            mod = v
            if mod is None:
                print(f"Passing {k}")
                pass
            else:
                print(f"Writing {k}")
                data = test_data[k]
                data = data.reset_index()
                data = data.drop_duplicates(
                    subset=["Geography", "Time"], keep="first"
                ).reset_index()
                data = data.set_index(["Geography"])
                data["month"] = pd.DatetimeIndex(data["Time"]).month

                if self.rainfall_scenario == "normal":
                    data["rainfall"] = data["rainfall"]
                    data = data.reset_index()
                    data = data.set_index(["Geography", "Time"])

                else:
                    scn_name = self.rainfall_scenario + "_rainfall"

                    rain_scenario_df = rain_scenario_df[
                        rain_scenario_df["scenario"] == scn_name
                    ]
                    rain_scenario_df = rain_scenario_df.drop_duplicates(
                        subset=["Geography", "month"], keep="first"
                    )
                    merged_rainfall = data.merge(
                        rain_scenario_df, on=["Geography", "month"]
                    )
                    merged_rainfall = merged_rainfall.set_index(["Geography", "Time"])
                    merged_rainfall = merged_rainfall.drop(
                        columns=["index", "rainfall_x"]
                    )
                    merged_rainfall = merged_rainfall.rename(
                        columns={"rainfall_y": "rainfall"}
                    )

                    data = merged_rainfall
                    data = data.reset_index()
                    data = data.set_index(["Geography", "Time"])

                ivs = list(self.train_vars) + [f"p_{k}_t-1"]
                preds = self.test(mod, data, ind_vars=ivs)

                if self.rainfall_scenario == "normal":
                    preds["rainfall_scenario"] = "Normal"
                    preds["rainfall"] = data["rainfall"]

                else:
                    scn_name = self.rainfall_scenario
                    preds["rainfall_scenario"] = scn_name
                    preds["rainfall"] = data["rainfall"]
                pred_dict[k] = preds

        pred_df = pd.DataFrame()
        for commodity, pred in pred_dict.items():
            pred["commodity"] = commodity
            pred_df = pred_df.append(pred)

        fname = f"{self.country_level}_price_predictions.csv"
        outpath = os.path.join(CONFIG.get("core", "cache_dir"), fname)
        pred_df.drop(columns=["geometry"], inplace=True)
        pred_df.to_csv(outpath)
        target = CkanTarget(
            dataset={"id": "841f5d52-04cc-4fbc-aee6-57864d8b9e63"},
            resource={"name": fname},
        )
        if target.exists():
            target.remove()
        target.put(outpath)
        with self.output().open("w") as dst:
            dst.write(pred_dict)


class RunPriceModelScenarios(Task):
    """
    Iterate over different values of scenario parameters and combine the results into a single data frame.
    This is primarily for the comparison of scenarios in Superset, which can only use one data source per chart.
    """

    ScenarioValues = luigi.DictParameter(
        default={"PercentOfNormalRainfall": [0.5, 0.75, 1, 1.25, 1.5]}
    )

    def requires(self):
        scenarios = []
        for param, val_list in self.ScenarioValues.items():
            for val in val_list:
                scenarios.append(PredictPrices(**{param: val}))
        return scenarios

    def output(self):
        return FinalTarget("scenario_runs.csv", task=self)

    def run(self):
        scenario_dfs = pd.DataFrame()
        for task in self.requires():
            with task.output().open("r") as src:
                pred_dict = src.read()
            pred_df = pd.DataFrame()
            for commodity, pred in pred_dict.items():
                pred["commodity"] = commodity
                pred_df = pred_df.append(pred)
            df = pred_df.assign(
                **{
                    f"{k}": f"{v}x normal rainfall"
                    for k, v in task.param_kwargs.items()
                    if k in self.ScenarioValues.keys()
                }
            )
            scenario_dfs = scenario_dfs.append(df)

        with self.output().open("w") as dst:
            scenario_dfs.to_csv(dst)


@requires(PredictPrices, ExtractBorderShapefile)
class GroupCommodities(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 365)

    def run(self):
        with self.input()[0].open("r") as f:
            pred_dict = f.read()

        with self.input()[1].open("r") as f:
            adminx_boundaries = f.read()

        group_prices = {}
        group_count = {}
        for k, v in pred_dict.items():
            if isinstance(v, dict):
                v = pd.DataFrame(v)
            index = v.index
            group = commodity_group_map[k.split("_")[1]]
            if group not in group_prices:
                group_prices[group] = v
                group_count[group] = 1
            else:
                group_prices[group]["predictions"] = group_prices[group][
                    "predictions"
                ].add(v["predictions"])
                group_count[group] += 1
        group_price_df = pd.DataFrame(index=index, columns=group_prices.keys())

        for k, v in group_prices.items():
            group_price_df[k] = v["predictions"].divide(group_count[k])
            if "geometry" not in group_price_df.columns:
                group_price_df["geometry"] = v["geometry"]
                group_price_df["rainfall"] = v["rainfall"]
                group_price_df["rainfall_scenario"] = v["rainfall_scenario"]

        group_price_df = group_price_df.reset_index()
        adminx_boundaries = adminx_boundaries.rename(columns={"adminx": "Geography"})
        adminx_boundaries["Geography"] = adminx_boundaries["Geography"].apply(
            lambda x: str(x).title()
        )

        merged_group_price_df = group_price_df.merge(adminx_boundaries, on="Geography")
        merged_group_price_df = merged_group_price_df.set_index(["Geography", "Time"])
        merged_group_price_df = merged_group_price_df.drop(columns=["geometry_x"])
        merged_group_price_df = merged_group_price_df.rename(
            columns={"geometry_y": "geometry"}
        )
        merged_group_price_df = merged_group_price_df.rename(
            columns={"geometry_y": "geometry"}
        )
        with self.output().open("w") as output:
            output.write(merged_group_price_df)


@requires(GroupCommodities)
class PriceToNetcdf(Task):
    def output(self):
        return FinalTarget(path="model_prices/model_prices.nc", task=self)

    def run(self):
        with self.input().open("r") as f:
            pred_dict = f.read()

        pred_dict = pred_dict.reset_index()
        pred_dict.drop()
        pred_dict["Country"] = self.country_level
        pred_dict = pred_dict.set_index(["Country", "Geography", "Time"])
        pred_dict = pred_dict.drop("geometry", 1)

        # TODO: Find why we have duplicated index
        pred_dict = pred_dict.loc[~pred_dict.index.duplicated()]

        # CHANGING TO DYNAMIC LOGIC BECAUSE NOT ALL COUNTRIES HAVE COMMODITIES FROM ALL CATEGORIES
        pred_dict = pred_dict.to_xarray()
        pred_dict.fillna(-9999.0)

        with self.output().open("w") as out:
            pred_dict.to_netcdf(out.name)


@requires(GroupCommodities, ExtractBorderShapefile)
class PriceToCSV(Task):
    def output(self):
        return FinalTarget(
            path=f"model_prices/model_prices_{self.country_level}_{self.rainfall_scenario}.csv",
            task=self,
        )

    def run(self):
        with self.input()[0].open("r") as f:
            price = f.read()
        price.drop(columns=["geometry"], inplace=True)
        price = price.reset_index()

        with self.input()[1].open("r") as s:
            shape_files = s.read()

        pred_dict = merge_with_shapefile(price, shape_files, "adminx")

        pred_dict = pred_dict.reset_index(drop=True)
        pred_dict["Country"] = self.country_level
        pred_dict.rename(columns={"Geography": "admin2"}, inplace=True)

        pred_dict = pred_dict.set_index(["Country", "admin2", "Time"])

        pred_dict = pred_dict.loc[~pred_dict.index.duplicated()]
        pred_dict = pred_dict.drop("geometry", 1)
        pred_dict.drop(columns="admin1_y", inplace=True)
        pred_dict.rename(columns={"admin1_x": "admin1"}, inplace=True)
        pred_dict = pred_dict.drop("Fruit", axis=1, errors="ignore")

        commodity_list = [
            "Pulses and vegetables",
            "Sugar, jam, honey, chocolate and candy",
            "Bread and Cereals",
            "Meat",
            "Milk, cheese and eggs",
            "Oils and fats",
        ]

        for x in commodity_list:
            print(x)
            if x not in pred_dict.columns:
                pred_dict.insert(3, x, "")

        with self.output().open("w") as out:
            pred_dict.to_csv(out.name)


@requires(GroupCommodities, ExtractBorderShapefile, GlobalParametersMPM)
class PriceToGeoJSON(Task):
    """
    This task consolidates all of the data used as input to the price model and writes it to a single directory.
    Requires all preprocessing tasks.
    """

    def output(self):
        if self.country_level == COUNTRY_CHOICES["ETH"]:
            group = [
                "Milk, cheese and eggs",
                "Pulses and vegetables",
                "Meat",
                "Bread and Cereals",
                "Sugar, jam, honey, chocolate and candy",
                "Fruit",
            ]
        elif self.country_level == COUNTRY_CHOICES["SS"]:
            group = [
                "Milk, cheese and eggs",
                "Pulses and vegetables",
                "Meat",
                "Bread and Cereals",
                "Sugar, jam, honey, chocolate and candy",
                "Oils and fats",
            ]
        try:
            outmap = {
                i: FinalTarget(
                    path=f"model_prices/price_gr_{i.replace(' ', '_')}_{self.country_level.replace(' ', '_')}_{self.rainfall_scenario}_rainfall.geojson",
                    task=self,
                    ACL="public-read",
                )
                for i in group
            }
        except KeyError:
            outmap = {
                i: FinalTarget(
                    path=f"model_prices/price_gr_{i.replace(' ', '_')}_{self.country_level.replace(' ', '_')}_{self.rainfall_scenario}_rainfall.geojson",
                    task=self,
                )
                for i in group
            }
        return outmap

    def run(self):

        with self.input()[0].open("r") as src:
            group_price = src.read()
            group_price = group_price.reset_index()
            group_price = group_price.drop(columns=["geometry"])

        with self.input()[1].open("r") as f:
            state_boundaries = f.read()

        if self.country_level == COUNTRY_CHOICES["ETH"]:
            group = [
                "Milk, cheese and eggs",
                "Pulses and vegetables",
                "Meat",
                "Bread and Cereals",
                "Sugar, jam, honey, chocolate and candy",
                "Fruit",
            ]

        elif self.country_level == COUNTRY_CHOICES["SS"]:
            group = [
                "Milk, cheese and eggs",
                "Pulses and vegetables",
                "Meat",
                "Bread and Cereals",
                "Sugar, jam, honey, chocolate and candy",
                "Oils and fats",
            ]

        for x in group:
            group_pr = group_price

            group_pr["start"] = pd.to_datetime(group_pr["Time"])
            group_pr["end"] = group_pr["start"] + pd.DateOffset(months=1)

            group_pr["start"] = group_pr["start"].astype(str)
            group_pr["end"] = group_pr["end"].astype(str)

            group_pr["Zone"] = group_pr["Geography"]
            group_pr["Month"] = group_pr["Time"].dt.strftime("%b")
            group_pr["Year"] = group_pr["Time"].dt.strftime("%Y")

            group_pr = group_pr[
                [
                    "Geography",
                    "Zone",
                    (x),
                    "start",
                    "end",
                    "Month",
                    "Year",
                    "rainfall",
                    "rainfall_scenario",
                ]
            ]

            group_pr.insert(1, "Commodity", (x))
            group_pr = group_pr.rename(columns={(x): "Price"})

            merged = merge_with_shapefile(group_pr, state_boundaries, gdf_on="State")
            merged = merged.set_index(["Geography"]).set_geometry("geometry")

            schema = datetime_schema(merged)  # noqa: F841

            with self.output()[x].open("w") as out:
                merged.to_file(out.name, driver="GeoJSON")


@requires(GroupCommodities, ReferenceRaster, GlobalParametersMPM)
class PricesGeotiff(Task):
    """
    Converts price vectors to geoTIFF rasters and burn each commodity price
    into a band (total of 6 bands), datetime tag included
    """

    def output(self):
        # return IntermediateTarget(
        #     path=f"{self.task_id}/", task=self, timeout=60 * 60 * 24 * 7
        # )
        month_list = pd.date_range(self.time.date_a, self.time.date_b)
        month_list = set([i.replace(day=1) for i in month_list])
        if self.country_level == COUNTRY_CHOICES["ETH"]:
            group = [
                "Milk, cheese and eggs",
                "Pulses and vegetables",
                "Meat",
                "Bread and Cereals",
                "Sugar, jam, honey, chocolate and candy",
                "Fruit",
            ]
        elif self.country_level == COUNTRY_CHOICES["SS"]:
            group = [
                "Milk, cheese and eggs",
                "Pulses and vegetables",
                "Meat",
                "Bread and Cereals",
                "Sugar, jam, honey, chocolate and candy",
                "Oils and fats",
            ]
        elif self.country_level == COUNTRY_CHOICES["KE"]:
            group = [
                "Milk, cheese and eggs",
                "Pulses and vegetables",
                "Meat",
                "Bread and Cereals",
                "Sugar, jam, honey, chocolate and candy",
                "Oils and fats",
            ]
        elif self.country_level == COUNTRY_CHOICES["UG"]:
            group = [
                "Pulses and vegetables",
                "Bread and Cereals",
                "Oils and fats",
            ]
        elif self.country_level == COUNTRY_CHOICES["SD"]:
            group = [
                "Sorghum (white)",
                "Millet",
                "Wheat",
                "Sorghum",
                "Sorghum (food aid)",
                "Exchange rate (unofficial)",
            ]
        try:
            outmap = {
                self.commodity_time(i, t): FinalTarget(
                    path=f"market_price_model/{self.country_level}_{t}_{i}_{self.rainfall_scenario}_rainfall_market_price.tiff",
                    task=self,
                    ACL="public-read",
                )
                for i in group
                for t in month_list
            }
        except KeyError:
            outmap = {
                self.commodity_time(i, t): FinalTarget(
                    path=f"market_price_model/{self.country_level}_{t}_{i}_{self.rainfall_scenario}_rainfall_market_price.tiff",
                    task=self,
                )
                for i in group
                for t in month_list
            }
        return outmap

    @staticmethod
    def commodity_time(commodity, time):
        time_str = time.strftime("%Y_%m_%d")
        return f"{time_str}_{commodity}"

    def run(self):
        with self.input()[0].open("r") as f:
            prices = f.read()
        prices.fillna(
            {i: 0 for i in prices.columns if i != "geometry"},
            method=None,
            inplace=True,
        )
        # prices.fillna(value=0, method=None, inplace=True)
        commodities = prices.columns.tolist()
        commodities.remove("geometry")
        commodities.remove("rainfall")
        commodities.remove("rainfall_scenario")
        # band_counts = len(commodities)
        band_counts = 1
        # use reference raster to ensure consistent georeferencing
        raster_path_names = self.input()[1]
        if self.country_level == COUNTRY_CHOICES["SS"]:
            raster_country = raster_path_names["ss_raster"].path
        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            raster_country = raster_path_names["eth_raster"].path
        else:
            raise NotImplementedError
        with rasterio.open(raster_country) as src:
            meta = src.meta.copy()
            geography = geography_f_country(self.country_level)
            try:
                geo_mask = geography["features"][0]["geometry"]
            except KeyError:
                geo_mask = geography
            masked_img, masked_transform = mask(src, [geo_mask], crop=True)
            meta.update(
                height=masked_img.shape[1],
                width=masked_img.shape[2],
                count=band_counts,
                dtype=rasterio.float64,  # need to specify dtype because price target is float
                transform=masked_transform,
                nodata=-9999,
            )
        for t in prices.index.unique(level="Time"):
            for c in commodities:
                with self.output()[self.commodity_time(c, t)].open(
                    "w"
                ) as raster_outname:
                    prices_t = prices.loc[prices.index.get_level_values("Time") == t]
                    shapes = (
                        (geom, value)
                        for geom, value in zip(prices_t["geometry"], prices_t[c])
                    )
                    burned = features.rasterize(
                        shapes=shapes,
                        fill=-9999,
                        out_shape=(meta["height"], meta["width"]),
                        transform=meta["transform"],
                    )
                    # Note to self, the for loop comes *after* the with rasterio.open
                    with rasterio.open(raster_outname, "w", **meta) as out_raster:
                        out_raster.update_tags(Time=t)
                        # for i in range(len(commodity_raster)):
                        out_raster.write_band(1, burned.astype(np.float64))
                        # Tag raster bands
                        band_idx = "band_" + str(1)
                        band_tag = {band_idx: burned}
                        out_raster.update_tags(**band_tag)


@requires(MarketAccessibilityPath, GlobalParametersMPM)
class MarketLocationToPrice(Task):
    """
    Convert the data to geodataframe
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24)

    def run(self):
        path_name = self.input()[0]
        if self.country_level == COUNTRY_CHOICES["ETH"]:
            market_location = path_name["market_location"].path

        df = pd.read_csv(market_location)
        point_geom = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
        gdf = gpd.GeoDataFrame(df, geometry=point_geom)

        with self.output().open("w") as out:
            out.write(gdf)


@requires(MarketAccessibilityPath, GlobalParametersMPM)
class CreateMarketRaster(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 7)

    def run(self):
        path_names = self.input()[0]

        market_location = path_names["market_location"].path
        travel_time = path_names["travel_time"].path

        geo_cluster = buffer_points(
            travel_time,
            market_location,
            buf_rad=5000,
            lat_name="latitude",
            lon_name="longitude",
            fname_out=None,
        )

        geo_cluster["price"] = 0

        with self.output().open("w") as output:
            output.write(geo_cluster)


@requires(
    GroupCommodities,
    ReferenceRaster,
    GlobalParametersMPM,
    MarketLocationToPrice,
    CreateMarketRaster,
)
class PricesGeotiffHighRes(Task):
    """
    Converts price vectors to geoTIFF rasters and burn each commodity price
    into a band (total of 6 bands), datetime tag included
    """

    def output(self):
        return IntermediateTarget(
            path=f"{self.task_id}/", task=self, timeout=60 * 60 * 24 * 7
        )

    def run(self):
        with self.input()[0].open("r") as f:
            prices = f.read()
            commodities = prices.columns.tolist()
            commodities.remove("geometry")
            commodities.remove("rainfall")
            commodities.remove("rainfall_scenario")
            band_counts = len(commodities) + 1

        # use reference raster to ensure consistent georeferencing

        with self.input()[4].open("r") as f:
            geo_cluster = f.read()
        market_mask = geo_cluster["geometry"]

        with self.input()[3].open("r") as f:
            market_location = f.read()

        market_location.reset_index(inplace=True)
        market_location.rename(columns={"index": "market_id"}, inplace=True)

        raster_path_names = self.input()[1]

        if self.country_level == COUNTRY_CHOICES["SS"]:
            raster_country = raster_path_names["ss_raster"].path

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            raster_country = raster_path_names["eth_raster"].path

        else:
            raise NotImplementedError

        with rasterio.open(raster_country) as src:
            meta = src.meta.copy()
            geography = geography_f_country(self.country_level)
            try:
                geo_mask = geography["features"][0]["geometry"]
            except KeyError:
                geo_mask = geography

            masked_img, masked_transform = mask(src, [geo_mask], crop=True)
            meta.update(
                height=masked_img.shape[1],
                width=masked_img.shape[2],
                count=band_counts,
                dtype=rasterio.float64,  # need to specify dtype because price target is float
                transform=masked_transform,
                nodata=-9999,
            )

        # for each time point, create a raster with the diff. commodity bands and tag the bands
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            for t in prices.index.unique(level="Time"):
                commodity_raster = []
                raster_outname = os.path.join(
                    tmpdir,
                    f"{self.country_level}_{t}_{self.rainfall_scenario}_rainfall_market_price.tiff",
                )
                prices_comm = []
                for c in commodities:
                    prices_t = prices.loc[prices.index.get_level_values("Time") == t]

                    comm_length = len(prices_t)

                    geo_admin = gpd.GeoDataFrame(
                        prices_t["geometry"], geometry="geometry"
                    )
                    poly = geo_admin["geometry"]

                    prices_comm = prices_t[c]

                    for i in range(comm_length):
                        market_location.loc[
                            market_location["geometry"].within(poly[i]), c
                        ] = prices_comm[i]

                    shapes = (
                        (geom, value)
                        for geom, value in zip(market_mask, market_location[c])
                    )
                    burned = features.rasterize(
                        shapes=shapes,
                        fill=-9999,
                        out_shape=(meta["height"], meta["width"]),
                        dtype=rasterio.float64,
                        transform=meta["transform"],
                    )

                    commodity_raster.append(burned)

                    shapes_id = (
                        (geom, value)
                        for geom, value in zip(
                            market_mask, market_location["market_id"]
                        )
                    )

                    market_id_burned = features.rasterize(
                        shapes=shapes_id,
                        fill=-9999,
                        out_shape=(meta["height"], meta["width"]),
                        dtype=rasterio.float64,
                        transform=meta["transform"],
                    )

                    # Note to self, the for loop comes *after* the with rasterio.open
                    with rasterio.open(raster_outname, "w", **meta) as out_raster:
                        out_raster.update_tags(Time=t)
                        for i in range(len(commodity_raster)):
                            out_raster.write_band(
                                i + 1, commodity_raster[i].astype(np.float64)
                            )

                            out_raster.write_band(
                                band_counts, market_id_burned.astype(np.float64)
                            )

                            # Tag raster bands

                            band_idx = "band_" + str(i + 1)
                            band_tag = {band_idx: commodities[i], "band_7": "market_id"}

                            out_raster.update_tags(**band_tag)


class PriceRaster(ExternalTask):
    """
    The price raster manually imported on Ckan
    """

    def output(self):
        return {
            "eth_price_raster": CkanTarget(
                dataset={"id": "528383cd-06b3-4e46-a5ce-2cf161546378"},
                resource={"id": "b07a7330-aacb-4b0a-939a-0561aaf82c6b"},
            ),
        }


@requires(
    MarketAccessibilityPath,
    ReferenceRaster,
    PricesGeotiffHighRes,
    GroupCommodities,
    GlobalParametersMPM,
)
class MergePriceAccessbilityRaster(Task):
    # Working with accessibility rast

    def output(self):

        return IntermediateTarget(
            path=f"{self.task_id}/", task=self, timeout=60 * 60 * 24 * 7
        )

    def run(self):

        path_name = self.input()[0]
        price_dir = self.input()[2].path

        if self.country_level == COUNTRY_CHOICES["ETH"]:
            travel_time_raster = path_name["travel_time"].path
            market_id_raster = path_name["market_id_raster"].path

            # market_location = path_name["market_location"].path
            # market_location_json = path_name["market_location_json"].path

            with rasterio.open(travel_time_raster) as dst:
                travel_time = dst.read(1)

            with rasterio.open(market_id_raster) as src:
                travel_id = src.read(1)

        files = os.listdir(price_dir)

        # price_input = [f for f in files]

        with self.input()[3].open("r") as f:
            prices = f.read()

            commodities = prices.columns.tolist()
            commodities.remove("geometry")
            commodities.remove("rainfall")
            commodities.remove("rainfall_scenario")
            band_counts = len(commodities)

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            for f in files:
                commodity_raster = []
                if f.startswith("Ethiopia"):
                    with rasterio.open(os.path.join(price_dir, f)) as src:
                        for j in range(1, band_counts + 1):
                            # print(j)
                            profile = src.profile

                            price1 = src.read(j)
                            price_id = src.read(band_counts + 1)

                            price_flat = price1.flatten()

                            price_id_flat = price_id.flatten()
                            price_commodity = np.array([price_flat, price_id_flat])
                            price_df = pd.DataFrame(
                                data=price_commodity, index=["price", "id"]
                            )
                            price = price_df.transpose()

                            df_price_mod = price[price["id"] != -9999.0]
                            df_price_mod = df_price_mod.drop_duplicates(keep="first")

                            df_price_mod.sort_values(by=["id"])
                            df_price_mod.reset_index(drop="True", inplace=True)
                            number_of_markets = len(df_price_mod["id"])

                            price_pixel = np.empty(price1.shape)
                            price_pixel.fill(-9999.0)

                            commodity_price_avg = df_price_mod["price"].mean()

                            for i in range(number_of_markets):
                                price_pixel = np.where(
                                    travel_id == i,
                                    df_price_mod.loc[i, "price"],
                                    price_pixel,
                                )
                            transport_coeff = (
                                0.002  # TODO modify this for each commodity
                            )

                            price_all_pix = np.where(
                                price_pixel != -9999.0,
                                price_pixel
                                + travel_time * (commodity_price_avg * transport_coeff),
                                -9999.0,
                            )
                            commodity_raster.append(price_all_pix)
                            # TODO name format requires a change
                        raster_outname = os.path.join(tmpdir, f"{f}",)
                        profile.update(count=6)

                    with rasterio.open(raster_outname, "w", **profile) as out_raster:

                        # out_raster.update_tags(Time=t)
                        for c in range(band_counts):
                            # print(c)
                            out_raster.write_band(
                                c + 1, commodity_raster[c].astype(np.float64)
                            )
                            # band_idx = "band_" + str(i + 1)
                            # band_tag = {band_idx: commodities[i]}
                            # out_raster.update_tags(**band_tag)


@requires(GroupCommodities, ReferenceRaster)
class RasterizePrices(Task):
    """
    Converts price shape vectors to dataframe format for passing down to Demand model
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=60)

    def run(self):
        with self.input()[0].open("r") as f:
            prices = f.read()

        # use reference raster to ensure consistent georeferencing
        with rasterio.open(self.input()[1].path) as src:
            meta = src.meta
            geography = geography_f_country(self.country_level)
            try:
                geo_mask = geography["features"][0]["geometry"]
            except KeyError:
                geo_mask = geography
            masked_img, masked_transform = mask(src, [geo_mask], crop=True)
            meta.update(
                height=masked_img.shape[1],
                width=masked_img.shape[2],
                transform=masked_transform,
            )

        # dictionary to store rasterized price maps
        commodity_prices_by_time = {}

        commodities = prices.columns.tolist()
        commodities.remove("geometry")

        for t in prices.index.unique(level="Time"):
            t_dict = {}
            for c in commodities:
                prices_t = prices.loc[prices.index.get_level_values("Time") == t]
                shapes = (
                    (geom, value)
                    for geom, value in zip(prices_t["geometry"], prices_t[c])
                )

                burned = features.rasterize(
                    shapes=shapes,
                    fill=-9999,
                    out_shape=(meta["height"], meta["width"]),
                    transform=meta["transform"],
                )
                t_dict[c] = burned.flatten()

            commodity_grid_df = pd.DataFrame.from_dict(t_dict, orient="columns")
            commodity_prices_by_time[t] = commodity_grid_df

        with self.output().open("w") as dst:
            dst.write(commodity_prices_by_time)


@requires(RasterizePrices)
class SavePriceModelOutput(Task):
    def output(self):
        return FinalTarget("commodity_prices.xlsx", task=self)

    def run(self):
        with self.input().open("r") as src:
            price_surfaces = src.read()
        with pd.ExcelWriter(self.output().path) as wb:
            for k, v in price_surfaces.items():
                v.to_excel(wb, sheet_name=str(k).split(" ")[0])


# These terminal tasks create raster outputs: PricesGeotiff, PricesGeotiffHighRes, MergePriceAccessbilityRaster
# Example: luigi --module models.market_price_model.tasks models.market_price_model.tasks.PricesGeotiff --local-scheduler
