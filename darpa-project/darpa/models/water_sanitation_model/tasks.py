import datetime
import logging
import os
from io import BytesIO

# import dask
import dask
import dask.dataframe as dd
import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import rasterio
import rioxarray  # noqa: F401
import xarray as xr
from affine import Affine
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from luigi import ExternalTask, Task
from luigi.contrib.s3 import S3Target
from luigi.util import inherits, requires
from rasterstats import zonal_stats
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.metrics import r2_score
from sklearn.model_selection import RandomizedSearchCV, StratifiedShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler as Scaler

from utils.geospatial_tasks.functions.geospatial import geography_f_country
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from .mappings import ghs_smod_map
from .x_data_prep import (
    CalculateSlope,
    ClusterGPS,
    GetPredictors,
    NightTimeLight,
    PullAccessibility,
    PullAridityIndex,
    PullFrictionSurface,
    PullGHSBuilt,
    PullGHSettlementLayers,
)
from .y_data_prep import DHSWASHData, JPMWASHData, LSMSWASHData

from sklearn.experimental import enable_hist_gradient_boosting  # noqa: F401 isort:skip
from sklearn.ensemble import HistGradientBoostingRegressor as Estimator  # isort:skip


logger = logging.getLogger("luigi-interface")


@inherits(ClusterGPS)
class TrainingData(Task):
    def requires(self):
        lsms = self.clone(LSMSWASHData)
        jmp = self.clone(JPMWASHData)
        pred = self.clone(GetPredictors)
        dhs = self.clone(DHSWASHData)
        if self.country_level == "Kenya":
            return [dhs, pred]
        elif self.data_source == "LSMS":
            return [lsms, pred]
        else:
            return [jmp, pred]

    def output(self):
        dst = f"models/WASH_train_data_{self.country_level}.csv"
        return IntermediateTarget(path=dst, task=self, timeout=31536000)

    def run(self):
        with self.input()[0].open() as src:
            df_y = src.read()
        try:
            df_x = pd.read_csv(self.input()[1].path)
        except UnicodeDecodeError:
            with self.input()[1].open() as src:
                df_x = src.read()

        df_x["month"] = pd.to_datetime(df_x["month"])
        df_x["year"] = df_x["month"].dt.year
        df_x["month_num"] = df_x["month"].dt.month
        if self.country_level in ["Ethiopia", "South Sudan", "Kenya"]:
            merge_vars = ["latitude", "longitude", "year", "month_num"]

        elif (self.country_level == "Djibouti") & (self.data_source == "LSMS"):
            merge_vars = ["region", "year", "month_num"]

        elif (self.country_level == "Uganda") & (self.data_source == "LSMS"):
            merge_vars = ["region", "year", "month_num"]

        elif (self.country_level in ["Djibouti", "Uganda"]) & (
            self.data_source == "JMP"
        ):
            df_y = df_y.rename(columns={"Year": "year"})
            merge_vars = ["Country", "year"]

        else:
            raise NotImplementedError
        if self.country_level == "Kenya":
            df_y = df_y.rename(columns={"LATNUM": "latitude", "LONGNUM": "longitude"})
        df = df_x.merge(df_y, on=merge_vars, how="outer",)
        df["travel_friction"] = df["travel_time_mean"] * df["friction_mean"]
        df["ghs_smod_v1"] = df["ghs_smod"].replace(ghs_smod_map)
        important_vars = [
            "travel_friction",
            "year",
            "dia_prev_2015_mean",
            "travel_time_mean",
            "pop_mean",
            "ntl_mean",
            "friction_mean",
            "temp_suit_mean",
            "dem_mean",
            "aridity_index_mean",
            "lst_night_mean",
            "nir_mean",
            "blue_mean",
            "ghs_built_mean",
            "ghs_pop_mean",
            "red_mean",
            "lst_day_mean",
            "mir_mean",
            "ndvi_mean",
            "evi_mean",
            "silt_mean",
            "sand_mean",
            "clay_mean",
            "rainfall_mean",
        ]
        for i in important_vars:
            if i in df.columns:
                df[f"{i}_log"] = np.log(df[i])
                df[f"{i}_square"] = np.square(df[i])
                df[f"{i}_sqrt"] = np.sqrt(df[i])
        with self.output().open("w") as out:
            df.to_csv(out.name, index=False)


@requires(TrainingData)
class UploadWASHTrainData(Task):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"name": f"WASH Train Data {self.country_level} v7"},
        )

    def run(self):
        self.output().put(self.input().path)


@inherits(GlobalParameters)
class PullTrainingData(ExternalTask):
    def output(self):
        if self.country_level == "Ethiopia":
            return CkanTarget(
                dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
                resource={"id": "530f9a5d-8594-4e60-b952-8a20f3a4b1e3"},
            )
        elif self.country_level == "Djibouti":
            return CkanTarget(
                dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
                resource={"id": "54467801-78db-4bf6-9f40-fe7c3fa2a230"},
            )
        elif self.country_level == "South Sudan":
            return CkanTarget(
                dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
                resource={"id": "230856ea-8c38-4632-a945-66e95cfaa284"},
            )
        elif self.country_level == "Uganda":
            return CkanTarget(
                dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
                resource={"id": "c8ab83ca-6c8b-41ac-8573-523e03c352b8"},
            )
        elif self.country_level == "Kenya":
            return CkanTarget(
                dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
                resource={"id": "f2a03e18-bb5c-4e80-b04f-21dde2ef1798"},
            )
        else:
            return NotImplementedError


@requires(PullTrainingData)
class TrainSanitation(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    @staticmethod
    def clean_dataset(df):
        assert isinstance(df, pd.DataFrame), "df needs to be a pd.DataFrame"
        df.dropna(inplace=True)
        indices_to_keep = ~df.isin([np.nan, np.inf, -np.inf]).any(1)
        return df[indices_to_keep]

    def run(self):
        df = pd.read_csv(self.input().path)
        # The model for kenya perform better with:'aridity_index_mean_square','friction_mean_square','temp_suit_mean_square', 'slope_mean', 'ghs_pop_mean_log'
        num_columns = [
            "year",
            "ntl_mean_sqrt",
            # "ghs_smod_mean",
            "aridity_index_mean_log",
        ]
        ordinal_columns = []  # ["ghs_smod"]
        parameters = {
            "estimator__regressor__loss": ["least_squares"],
            "estimator__regressor__learning_rate": [0.05, 0.1, 0.15],
            "estimator__regressor__max_iter": [
                20,
                30,
                40,
                50,
                60,
                70,
                80,
                90,
                100,
                150,
                200,
                250,
                300,
                500,
                600,
                700,
                800,
                900,
                1000,
            ],
            "estimator__regressor__max_leaf_nodes": [
                None,
                15,
                14,
                11,
                13,
                10,
                9,
                8,
                7,
                5,
                4,
                3,
            ],
            "estimator__regressor__max_depth": [None],
            "estimator__regressor__min_samples_leaf": [
                60,
                65,
                70,
                75,
                80,
                85,
                90,
                95,
                100,
            ],
            "estimator__regressor__l2_regularization": [
                0.2,
                0.3,
                0.4,
                0.5,
                0.6,
                0.7,
                0.8,
                0.9,
            ],
        }

        target_vars = "unimproved_sanitation"
        vars_list = num_columns + ordinal_columns + [target_vars]
        df = df.dropna(subset=vars_list)
        df = df[vars_list]
        df = self.clean_dataset(df)
        df = df.reset_index(drop=True)
        split = StratifiedShuffleSplit(n_splits=1, test_size=0.15, random_state=42)
        bins = np.linspace(start=0, stop=1, num=10)
        df["y_binned"] = np.digitize(df[target_vars], bins, right=True)
        for train_index, test_index in split.split(df, df["y_binned"]):
            train_set = df[vars_list].iloc[train_index].copy()
            test_set = df[vars_list].iloc[test_index].copy()

        X_train = train_set[num_columns + ordinal_columns].copy()
        y_train = train_set[target_vars]

        X_test = test_set[num_columns + ordinal_columns].copy()
        y_test = test_set[target_vars]

        preprocessor = ColumnTransformer([("numeric", Scaler(), num_columns)])

        estimator = TransformedTargetRegressor(Estimator())
        pipeline = Pipeline([("preprocessor", preprocessor), ("estimator", estimator)])

        param_search = RandomizedSearchCV(
            estimator=pipeline,
            param_distributions=parameters,
            n_iter=2000,
            cv=3,
            verbose=1,
            n_jobs=-1,
            scoring="neg_mean_squared_error",
        )
        param_search.fit(X_train, y_train)
        best_random = param_search.best_estimator_
        y_pred = best_random.predict(X_test)
        y_pred[y_pred > 1] = 1
        y_pred[y_pred < 0] = 0
        r2_test = r2_score(y_test, y_pred)
        y_pred_train = best_random.predict(X_train)
        r2_train = r2_score(y_train, y_pred_train)
        logger.info(f"Test r2 is {r2_test}")
        logger.info(f"Train r2 is {r2_train}")
        with self.output().open("w") as out:
            out.write(
                {
                    "model": best_random,
                    "eval": {"y_pred": y_pred, "y_test": y_test},
                    "preds": num_columns + ordinal_columns,
                }
            )


@requires(PullTrainingData)
class TrainUnimprovedWaterModel(TrainSanitation):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        data = pd.read_csv(self.input().path)

        num_columns = [
            "ghs_built_mean_sqrt",
            "travel_time_mean_sqrt",
            "aridity_index_mean_sqrt",
            "friction_mean_square",
            "slope_mean",
            "ntl_mean_log",
            "year",
        ]
        ordinal_vars = ["ghs_smod"]
        target_vars = "unimproved_drinking_water"
        vars_list = num_columns + ordinal_vars + [target_vars]

        df = data.dropna(subset=vars_list)
        df = df[vars_list]

        df = self.clean_dataset(df)
        df = df.reset_index(drop=True)
        split = StratifiedShuffleSplit(n_splits=1, test_size=0.15, random_state=42)
        bins = np.linspace(start=0, stop=1, num=10)
        df["y_binned"] = np.digitize(df[target_vars], bins, right=True)
        for train_index, test_index in split.split(df, df["y_binned"]):
            train_set = df[vars_list].iloc[train_index].copy()
            test_set = df[vars_list].iloc[test_index].copy()

        X_train = train_set[num_columns + ordinal_vars].copy()
        y_train = train_set[target_vars]
        X_test = test_set[num_columns + ordinal_vars].copy()
        y_test = test_set[target_vars]

        preprocessor = ColumnTransformer([("numeric", Scaler(), num_columns)])
        estimator = TransformedTargetRegressor(Estimator())
        pipeline = Pipeline([("preprocessor", preprocessor), ("estimator", estimator)])

        parameters = {
            "estimator__regressor__learning_rate": [0.0125],
            "estimator__regressor__max_iter": [
                50,
                100,
                150,
                200,
                250,
                300,
                350,
                360,
                370,
                380,
                390,
                400,
                410,
                420,
                430,
                440,
                450,
                500,
                550,
                600,
                650,
                700,
                750,
                800,
                850,
                900,
                950,
            ],
            "estimator__regressor__max_depth": [None],
            "estimator__regressor__max_leaf_nodes": [None],
            "estimator__regressor__min_samples_leaf": [55, 60, 65, 70],
            "estimator__regressor__l2_regularization": [
                0.35,
                0.40,
                0.55,
                0.6,
                0.7,
                0.75,
            ],
        }

        param_search = RandomizedSearchCV(
            estimator=pipeline,
            param_distributions=parameters,
            n_iter=900,
            cv=3,
            verbose=1,
            n_jobs=-1,
            scoring="neg_mean_squared_error",
        )

        param_search.fit(X_train, y_train)
        best_random = param_search.best_estimator_

        y_pred = best_random.predict(X_test)
        y_pred[y_pred > 1] = 1
        y_pred[y_pred < 0] = 0
        r2_test = r2_score(y_test, y_pred)
        y_pred_train = best_random.predict(X_train)
        r2_train = r2_score(y_train, y_pred_train)
        logger.info(f"Test r2 is {r2_test}")
        logger.info(f"Train r2 is {r2_train}")
        with self.output().open("w") as out:
            out.write(
                {
                    "model": best_random,
                    "eval": {"y_test": "y_test", "y_pred": "y_pred"},
                    "preds": num_columns + ordinal_vars,
                }
            )


@inherits(GlobalParameters)
class PullSanitationInferenceData(ExternalTask):
    def output(self):
        dst = "s3://darpa-output-dev/final_targets/WASH Inference data/"
        if self.country_level == "Ethiopia":
            return S3Target(
                path=os.path.join(dst, "ethiopia_toilet_facility_predictors.gzip")
            )
        elif self.country_level == "South Sudan":
            return S3Target(
                path=os.path.join(
                    dst, "south_sudan_toilet_facility_predictors.parquet.gzip"
                )
            )
        elif self.country_level == "Uganda":
            return S3Target(
                path=os.path.join(dst, "uganda_toilet_facility_predictors.parquet.gzip")
            )
        elif self.country_level == "Djibouti":
            return S3Target(
                path=os.path.join(
                    dst, "djibouti_toilet_facility_predictors.parquet.gzip"
                )
            )
        elif self.country_level == "Kenya":
            return S3Target(
                path=os.path.join(dst, "kenya_toilet_facility_predictors.parquet.gzip")
            )
        else:
            raise NotImplementedError


@inherits(GlobalParameters)
class SanitationInferenceData(Task):
    def requires(self):
        ntl_task = self.clone(NightTimeLight)
        aridity_task = self.clone(PullAridityIndex)
        month_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
        month_list = month_list[:-1]
        train_task = self.clone(TrainSanitation)
        friction_task = self.clone(PullFrictionSurface)
        ghs_smod_task = self.clone(PullGHSettlementLayers)
        return {
            "ntl_mean": {i: ntl_task.clone(month=i) for i in month_list},
            "ghs_smod": ghs_smod_task,
            "aridity_index_mean": aridity_task,
            "train": train_task,
            "friction_mean": friction_task,
        }

    def output(self):
        dst = f"WASH Inference data/{self.country_level.lower().replace(' ', '_')}_toilet_facility_predictors.parquet.gzip"
        return FinalTarget(path=dst)

    def read_rasterio(self, src_file, name=None, month=None):
        da = xr.open_rasterio(src_file)
        da = da.squeeze(dim="band")
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]
        da = da.rio.clip(geom)
        da = da.where(da != da.attrs["_FillValue"])

        da = da.rio.reproject(
            "EPSG:4326", resolution=(0.05000000074505806, 0.05000000074505806)
        )

        if name:
            da.name = name
        if month:
            da = da.expand_dims({"month": [month]})
        return da

    def read_monthly_raster(self, data_map, name=None):
        ds = xr.concat(
            [self.read_rasterio(v.path, name, k) for k, v in data_map.items()], "month"
        )
        return ds

    def run(self):
        input_map = self.input()
        ds_map = {}
        ds_map["ntl_mean"] = self.read_monthly_raster(input_map["ntl_mean"], "ntl_mean")
        month_set = set(ds_map["ntl_mean"]["month"].data)
        month = list(month_set)[0]

        monthly_inputs = ["ntl_mean"]

        static_inputs = set(input_map.keys()) - set(monthly_inputs + ["train"])

        for i in static_inputs:
            ds_map[i] = self.read_rasterio(input_map[i].path, i, month)

        for k in ds_map:
            if k != "friction_mean":
                ds_map[k] = ds_map[k].rio.reproject_match(ds_map["friction_mean"])

        ds_map["friction_mean"]["x"] = ds_map["ghs_smod"]["x"]
        ds_map["friction_mean"]["y"] = ds_map["ghs_smod"]["y"]

        ds = xr.merge([v for _, v in ds_map.items()], join="left")
        ds = ds.ffill(dim="month")
        df = ds.to_dataframe()
        with self.output().open("w") as out:
            df.to_parquet(out.name, compression="gzip")


@requires(SanitationInferenceData)
class UploadSanitationInferenceData(Task):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"name": f"Sanitation Inference {self.country_level}"},
        )

    def run(self):
        src_file = self.input().path
        self.output().put(src_file)


@requires(UploadSanitationInferenceData, TrainSanitation, GlobalParameters)
class PredictUnimprovedToiletFacility(Task):
    aridity_index_percent_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[-0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0],
        var_type=np.float,
        description="""Percentage change in aridity index. Positive values increase
        the aridity index and negative values reduce the aridity index.
        The Aridity Index (AI) is a simple but convenient numerical indicator
        of aridity based on long-term climatic water deficits and is calculated
        as the ratio P/PET. The less the values the drier the area.""",
    )

    night_light_percent_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[-0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0],
        var_type=float,
        description="""Percentage change in night time light. Possitive values increase
        the night time light and negative values reduces the night time light.
        It is a measure of the intensity of light emitted from Earth at night and is
        used to characterize the intensity of the socioeconomic activities and urbanization.""",
    )
    smod_offset = luigi.IntParameter(default=0)

    def read_rasterio(self, src_file, name=None, month=None):
        da = xr.open_rasterio(src_file)
        da = da.squeeze(dim="band")
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]
        da = da.rio.clip(geom)
        da = da.where(da != da.attrs["_FillValue"])
        if name:
            da.name = name
        if month:
            da = da.expand_dims({"month": [month]})
        return da

    def read_monthly_raster(self, data_map, name=None):
        ds = xr.concat(
            [self.read_rasterio(v.path, name, k) for k, v in data_map.items()], "month"
        )
        return ds

    def apply_smod_offset(self, x):
        if (x == 0) | (np.isnan(x)):
            return x
        elif (x + self.smod_offset) > 3:
            return 3
        elif (x + self.smod_offset) < 1:
            return 1
        else:
            return x + self.smod_offset

    def output(self):
        dst = f"water_sanitation_model/toilet_facility_{self.country_level}.nc"
        return FinalTarget(path=dst, task=self)

    def run(self):
        with self.input()[1].open() as src:
            inputs = src.read()

        model = inputs["model"]
        predictors = inputs["preds"]

        y_list = []
        df = dd.read_parquet(self.input()[0].path)
        if self.country_level in ["Ethiopia", "South Sudan"]:
            df = df.repartition(npartitions=10)
        for part in df.partitions:
            part = part.compute()
            part = dask.delayed(self.data_preprocessing)(part)

            y_part = dask.delayed(self.prediction)(part, predictors, model)
            y_part = y_part.compute()
            y_list.append(y_part)

        y = xr.concat(y_list, dim="z")
        y = y.sel(z=~y.get_index("z").duplicated())
        y = y.unstack("z")
        y.name = "unimproved_sanitation"
        with self.output().open("w") as out:
            y.to_netcdf(out.name)

    def prediction(self, part, predictors, model):
        X = part[predictors]
        X = X.replace([np.inf, -np.inf], np.nan)
        mask = ~X.isna().any(axis=1)
        y = pd.Series(index=X.index)
        y.loc[mask] = model.predict(X.loc[mask])
        y[y > 1] = 1
        y[y < 0] = 0
        y = y.to_xarray()
        y = y.stack(z=("month", "x", "y"))
        return y

    def data_preprocessing(self, df):
        df = df.reset_index()
        df["year"] = df["month"].dt.year
        df["ghs_smod"] = df["ghs_smod"].replace(ghs_smod_map)
        df = df.set_index(["month", "x", "y"])

        # Apply scenario
        df["ntl_mean"] = df["ntl_mean"] * (1 + self.night_light_percent_change)
        df["aridity_index_mean"] = df["aridity_index_mean"] * (
            1 + self.aridity_index_percent_change
        )
        if self.smod_offset != 0:
            df["ghs_smod"] = df["ghs_smod"].apply(lambda x: self.apply_smod_offset(x))

        for i in df.columns:
            if i not in ["year", "month"]:
                df[f"{i}_log"] = np.log(df[i])
                df[f"{i}_square"] = np.square(df[i])
                df[f"{i}_sqrt"] = np.sqrt(df[i])
        return df


@inherits(GlobalParameters)
class PullWaterInferenceData(ExternalTask):
    def output(self):
        dst = "s3://darpa-output-dev/final_targets/WASH Inference data/"
        if self.country_level == "Ethiopia":
            return S3Target(
                path=os.path.join(dst, "ethiopia_water_source_predictors.parquet.gzip")
            )
        elif self.country_level == "South Sudan":
            return S3Target(
                path=os.path.join(
                    dst, "south_sudan_water_source_predictors.parquet.gzip"
                )
            )
        elif self.country_level == "Uganda":
            return S3Target(
                path=os.path.join(dst, "uganda_water_source_predictors.parquet.gzip")
            )
        elif self.country_level == "Djibouti":
            return S3Target(
                path=os.path.join(dst, "djibouti_water_source_predictors.parquet.gzip")
            )
        elif self.country_level == "Kenya":
            return S3Target(
                path=os.path.join(dst, "kenya_water_source_predictors.parquet.gzip")
            )
        else:
            raise NotImplementedError


class DrinkingWaterInferenceData(SanitationInferenceData):
    def requires(self):
        travel_time = self.clone(PullAccessibility)
        aridity_task = self.clone(PullAridityIndex)
        train_task = self.clone(TrainUnimprovedWaterModel)
        ntl_task = self.clone(NightTimeLight)
        friction_task = self.clone(PullFrictionSurface)
        ghs_build = self.clone(PullGHSBuilt)
        slope_task = self.clone(CalculateSlope)
        ghs_smod_task = self.clone(PullGHSettlementLayers)
        month_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
        month_list = month_list[:-1]
        return {
            "ghs_built_mean": ghs_build,
            "travel_time_mean": travel_time,
            "friction_mean": friction_task,
            "aridity_index_mean": aridity_task,
            "slope_mean": slope_task,
            "ntl_mean": {i: ntl_task.clone(month=i) for i in month_list},
            "ghs_smod": ghs_smod_task,
            "model": train_task,
        }

    def output(self):
        dst = f"WASH Inference data/{self.country_level.lower().replace(' ', '_')}_water_source_predictors.parquet.gzip"
        return FinalTarget(path=dst)

    def run(self):
        input_map = self.input()

        ds_map = {}
        monthly_vars = []
        static_vars = []
        for k, v in input_map.items():
            if isinstance(v, dict):
                monthly_vars.append(k)
            else:
                static_vars.append(k)
        for i in monthly_vars:
            if i != "pop_mean":
                ds_map[i] = self.read_monthly_raster(input_map[i], i)
                month_set = set(ds_map[i]["month"].data)
                month = list(month_set)[0]
            else:
                ds_map[i] = xr.concat([self.read_pop(i) for i in month_set], "month")

        static_vars.remove("model")
        for i in static_vars:
            ds_map[i] = self.read_rasterio(input_map[i].path, i, month)
        match = "friction_mean"
        for k in ds_map:
            if k != match:
                ds_map[k] = ds_map[k].rio.reproject_match(ds_map[match])
        temp = [k for k in ds_map if k != match][0]
        ds_map[match]["x"] = ds_map[temp]["x"]
        ds_map[match]["y"] = ds_map[temp]["y"]
        ds = xr.merge([v for _, v in ds_map.items()], join="left")

        ds = ds.ffill(dim="month")
        df = ds.to_dataframe()
        with self.output().open("w") as out:
            df.to_parquet(out.name, compression="gzip")


@requires(DrinkingWaterInferenceData)
class UploadDrinkingWaterInference(Task):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"name": f"Drinking Water Inference {self.country_level}"},
        )

    def run(self):
        src_file = self.input().path
        self.output().put(src_file)


@requires(DrinkingWaterInferenceData, TrainUnimprovedWaterModel, GlobalParameters)
class PredictUnimprovedDrinkingWater(Task):
    travel_time_percent_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[-0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0],
        var_type=float,
        description="""Positive values indicate an increase in travel time and
        negative values indicate a decrease in travel time.
        Travel Time: minutes required to reach the nearest town using all modes of transportation except for air travel.
        For example: In heavy rainfall unpaved road networks will have an increase in travel time;
        or paving an unpaved road network would decrease the travel time.
        Reference Kimetrica's accessibility model for more information on how to properly reflect increase/decrease in percent travel time:
        https://gitlab.com/kimetrica/darpa/darpa/-/tree/master/models/accessibility_model
        """,
    )

    aridity_index_percent_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[-0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0],
        var_type=float,
        description="""Percentage change in aridity index. Positive values increase
    the aridity index and negative values reduce the aridity index.
    The Aridity Index (AI) is a simple but convenient numerical indicator
    of aridity based on long-term climatic water deficits and is calculated
    as the ratio P/PET. The less the values the drier the area.""",
    )
    night_light_percent_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[-0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0],
        var_type=float,
        description="""Percentage change in night time light. Possitive values increase
        the night time light and negative values reduces the night time light.
        It is a measure of the intensity of light emitted from Earth at night and is
        used to characterize the intensity of the socioeconomic activities and urbanization.
        """,
    )
    smod_offset = luigi.IntParameter(default=0)

    def apply_smod_offset(self, x):
        if (x == 0) | (np.isnan(x)):
            return x
        elif (x + self.smod_offset) > 3:
            return 3
        elif (x + self.smod_offset) < 1:
            return 1
        else:
            return x + self.smod_offset

    def output(self):
        dst = (
            f"water_sanitation_model/unimproved_drinking_water_{self.country_level}.nc"
        )
        return FinalTarget(path=dst, task=self)

    def read_rasterio(self, src_file, name=None, month=None):
        da = xr.open_rasterio(src_file)
        try:
            da = da.squeeze(dim="band")
        except ValueError:
            da = da.sel(band=1)
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]
        da = da.rio.clip(geom)
        da = da.where(da != da.attrs["_FillValue"])
        if name:
            da.name = name
        if month:
            da = da.expand_dims({"month": [month]})
        return da

    def read_monthly_raster(self, data_map, name=None):
        ds = xr.concat(
            [self.read_rasterio(v.path, name, k) for k, v in data_map.items()], "month"
        )
        return ds

    def read_pop(self, month):
        input_map = self.input()
        year = pd.to_datetime(month).year
        da = self.read_rasterio(input_map["pop_mean"][year].path, "pop_mean")
        da = da.expand_dims({"month": [month]})
        return da

    def run(self):
        with self.input()[1].open() as src:
            inputs = src.read()

        model = inputs["model"]
        predictors = inputs["preds"]

        y_list = []
        df = dd.read_parquet(self.input()[0].path)
        if self.country_level in ["Ethiopia", "South Sudan"]:
            df = df.repartition(npartitions=10)
        for part in df.partitions:
            part = part.compute()
            part = dask.delayed(self.data_preprocessing)(part)

            y_part = dask.delayed(self.prediction)(part, predictors, model)
            y_part = y_part.compute()
            y_list.append(y_part)

        y = xr.concat(y_list, dim="z")
        y = y.sel(z=~y.get_index("z").duplicated())
        y = y.unstack("z")
        y.name = "unimproved_drinking_water"
        with self.output().open("w") as out:
            y.to_netcdf(out.name)

    def data_preprocessing(self, df):
        df = df.reset_index()
        df["year"] = df["month"].dt.year
        df = df.set_index(["month", "x", "y"])
        # Apply scenario
        df["travel_time_mean"] = df["travel_time_mean"] * (
            1 + self.travel_time_percent_change
        )
        df["aridity_index_mean"] = df["aridity_index_mean"] * (
            1 + self.aridity_index_percent_change
        )
        df["ntl_mean"] = df["ntl_mean"] * (1 + self.night_light_percent_change)

        if self.smod_offset != 0:
            df["ghs_smod"] = df["ghs_smod"].apply(lambda x: self.apply_smod_offset(x))

        var_list = set(df.columns) - {"band", "spatial_ref"}
        for i in var_list:
            df[f"{i}_log"] = np.log(df[i])
            df[f"{i}_square"] = np.square(df[i])
            df[f"{i}_sqrt"] = np.sqrt(df[i])
        return df

    def prediction(self, part, predictors, model):
        X = part[predictors]
        X = X.replace([np.inf, -np.inf], np.nan)
        mask = ~X.isna().any(axis=1)
        y = pd.Series(index=X.index)
        y.loc[mask] = model.predict(X.loc[mask])
        y[y > 1] = 1
        y[y < 0] = 0
        y = y.to_xarray()
        y = y.stack(z=("month", "x", "y"))
        return y


class PullPopulationData(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "dbe49118-c859-478a-9000-74c9487397b2"},
            resource={"id": "42ee2f3c-5dd5-4240-bb1e-47dea03a943e"},
        )


@requires(PullPopulationData)
@inherits(GlobalParameters)
class MaskPopulationData(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        da = xr.open_rasterio(self.input().path)
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]
        da = da.rio.clip(geom)
        with self.output().open("w") as out:
            out.write(da)


@requires(
    PredictUnimprovedToiletFacility, PredictUnimprovedDrinkingWater, MaskPopulationData
)
class PredictUnimprovedSanitation(Task):
    def output(self):
        dst = f"water_sanitation_model/unimproved_sanitation_{self.country_level}.nc"
        return FinalTarget(path=dst, task=self)

    def run(self):
        da_fac = xr.open_dataset(self.input()[0].path)
        da_fac = da_fac.rio.write_crs(4326)
        da_water = xr.open_dataset(self.input()[1].path)
        da_water = da_water.rio.write_crs(4326)
        with self.input()[2].open() as src:
            pop_da = src.read()
        pop_da = pop_da.where(pop_da != pop_da._FillValue).sel(band=1, drop=True)
        pop_da = pop_da.to_dataset(name="population_count")
        pop_da = pop_da.rio.reproject_match(da_water)

        pop_ds = xr.concat(
            [pop_da.expand_dims({"month": [i]}) for i in da_water["month"].values],
            dim="month",
        )

        ds = xr.merge([da_water, da_fac, pop_ds])
        ds = ds.assign(
            unimproved_drinking_water_population=ds["unimproved_drinking_water"]
            * ds["population_count"]
        )
        ds = ds.assign(
            unimproved_sanitation_population=ds["unimproved_sanitation"]
            * ds["population_count"]
        )

        ds["unimproved_drinking_water_population"] = ds[
            "unimproved_drinking_water_population"
        ].round(0)
        ds["unimproved_sanitation_population"] = ds[
            "unimproved_sanitation_population"
        ].round(0)

        with self.output().open("w") as out:
            ds.to_netcdf(out.name)


@requires(PredictUnimprovedToiletFacility)
class UnimprovedSanitationRaster(Task):
    def get_dst(self, month):
        return f"water_sanitation_model/unimproved_sanitation_{self.country_level}_{month.strftime('%y_%m')}.tif"

    def output(self):
        month_list = pd.date_range(
            datetime.date(2012, 4, 1), datetime.date(2021, 2, 1), freq="M"
        )  # Hard coded date
        return {i: FinalTarget(path=self.get_dst(i), task=self) for i in month_list}

    def run(self):
        with self.input().open() as src:
            src_byte = src.read()
        da = xr.open_dataarray(BytesIO(src_byte))
        da = da.astype("float32")
        da = da.fillna(-9999.0)
        affine = da.rio.transform()
        transform = Affine(
            affine.a,
            affine.b,
            affine.c,
            affine.d,
            -1 * affine.e,
            affine.f + (affine.e * (da["y"].size - 1)),
        )
        meta = dict(
            driver="GTiff",
            dtype="float32",
            nodata=-9999.0,
            count=1,
            crs="EPSG:4326",
            transform=transform,
            width=da.rio.width,
            height=da.rio.height,
        )

        for month, target in self.output().items():
            temp = da.sel(month=month)
            temp = temp.transpose("y", "x")
            with target.open("w") as out:
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write(np.flipud(temp.values), 1)


@requires(PredictUnimprovedDrinkingWater)
class UnimprovedDrinkingWaterRaster(UnimprovedSanitationRaster):
    def get_dst(self, month):
        return f"water_sanitation_model/unimproved_water_{self.country_level}_{month.strftime('%y_%m')}.tif"

    def output(self):
        month_list = pd.date_range(
            datetime.date(2012, 4, 1), datetime.date(2021, 2, 1), freq="M"
        )  # Hard coded date
        return {i: FinalTarget(path=self.get_dst(i), task=self) for i in month_list}


@inherits(GlobalParameters)
class PullAdminShapefile(ExternalTask):
    def output(self):
        if self.country_level == "Ethiopia":
            return CkanTarget(
                dataset={"id": "d07b30c6-4909-43fa-914b-b3b435bef314"},
                resource={"id": "d4804e8a-5146-48cd-8a36-557f981b073c"},
            )
        elif self.country_level == "South Sudan":
            return CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "816e8cfc-ac8f-4afb-8d0c-7641d9c3e944"},
            )
        elif self.country_level == "Djibouti":
            return CkanTarget(
                dataset={"id": "cf5f891c-6fec-4509-9e3a-0ac69bb1b6e7"},
                resource={"id": "412a2c5b-cee2-4e44-bd2a-2d8511834c8e"},
            )
        elif self.country_level == "Kenya":
            return CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "9da31938-86a7-43d8-947a-c8cdc9202ae4"},
            )
        elif self.country_level == "Uganda":
            return CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "01038e6c-e900-4e22-a027-fa05f593658c"},
            )
        else:
            raise NotImplementedError


@requires(UnimprovedSanitationRaster, PullAdminShapefile)
class UnimprovedSanitationGeojson(Task):
    variable = luigi.Parameter(default="unimproved_sanitaion")

    def output(self):
        return FinalTarget(
            path=f"water_sanitation_model/unimproved_sanitation_{self.country_level}.geojson",
            task=self,
        )

    @staticmethod
    def get_mean(gdf, src_file, month):
        temp = zonal_stats(
            gdf, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        temp = gpd.GeoDataFrame.from_features(temp)
        temp["month"] = datetime.date(month.year, month.month, 1).strftime("%Y-%m-%d")
        return temp

    @staticmethod
    def get_schema(gdf):
        schema = gpd.io.file.infer_schema(gdf)
        schema["properties"]["month"] = "datetime"
        return schema

    def run(self):
        raster_map = self.input()[0]
        gdf = gpd.read_file(f"zip://{self.input()[1].path}")
        out_df = pd.concat(
            [self.get_mean(gdf, v.path, k) for k, v in raster_map.items()]
        )
        out_df = out_df.rename(columns={"mean": self.variable})
        out_df["month"] = pd.to_datetime(out_df["month"])
        schema = self.get_schema(out_df)
        with self.output().open("w") as out:
            out_df.to_file(out.name, driver="GeoJSON", schema=schema, index=False)


@requires(UnimprovedDrinkingWaterRaster, PullAdminShapefile)
class UnimprovedDrinkingWaterGeojson(UnimprovedSanitationGeojson):
    variable = luigi.Parameter(default="unimproved_drinking_water")

    def output(self):
        return FinalTarget(
            path=f"water_sanitation_model/unimproved_drinking_water_{self.country_level}.geojson",
            task=self,
        )
