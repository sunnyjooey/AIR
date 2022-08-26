import os
from functools import reduce
import json
from pathlib import Path

import luigi
from luigi.util import requires
from kiluigi.targets import IntermediateTarget, FinalTarget, CkanTarget
from kiluigi.tasks import ExternalTask, Task, ReadDataFrameTask, VariableInfluenceMixin
import numpy as np
import pandas as pd
import xarray as xr
import rasterio
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import PowerTransformer

# from sklearn.ensemble import ExtraTreesRegressor as Estimator
from sklearn.linear_model import LinearRegression as Estimator
from sklearn.pipeline import Pipeline

from utils.geospatial_tasks.functions.geospatial import (
    convert_multiband_raster_to_csv_points,
    downsample_raster,
)
from utils.geospatial_tasks.tasks import (
    RasterizeAdminFeatures,
    GetAdminFeatures,
)
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters
from utils.scenario_tasks.functions.geography import MaskDataToGeography
from utils.visualization_tasks.push_csv_to_database import PushCSVToDatabase
from ..demand_model.mappings import county_code_to_rasterize, state_code_to_rasterize
from .utils import grid
from .x_data_prep import GetRasterData

# from .x_data_prep import MergeRasterEconomicClusters


# -------------------------
# Pull Data and Train Model
# -------------------------


@requires(GlobalParameters)
class PullTrainingData(ExternalTask):
    def output(self):

        # TODO: eliminate with geo parameter standardization
        if self.country_level == "Ethiopia":
            iso3166 = "eth"
        if self.country_level == "South Sudan":
            iso3166 = "ssd"

        # TODO: country = self.geography["id"]
        country = iso3166
        if country == "eth":
            target = CkanTarget(
                dataset={"id": "0bcd76ef-6cae-4ad2-9de4-7895f7326f27"},
                resource={"id": "14679963-008b-47cc-8551-16db0acd17a8"},
            )
        elif country == "ssd":
            target = CkanTarget(
                dataset={"id": "8399cfb4-fb5f-4e71-93a9-db77ce3b74a4"},
                resource={"id": "cd736b2b-c12e-45e0-80b2-df5f84eafc7b"},
            )
        else:
            raise NotImplementedError
        return target


@requires(PullTrainingData)
class ReadTrainingData(ReadDataFrameTask):

    read_method = "read_csv"
    read_args = {"index_col": 0}


# @requires(MergeRasterEconomicClusters)
@requires(ReadTrainingData)
class CleanHouseholdEconData(Task):

    y_var = luigi.Parameter(default="consumption_expenditure", significant=False)

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        with self.input().open("r") as f:
            df = f.read()
        df = df.dropna(subset=[self.y_var])
        df = df.drop(df[df[self.y_var] > 200].index)
        # FIXME: pipeline is missing "date" for Ethiopia
        if self.country_level == "Ethiopia":
            df["month"] = 12
        else:
            try:
                df["month"] = df["date"].dt.month
            except KeyError:
                df["month"] = 12
        # TODO: what is the dropping of waves about?
        # df = df[df["wave"] < 3].copy()
        with self.output().open("w") as output:
            output.write(df)


@requires(CleanHouseholdEconData)
class Train(Task, VariableInfluenceMixin):
    """
    Train the household economic model on cluster data.
    """

    x_vars = luigi.ListParameter(
        default=["accessibility", "night_lights", "population", "landcover", "month"],
        significant=False,
    )
    calculate_influence = luigi.BoolParameter(default=False, significant=False)
    grid_search = luigi.BoolParameter(default=False, significant=False)

    def output(self):
        output = {"model": IntermediateTarget(task=self)}
        if self.calculate_influence:
            output["influence_grid"] = self.get_influence_grid_target()
        return output

    def run(self):

        # read training data and targets
        with self.input().open("r") as f:
            df = f.read()
        x = df.loc[:, self.x_vars]
        y = df.loc[:, self.y_var]

        # create a pipeline with tunable preprocessor and estimator
        preprocessor = ColumnTransformer(
            [
                (
                    "numeric",
                    PowerTransformer(),
                    list(set(self.x_vars) - {"landcover", "month"}),
                ),
                # TODO: OneHot is a poor method for handling categorical
                #       features in CARTs. Scikit-learn has not implemented
                #       a good way (see:
                #       https://github.com/scikit-learn/scikit-learn/issues/12398)
                (
                    "unordered",
                    OneHotEncoder(categories="auto", handle_unknown="ignore"),
                    ["landcover", "month"],
                ),
            ]
        )

        # If the chosen estimator does not yield an oob_score, then use
        # GridSearchCV rather than the `max` statement that follows.
        # estimator = Estimator(bootstrap=True, oob_score=True)
        estimator = TransformedTargetRegressor(Estimator(), PowerTransformer())
        pipeline = Pipeline([("preprocessor", preprocessor), ("estimator", estimator)])

        # fit the pipeline, with optional tuning using a grid search
        if self.grid_search is True:
            parameters = {
                "estimator__n_estimators": [30, 50, 70, 90],
                "estimator__criterion": ["mse"],
                "estimator__max_depth": [None],
                "estimator__min_samples_split": [2, 4, 8, 16],
                "estimator__min_samples_leaf": [1, 3, 5, 7],
                "estimator__min_weight_fraction_leaf": [0.0],
                "estimator__max_features": ["auto"],
                "estimator__max_leaf_nodes": [None],
                "estimator__min_impurity_decrease": [0.0],
            }
            pipeline = max(
                # TODO: alternative for non-bagging algorithms
                grid(pipeline, x, y, parameters),
                key=lambda x: x["estimator"].oob_score_,
            )
        else:
            # TODO: Scenario specific hyper-parameters should probably not be
            #       baked in, but not sure where to store, if even necessary.
            parameters = {}
            pipeline.set_params(**parameters)
            pipeline.fit(x, y)

        # Calculate the influence grid if needed
        if self.calculate_influence is True:
            model_importance = pipeline["estimator"].feature_importances_
            # TODO: pipeline['preprocessor'].get_feature_names() is not
            #       implemented for PowerTransformer (PR?)
            influence_grid = pd.DataFrame(
                [model_importance, list(set(self.x_vars) - {"landcover"})],
                index=["influence_percent", "source_variable"],
                columns=np.arange(0, len(model_importance)),
            ).T
            self.store_influence_grid(self.y_var, influence_grid)

        with self.output()["model"].open("w") as output:
            output.write(pipeline)


# -------------------------
# Predict for Gridded Input
# -------------------------


@requires(Train, GetRasterData)
class Predict(Task):
    def output(self):
        return IntermediateTarget(task=self)

    def run(self):

        with self.input()[0]["model"].open("r") as f:
            model = f.read()

        with self.input()[1].open("r") as f:
            x = f.read()
        profile = next(v for v in x.variables.values() if v.attrs).attrs

        # interpolate time-series
        # FIXME: can surely do better
        x = x.bfill("period")
        # convert to dataframe
        # TODO: use sklearn with xarray without `to_dataframe`
        x = x.to_dataframe()
        # breakpoint()
        # drop NaN values to execute the model
        mask = ~x.isna().any(axis=1)

        # add variable derived from index
        x.loc[mask, "month"] = x.index[mask].get_level_values("period").month

        # generate model predictions for novel data
        y = pd.Series(index=x.index)
        y.loc[mask] = model.predict(x.loc[mask])

        # cast back to multi-dimensional array
        y = y.to_xarray()
        y = y.drop_vars(["x", "y"])

        # clip to zero
        y = y.clip(0, None)

        # write tiles
        profile["dtype"] = np.dtype(np.float)
        profile["driver"] = "GTiff"
        profile["width"] = y.sizes["x"]
        profile["height"] = y.sizes["y"]
        profile["count"] = 1
        for k in [
            "res",
            "is_tiled",
            "scales",
            "offsets",
            "AREA_OR_POINT",
            "nodatavals",
        ]:
            profile.pop(k)
        ds = []
        parent = Path(
            IntermediateTarget(path=self.task_id.replace(".", "/") + "/").path
        )
        parent.mkdir(exist_ok=True, parents=True)
        for period in y["period"]:
            target = IntermediateTarget(
                path=str(
                    Path(*self.task_id.split("."), str(period.data)).with_suffix(
                        ".tiff"
                    )
                ),
            )
            # write geotiff files
            with rasterio.open(target.path, "w", **profile) as dst:
                dst.write(y.loc[{"period": period}].data.T, 1)

            # open using `chunks` to create dask array (no data read)
            da = (
                xr.open_rasterio(target.path, parse_coordinates=False, chunks={})
                .sel(band=1)
                .drop("band")
            )
            da.name = self.y_var
            ds.append(
                xr.merge(
                    [da, xr.DataArray(data=target.path, name="tiles")]
                ).assign_coords({"period": period})
            )

        # write to pickle
        ds = xr.concat(ds, "period").sortby("period")
        with self.output().open("w") as f:
            f.write(ds)


# -----------------------------------------------
# Aggregate Prediction to Administrative Features
# -----------------------------------------------


@requires(Predict, GetAdminFeatures)
class Rasterize(RasterizeAdminFeatures):
    """burn administrative boundaries into raster matching model prediction"""

    def run(self):

        # read reference projection
        with self.input()[0].open() as f:
            ds = f.read()

        da = self.rasterize(ds[self.y_var])

        with self.output().open("w") as f:
            f.write(da)


@requires(Predict, Rasterize)
class Aggregate(Task):
    def output(self):
        return IntermediateTarget(task=self)

    def run(self):

        with self.input()[0].open() as f:
            y = f.read()[self.y_var]

        with self.input()[1].open() as f:
            ds = f.read()

        mask = ds["index"] == ds["zone"]
        # FIXME: this is not correct! needs weighting by population!
        da = (y * mask).mean(dim=("x", "y"))
        da.name = y.name
        da.load()

        # merge with zone and drop index
        ds = xr.merge((ds, da)).drop_vars(("zone", "index"))

        with self.output().open("w") as f:
            f.write(ds)


# --------------------------------
# TODO: evaluate below for discard
# --------------------------------


# read reference projection metadata
with open("models/economic_model/data/raster_proj.json", "r") as json_data:
    REF_PROJ = json.load(json_data)


@requires(GetRasterData)
class ConvertRastersToTable(Task):
    def output(self):

        return IntermediateTarget(task=self)

    def run(self):

        model = self.clone(Train)
        with model.output()["model"].open() as f:
            model = f.read()

        with self.input().open() as f:
            ds = f.read()

        # interpolate time-series
        # FIXME: can surely do better
        ds = ds.bfill("period")

        # apply the model fit
        df = ds.to_dataframe()

        # y = model.predict(df)  # unused?

        # Read values from all pixels at each time period
        dfs = []
        for period, target in self.input().items():
            try:
                with rasterio.open(target.path) as src:
                    profile = src.profile
                    df = pd.DataFrame(
                        index=pd.MultiIndex.from_product(
                            [
                                [period.to_timestamp()],  # FIXME: period is borken
                                range(src.width),
                                range(src.height),
                            ],
                            names=["period", "row", "col"],
                            #  TODO: confirm dim order with reshape below
                        ),
                        data=src.read().reshape((src.count, -1)).transpose(),
                        columns=[src.tags(i + 1)["variable"] for i in range(src.count)],
                    )
            except rasterio.errors.RasterioIOError:  # TODO-req-files
                continue
            dfs.append(df)
        df = pd.concat(dfs, sort=True)

        with self.output().open("w") as output:
            output.write({"data": df, "metadata": profile})


# @requires(Combine)
class MaskResults(GlobalParameters, MaskDataToGeography):
    def complete(self):
        return super(MaskDataToGeography, self).complete()

    # TODO: the output is an IntermediateTarget within kiluigi, change to a
    #       FinalTarget there?
    # TODO: all other uses of GlobalParameters have been as a requires,
    #       why is this different?


@requires(GetAdminFeatures, ConvertRastersToTable)
class AdminRaster(RasterizeAdminFeatures):
    def output(self):
        target_dir = Path(*self.task_id.split("."))
        return IntermediateTarget(path=str(target_dir / "admin.tiff"))


# @requires(RasterizeAdminFeatures, Output)
class ConvertModelResultsToCSV(Task):

    dscale = luigi.FloatParameter(default=0.1)

    def output(self):
        return FinalTarget(path="econ_superset.csv")

    def convert_raster(self, src_fname, admin_df, z):
        """
        Function to downscale, convert to csv of points, and merge with admin names.
        """
        if self.dscale != 1:
            src_fname = downsample_raster(src_fname, dscale=self.dscale)
        df = convert_multiband_raster_to_csv_points(src_fname, drop_na=False)
        df.columns = ["X", "Y", z]
        df = df.merge(admin_df, left_on=["X", "Y"], right_on=["X", "Y"], how="right")
        df = df.replace({-9999.0: np.nan})
        #         Remove downscaled raster
        if self.dscale != 1:
            os.remove(src_fname)

        return df

    def read_excel_map(self, excel_map):
        temp = {}
        for k, v in excel_map.items():
            temp[k] = pd.read_excel(v)
        df = pd.concat(temp)
        return df

    def run(self):
        files = {
            "admin": self.input()[0],
            "econ": self.input()[1][1].path,  # raster filepath for cons exp
        }

        state_admin_fn = downsample_raster(
            files["admin"][0].path, dscale=self.dscale, resampling="mode"
        )
        county_admin_fn = downsample_raster(
            files["admin"][1].path, dscale=self.dscale, resampling="mode"
        )
        state_admin_df = convert_multiband_raster_to_csv_points(
            state_admin_fn, drop_na=True
        )
        state_admin_df.columns = ["X", "Y", "State"]
        county_admin_df = convert_multiband_raster_to_csv_points(
            county_admin_fn, drop_na=True
        )
        county_admin_df.columns = ["X", "Y", "County"]
        admin_df = state_admin_df.merge(
            county_admin_df, left_on=["X", "Y"], right_on=["X", "Y"]
        )

        inverted_dict = {v: k for k, v in state_code_to_rasterize.items()}
        admin_df = admin_df.replace({"State": inverted_dict})
        inverted_dict = {v: k for k, v in county_code_to_rasterize.items()}
        admin_df = admin_df.replace({"County": inverted_dict})
        final_output = {}
        data_files = [f for f in list(files.keys()) if f != "admin"]

        # If this is a baseline run, just output baseline stats
        if self.PercentOfNormalRainfall == 1:

            # Downsample all the rasters and convert to csv
            for k in data_files:
                if type(files[k]) is str:
                    final_output[k] = self.convert_raster(
                        files[k], admin_df, "baseline"
                    )

                elif type(files[k]) is list:
                    final_output[k] = [
                        self.convert_raster(f, admin_df, "baseline") for f in files[k]
                    ]
                elif type(files[k]) is dict:
                    final_output[k] = self.read_excel_map(files[k])

        # If this is a scenario run, merge the scenario with the baseline
        elif self.PercentOfNormalRainfall != 1:

            # Do the scenario
            scenario = {}
            # Downsample all the rasters and convert to csv
            for k in data_files:
                if type(files[k]) is str:
                    scenario[k] = self.convert_raster(files[k], admin_df, "scenario")
                elif type(files[k]) is list:
                    scenario[k] = [
                        self.convert_raster(f, admin_df, "scenario") for f in files[k]
                    ]
                elif type(files[k]) is dict:
                    scenario[k] = self.read_excel_map(files[k])

            # Do the baseline
            baseline = {}
            # Convert the filenames to the baseline filenames
            for k in data_files:
                if type(files[k]) is str:
                    files[k] = (
                        files[k].split(f"_{self.PercentOfNormalRainfall}")[0]
                        + "_1.0.tif"
                    )
                elif type(files[k]) is list:
                    files[k] = [
                        f.split(f"_{self.PercentOfNormalRainfall}")[0] + "_1.0.tif"
                        for f in files[k]
                    ]
                elif type(files[k]) is dict:
                    files[k] = {
                        k: v.split(f"_{self.PercentOfNormalRainfall}")[0] + "_1.0.xlsx"
                        for k, v in files[k].items()
                    }

            # Downsample all the rasters and convert to csv
            for k in data_files:
                if type(files[k]) is str:
                    baseline[k] = self.convert_raster(files[k], admin_df, "baseline")
                elif type(files[k]) is list:
                    baseline[k] = [
                        self.convert_raster(f, admin_df, "baseline") for f in files[k]
                    ]
                elif type(files[k]) is dict:
                    baseline[k] = self.read_excel_map(files[k])

            # Merge baseline and scenario
            for k in data_files:
                if type(baseline[k]) is list:
                    final_output[k] = [None] * len(baseline[k])
                    for i in range(len(baseline[k])):
                        colname = (
                            files["prices"][i].split("_rasterized")[0].split("/")[-1]
                        )
                        baseline[k][i] = baseline[k][i].rename(
                            columns={"baseline": f"{colname}_baseline"}
                        )
                        scenario[k][i] = scenario[k][i].rename(
                            columns={"scenario": f"{colname}_scenario"}
                        )
                        final_output[k][i] = baseline[k][i].merge(
                            scenario[k][i],
                            left_on=["X", "Y", "State", "County"],
                            right_on=["X", "Y", "State", "County"],
                        )
                elif k == "crop":
                    final_output[k] = pd.concat([baseline[k], scenario[k]])
                else:
                    final_output[k] = baseline[k].merge(
                        scenario[k],
                        left_on=["X", "Y", "State", "County"],
                        right_on=["X", "Y", "State", "County"],
                    )

        # Merge lists into one dataframe
        for k in final_output.keys():
            if type(final_output[k]) is list:
                final_output[k] = reduce(
                    lambda x, y: pd.merge(x, y, on=["X", "Y", "State", "County"]),
                    final_output[k],
                )

        #         Merge econ and needs, rename columns
        final_output["econ"] = final_output["econ"].rename(
            columns={
                "baseline": "consumption_expenditure_baseline",
                "scenario": "consumption_expenditure_scenario",
            }
        )

        for k in final_output.keys():
            final_output[k] = final_output[k].set_index("State")

        with self.output().open("w") as f:
            final_output["econ"].to_csv(f)


@requires(ConvertModelResultsToCSV)
class PushModelResults(PushCSVToDatabase):
    pass
