"""The demand model is trained on data from the High Frequency World Bank Survey to
estimate consumption shares based on market prices and consumption expenditure.
The model is then run on the output of the Market Price and Household Economic models
to estimate consumption shares across a raster file."""

import json
import numpy as np
import os
import pandas as pd
import rasterio
import subprocess  # noqa: S404

import luigi
from luigi import Task
from luigi.configuration import get_config
from luigi.util import requires
from rasterio.mask import mask
from rasterio.features import shapes
import geojson

from models.demand_model.mappings import food_groupings, food_groupings_calories

from kiluigi.targets import IntermediateTarget, FinalTarget

from models.economic_model.x_data_prep import CalculateConsumptionExpenditure
from models.economic_model.output import Combine as OutputEconomicModelResults
from models.market_price_model.tasks import RasterizePrices, ReferenceRaster


CONFIG = get_config()


DEMAND_OUTPUT_PATH = os.path.join(CONFIG.get("paths", "output_path"), "demand_model")
# Create output path if it doesn't exist
os.makedirs(DEMAND_OUTPUT_PATH, exist_ok=True)

with open("models/economic_model/data/raster_proj.json", "r") as json_data:
    REF_PROJ = json.load(json_data)


@requires(CalculateConsumptionExpenditure)
class CreateAidsModelInput(Task):
    """
    Define how you want to aggregate the food groups and create input for AIDS model.
    """

    # TODO: task does not have GlobalParameters, should it?

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 5)

    def run(self):
        with self.input().open("r") as f:
            data = f.read()

        df = data["food"]
        total_df = data["consumption_expenditure"]

        # Get weightings for each food group
        df = df.merge(
            total_df[["total_expenditure"]],
            left_on="hhid",
            right_index=True,
            how="left",
        )

        df["w"] = df["expenditure"] / df["total_expenditure"]
        df["p"] = df["price_usd_per_kg"]
        #        df['q'] = df['cons_quant_kg'].copy()
        df_weights = df.pivot(index="hhid", values=["w", "p"], columns="food_groups")
        df_weights.columns = ["".join(col).strip() for col in df_weights.columns.values]

        # Merge weights with the total dataframe
        output_df = total_df.merge(df_weights, left_on="hhid", right_on="hhid")
        output_df = output_df.rename(columns={"total_expenditure": "xFood"})

        # Fill empty or 0 prices with EA or state average
        food_group_list = [
            "p" + x for x in list(food_groupings.values()) if x is not np.nan
        ]
        for i in food_group_list:
            # First replace 0 price with NA
            output_df = output_df.replace({i: {0: np.nan}})

            # Replace any values missing values with EA average
            output_df[i] = output_df.groupby(["wave", "state", "ea"]).transform(
                lambda x: x.fillna(x.mean())
            )[i]

            # If there are still missing values, replace with the state average
            output_df[i] = output_df.groupby(["wave", "state"]).transform(
                lambda x: x.fillna(x.mean())
            )[i]

            # If there are still missing values, replace with the whole wave average
            output_df[i] = output_df.groupby(["wave"]).transform(
                lambda x: x.fillna(x.mean())
            )[i]

        # Fill NaN vales with 0 as is required for AIDS model
        output_df = output_df.fillna(0)

        # Drop all households with 0 expenditure for AIDS model
        output_df = output_df.drop(output_df.loc[output_df["xFood"] == 0].index)

        # Write dataframe out to excel file
        with self.output().open("w") as output:
            output.write(output_df)


@requires(CreateAidsModelInput)
class SpecifyTrainDataAidsModel(Task):
    """
    Select the data to be used in training.
    """

    def output(self):
        return FinalTarget(path="aids_model_train.csv")

    # TODO: revisit how we are training this model, right now we are taking waves 1 & 2
    def run(self):
        with self.input().open("r") as f:
            df = f.read()

        df_train = df[df["wave"] < 2].copy()
        df_train.to_csv(self.output().path)


@requires(CreateAidsModelInput)
class SpecifyTestDataAidsModel(Task):
    """
    Select the data to be used in testing.
    """

    def output(self):
        return FinalTarget(path="aids_model_test.csv")

    def run(self):
        with self.input().open("r") as f:
            df = f.read()

        df_test = df[df["wave"] >= 2].copy()
        df_test.to_csv(self.output().path)


@requires(RasterizePrices, OutputEconomicModelResults)
class AddConsumptionExpenditure(Task):
    """
    Add consumption expenditure as a column to the price data to create input data
    to run the demand model.This takes a long time to run
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=600 * 5)

    def run(self):
        with self.input()[0].open("r") as f:
            priceDf = f.read()
        with rasterio.open(self.input()[1][1].path) as src:
            masked_image, masked_transform = mask(src, [self.geography], crop=True)
            ce = masked_image

        priceDf["xFood"] = ce.flatten()
        with self.output().open("w") as output:
            output.write(priceDf)


@requires(AddConsumptionExpenditure)
class CleanExogenousDemandInput(Task):
    """
    The Demand Model cannot run on values of 0 consumption expenditure,
    so those will be dropped here. Later, the results will be merged back in with the
    full flattened raster that contains 0 values.
    """

    def output(self):
        return FinalTarget(path="demand_model_inputs_clean.csv")

    def run(self):
        # input is actually a dictionary

        with self.input().open("r") as f:
            df = f.read()
        # loop through the key values of df dictionary
        aggreg_df = pd.DataFrame()
        # select the first 4 and leave the last one 'xFood'
        dictionary_len = len(list(df.items())[:-1])
        for i in range(dictionary_len):
            val_df = list(df.items())[i][1]
            val_df["xFood"] = list(df.items())[-1][1]
            val_df.columns = ["p" + x for x in list(val_df.columns) if x != "xFood"] + [
                "xFood"
            ]
            val_df["X"] = list(df.items())[i][0]
            val_df.replace({"xFood": {-9999.0: np.nan}}, inplace=True)
            aggreg_df = aggreg_df.append(val_df)

        df_output = aggreg_df.dropna(subset=["xFood"])
        df_output["raster_idx"] = df_output.index
        df_output.to_csv(self.output().path, index=False)


@requires(
    CleanExogenousDemandInput, SpecifyTrainDataAidsModel, SpecifyTestDataAidsModel
)
class RunAIDSModelR(Task):
    """
    Run the AIDS model in R.
    """

    AIDS_MODEL_SCRIPT = "models/demand_model/simple_AIDS_model.R"
    PATH_TO_R_INSTALL = "Rscript"  # "/usr/local/bin/Rscript"  #

    def output(self):
        return FinalTarget(path="AIDS_results.csv")

    def run(self):
        proc = subprocess.Popen(  # noqa: S603, S607 - ignore security check
            [self.PATH_TO_R_INSTALL, self.AIDS_MODEL_SCRIPT],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                "Execution of '%s' failed: %d %s %s"
                % (self.AIDS_MODEL_SCRIPT, proc.returncode, stdout, stderr)
            )


@requires(RunAIDSModelR, AddConsumptionExpenditure)
class MergeAidsModelResultsWithFullRaster(Task):
    """
    Get the AIDS model results and merge with the full, flattened raster.
    """

    def output(self):

        # return FinalTarget(
        #   f"demand_model_output_flattened_{self.PercentOfNormalRainfall}.csv", task=self,
        # )
        return IntermediateTarget(path=f"{self.task_id}/", timeout=360 * 50)

    def run(self):
        results = pd.read_csv(self.input()[0].path)
        results.set_index("raster_idx", drop=True, inplace=True)

        with self.input()[1].open("r") as f:
            ref_raster_flattened = f.read()

        aggreg_ref_df = pd.DataFrame()
        val_df = list(ref_raster_flattened.items())[0][1]
        val_df["xFood"] = list(ref_raster_flattened.items())[-1][1]
        val_df.columns = ["p" + x for x in list(val_df.columns) if x != "xFood"] + [
            "xFood"
        ]
        val_df["X"] = list(ref_raster_flattened.items())[0][0]
        val_df.replace({"xFood": {-9999.0: np.nan}}, inplace=True)
        aggreg_ref_df = aggreg_ref_df.append(val_df)  # the index matches the raster_idx

        # Merge the AIDS Model results to the ref df making sure to preserve the
        # indices.

        raster_ref_df = aggreg_ref_df[["xFood"]].copy()
        del aggreg_ref_df
        # merge for each month... del intermediate dataframe at the end to save memory
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            month_ls = results.X.unique().tolist()

            for month_tp in month_ls:
                month_result = results[results["X"] == month_tp]
                df = raster_ref_df.merge(
                    month_result, how="left", left_index=True, right_index=True
                )
                csv_outname = os.path.join(
                    tmpdir,
                    f"demand_model_output_flattened_{month_tp}_{self.PercentOfNormalRainfall}.csv",
                )

                df.replace({0: np.nan}, inplace=True)
                df.to_csv(csv_outname, index=False)
                del df
                del month_result


@requires(MergeAidsModelResultsWithFullRaster, ReferenceRaster)
class CreateCaloricNeedsSurface(Task):
    """
    Apply a recommended daily caloric intake (default 2100 cal) and
    generate a per adult daily "caloric deficit" (needs) surface.
    """

    caloric_threshold = luigi.FloatParameter(
        default=2100, description="Caloric threshold in calories"
    )

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=360 * 20, task=self)

    def run(self):
        merged_df_ls = os.listdir(self.input()[0].path)

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)

            for merged_df in merged_df_ls:
                df_fname = os.path.join(self.input()[0].path, merged_df)
                df = pd.read_csv(df_fname)

                # Remap (rename) the columns which seem to have been changed by the
                # AIDS model run. This is a short-term fix.
                # @TODO: Find out exactly where the columns are being renamed and fix it.
                inline_mapping = {
                    "qBread.and.Cereals": "qBread and Cereals",
                    "qMeat": "qMeat",
                    "qMilk..cheese.and.eggs": "qMilk, cheese and eggs",
                    "qOils.and.fats": "qOils and fats",
                    "qPulses.and.vegetables": "qPulses and vegetables",
                    "qSugar..jam..honey..chocolate.and.candy": "qSugar, jam, honey, chocolate and candy",
                }
                df.rename(columns=inline_mapping, inplace=True)

                df["total_calories"] = 0
                for food_group in food_groupings_calories:
                    # Calculate the equivalent total calories that an adult person could afford.
                    # Need to multiply the food groupings calories by 1000 to convert from kg to grams  equivalent
                    # because the q* columns are also in kg.
                    df[f"c{food_group}"] = (
                        1000
                        * food_groupings_calories[food_group]
                        * df[f"q{food_group}"]
                    )
                    # Update the cumulative total calories so far.
                    df["total_calories"] = df["total_calories"] + df[f"c{food_group}"]

                # Calculate the caloric deficit
                df["caloric_deficit"] = df["total_calories"] - self.caloric_threshold

                # parse the month
                month_val = df_fname.split("/")[-1].split(".")[0].split("_")[-2]
                raster_out_fname = os.path.join(
                    tmpdir,
                    f"caloric_needs_{self.caloric_threshold}_{month_val}_{self.PercentOfNormalRainfall}.tif",
                )
                with rasterio.open(self.input()[1].path) as ref_raster:
                    meta = ref_raster.meta.copy()
                    # crop to ensure consiste array shape
                    masked_ref, masked_transform = mask(
                        ref_raster, [self.geography], crop=True
                    )
                    shape = (masked_ref.shape[1], masked_ref.shape[2])

                    meta.update(
                        {
                            "driver": "GTiff",
                            "height": masked_ref.shape[1],
                            "width": masked_ref.shape[2],
                            "transform": masked_transform,
                            "nodata": -9999,
                        }
                    )
                with rasterio.open(raster_out_fname, "w", **meta) as out_raster:
                    caloric_array = df["caloric_deficit"].values.reshape(shape)
                    out_raster.write(caloric_array.astype(rasterio.float32), 1)


@requires(CreateCaloricNeedsSurface)
class CaloricNeedsGeojson(Task):
    """
    takes the caloric deficit raster from previous task and converts it to geojson format
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=360 * 20, task=self)

    def run(self):
        # Open and read raster file with caloric deficit estimates
        raster_ls = os.listdir(self.input().path)
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)

            for raster_file in raster_ls:
                raster_fname = os.path.join(self.input().path, raster_file)
                split_name = raster_fname.split("/")[-1].split(".tif")[0]

                with rasterio.open(raster_fname) as caloric_def_raster:
                    caloric_def_arr = caloric_def_raster.read(1)
                    results = (
                        {"properties": {"caloric_deficit": v}, "geometry": s}
                        for i, (s, v) in enumerate(
                            shapes(
                                caloric_def_arr,
                                mask=None,
                                transform=caloric_def_raster.transform,
                            )
                        )
                    )

                out_fname = os.path.join(tmpdir, f"{split_name}.geojson")

                with open(out_fname, "w") as outfile:
                    geojson.dump(list(results), outfile, allow_nan=True)
