import os
from pathlib import Path

import luigi
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from kiluigi.tasks import ExternalTask, ReadDataFrameTask, Task
from luigi import LocalTarget
from luigi.configuration import get_config
from luigi.date_interval import Custom as DateInterval
from luigi.util import inherits, requires
from rasterio.warp import Resampling, reproject

from utils.geospatial_tasks.functions.geospatial import (
    buffer_points,
    geography_f_country,
)
from utils.geospatial_tasks.functions.remote_sensing_functs import calc_cluster_data
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from ..accessibility_model.tasks import TravelTimeGeotiff
from ..population_model.tasks import HiResPopRasterMasked
from .y_data_prep import CalculateConsumptionExpenditure

CONFIG = get_config()


# ---------------------------------------
# Get All Input Data Formatted and Joined
# ---------------------------------------
class PullRasterDataCkan(ExternalTask):  # tags='ingestion'
    """
    Description:
        Pull the raster data for independent variables.
    Inputs:
        accessibility (Raster): the accessibility data from the`Malaria Atlas Project
        <https://map.ox.ac.uk/wp-content/uploads/accessibility/
        accessibility_to_cities_2015_v1.0.zip>`_
        night_lights (Raster): the night lights data from `NOAA
        <https://data.ngdc.noaa.gov/instruments/remote-sensing/passive/
        spectrometers-radiometers/imaging/viirs/dnb_composites/v10/2015/>`_
        landcover (Raster): the landcover data from `USGS
        <https://landcover.usgs.gov/global_climatology.php>`_
        population_2015 (Raster): the population data from the population model
        population_2016 (Raster): the population data from the population model
        population_2017 (Raster): the population data from the population model
    Outputs:
        Raster Filepaths (Dictionary of CkanTargets): local filepaths to all data
        pulled in from CKAN server.
    User Defined Parameters:
    References:
    Methods:
    """

    def output(self):
        return {
            "night_lights": CkanTarget(
                dataset={"id": "83a07464-6aa1-4611-8e51-d2e04c4a7b97"},
                resource={"id": "07b859ae-cdea-4f7b-99a6-4d48a952f0a6"},
            ),
            "landcover": CkanTarget(
                dataset={"id": "83a07464-6aa1-4611-8e51-d2e04c4a7b97"},
                resource={"id": "5712155e-5321-4f01-9eab-0de727af53be"},
            ),
        }


@requires(
    PullRasterDataCkan, HiResPopRasterMasked, TravelTimeGeotiff,
)
class GetRasterData(Task):
    """
    Incorporate modeled data directly from where it was uploaded to CKAN.
    Accessibility is output from the accessibility model, add it to the raster
    data from CKAN. Population is likewise output from the population model.
    """

    # TODO: eventually, frequency should be set at highest frequency among
    #    model inputs, probably. same approach as spatial resolution, at any
    #    rate
    freq = luigi.ChoiceParameter(choices=["Y", "M", "D"], default="M",)

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):

        # combine inputs into one dictionary
        inputs = self.input()[0]
        inputs.update({"population": self.input()[1], "accessibility": self.input()[2]})

        # TODO: work on getting other model outputs to be file-like
        #       targets, not folder-like. making this mess unnecessary.
        for variable, target in inputs.items():
            periods = {}
            for path in Path(target.path).rglob("*"):
                with rasterio.open(path) as src:
                    date = src.tags().get("Time", None)
                if not date:
                    date = path.name.split("_").pop(0)
                    if len(date) == 6:
                        date += "01"
                output = IntermediateTarget(
                    path=str(path.relative_to(target.root_path))
                )
                periods[pd.Period(date).asfreq("M")] = output
            if periods:
                inputs[variable] = periods

        # reset frequencies to model's output frequency
        for variable, targets in inputs.items():
            try:
                targets = {k.asfreq(self.freq): v for k, v in targets.items()}
            except AttributeError:
                targets = {pd.Period(self.time.date_b, self.freq): targets}
            inputs[variable] = targets

        # geospatial transform where necessary
        # TODO: better way to determine output profile, rather than just
        #  copying from one upstream target (population model)
        ref_src = next(v for v in inputs["population"].values())
        ref_keys = [
            "width",
            "height",
            "crs",
            "transform",
            "dtype",
            "nodata",
        ]
        with rasterio.open(ref_src.path) as src:
            ref_profile = {k: src.profile[k] for k in ref_keys}
        ref_profile["dtype"] = np.dtype(np.float)
        ref_profile["nodata"] = np.nan
        for variable, targets in inputs.items():
            for period, src_target in targets.items():
                with rasterio.open(src_target.path) as src:
                    if all(src.profile[k] == ref_profile[k] for k in ref_keys):
                        continue
                    profile = src.profile
                    profile.update(ref_profile)
                    name = f"{variable}_{period}.tiff"
                    parent = Path(self.output().path).with_suffix("")
                    parent.mkdir(parents=True, exist_ok=True)
                    path = parent.relative_to(self.output().root_path)
                    dst_target = IntermediateTarget(path=str(path / name))
                    with rasterio.open(dst_target.path, "w", **profile) as dst:
                        # TODO: how does this function handle nan and nodatavals
                        #       in src?
                        # FIXME: assumes each input is univariate
                        reproject(
                            source=rasterio.band(src, 1),
                            destination=rasterio.band(dst, 1),
                            resampling=Resampling.nearest,
                        )
                    targets[period] = dst_target

        # collect homogenized variables in xarray Dataset
        ds = [
            xr.Dataset(
                coords={
                    "period": pd.period_range(
                        self.time.date_a, self.time.date_b, freq=self.freq
                    )
                },
            )
        ]
        for variable, targets in inputs.items():
            da = []
            for period, target in targets.items():
                da_period = xr.open_rasterio(
                    target.path, parse_coordinates=False, chunks={},
                )
                da_period.name = variable
                da_period["period"] = period
                da_period = da_period.isel(band=0)
                da.append(da_period)
            da = xr.concat(da, "period")
            ds.append(da)
        ds = xr.merge(ds)

        with self.output().open("w") as f:
            f.write(ds)


@requires(CalculateConsumptionExpenditure,)
@inherits(GlobalParameters)
class GetRasterTrainingData(Task):
    """
    Identify additional data not pulled by `GetRasterData`, required to fit
    against the training data.
    """

    # TODO: may not need to be  a separate task if the model eventually
    #       requires time series
    # FIXME: spatial location of raster data should be based on GPS in
    #        training data, so that GlobalParameters are not needed.

    def output(self):
        # return {
        #     "population_2015": CkanTarget(
        #         dataset={"id": "83a07464-6aa1-4611-8e51-d2e04c4a7b97"},
        #         resource={"id": "088ed2c6-586f-4379-bbdd-a56078d8f991"},
        #     ),
        #     "population_2016": CkanTarget(
        #         dataset={"id": "83a07464-6aa1-4611-8e51-d2e04c4a7b97"},
        #         resource={"id": "dae2cd6c-d098-4ff9-880d-a4a6d038b64c"},
        #     ),
        #     "population_2017": CkanTarget(
        #         dataset={"id": "83a07464-6aa1-4611-8e51-d2e04c4a7b97"},
        #         resource={"id": "d6da95ed-2d17-4bb7-be53-8ecb4cefecb0"},
        #     ),
        # }

        return IntermediateTarget(task=self)

    def run(self):

        targets = {}
        with self.input().open("r") as f:
            data = f.read()
        df = data["consumption_expenditure"]
        df["year"] = df["date"].dt.year
        for date in df.groupby("year")["date"].first():

            # the task generating this variable responds to the "time"
            # parameter, but is only dependent on year. date_a is
            # expected to always be the first of the month at present
            date_a = date.date()
            date_b = (date + pd.DateOffset(days=1)).date()
            time = DateInterval(date_a, date_b)
            task = GetRasterData(
                admin_level=self.admin_level,
                country_level=self.country_level,
                time=time,
                geography=geography_f_country(self.country_level),
                # TODO: establish practice that parameter defaults are for
                #       training?
            )
            #  FIXME: something in the task_id construction is borken, which
            #         converting to str and back works around
            task = task.from_str_params(task.to_str_params())

            # schedule necessary Task
            yield task

            # collect outputs
            year = date_a.year
            target = task.output()
            path = next(Path(target.path).rglob(f"{year}*"))
            # target.relative_path = path.relative_to(target.root_path)
            targets[year] = str(path)

        with self.output().open("w") as f:
            f.write(targets)


class PullGPSDataLocal(ExternalTask):  # tags='ingestion'
    """
    Pull the GPS data for the enumeration area clusters. This is private data that
    must be stored in a secure location. For now, that is the analyst's local drive.
    This task will only need to be run when the dependent variables for
    the Household Economic Model are manipulated.

    Inputs:
        gps (DataFrame): the cluster coordinates for enumeration ares in the household
        survey data
    Outputs:
        DataFrame Filepath (Dictionary of LocalTarget): local filepath to the GPS data.
    User Defined Parameters:
    Literary References:
    Methods:
    """

    def output(self):
        return LocalTarget(
            os.path.join(
                CONFIG.get("paths", "output_path"),
                # "models/economic_model/EA_gps_full.csv"
                "economic_model/EA_gps.csv",  # FIXME: confirm same data?
            )
        )


@requires(GetRasterTrainingData, PullGPSDataLocal)
class ExtractClusterData(Task):  # tags="data assembly"
    """
    Description:
        Take GPS Coordinates for clusters and extract the median value of the desire\
        rasters for each cluster.
    Inputs:
        Raster Filepaths (Dictionary of CkanTargets): raster files of independent\
        variables
        DataFrame Filepath (Dictionary of LocalTarget): dataframe file of GPS points\
        for cluster centers.
    Outputs:
        raster_clusters_data_file (Serialized DataFrame): dataframe with independent
        variables for each cluster.
    User Defined Parameters:
        data_list (List): list of variables from the input dataframe to be used as\
        independent variables in the model.\
        Default = `['accessibility', 'population', 'night_lights', 'landcover']`
        multi_wave_data (List): list of variables in data_list that vary round to\
        round. Default = `['population']`
        waves (Dict): a dictionary translating the integer number wave to the string\
        year. Default = `{1: '2015', 2: '2016', 3: '2016', 4: '2017'}`
    Literary References:
    Methods:
        buffer_points() from the `geospatialtasks repo \
        <https://github.com/kimetrica/geospatialtasks/blob/input_functions/\
        functions/geospatial.py>`_
        calc_cluster_data() from the `geospatialtasks repo\
        <https://github.com/kimetrica/geospatialtasks/blob/input_functions/\
        functions/remote_sensing_functs.py>`
    """

    data_list = luigi.ListParameter(
        default=["accessibility", "population", "night_lights", "landcover"],
        significant=False,
    )
    multi_wave_data = luigi.ListParameter(default=["population"], significant=False)
    waves = luigi.DictParameter(
        default={1: "2015", 2: "2016", 3: "2016", 4: "2017"}, significant=False
    )

    def output(self):
        return IntermediateTarget(task=self)

    def merge_dataframes(self, stats_df, df, data_col_name_map, on_wave=False):
        """
        Concatenate multiple rounds of data.
        stats_dfs = list of dataframes to concatenate
        df = dataframe with the one row per cluster that will acrew all the data
        data_col_name_map =  map of old to new data column name
                             e.g {'median': 'population'}
        """
        if on_wave:
            merge_index = ["latitude", "longitude", "wave"]
        else:
            merge_index = ["latitude", "longitude"]
        old_col_name = list(data_col_name_map.keys())[0]

        # Add to location df
        df = df.merge(
            stats_df[merge_index + [old_col_name]], on=merge_index, how="outer"
        )
        df = df.rename(columns=data_col_name_map)

        return df

    def run(self):

        # raster data for all independent variables
        inputs = self.input()[0]

        # location associated with dependent variable
        loc_csv_file = self.input()[1]

        # Reference raster for geo-referencing
        ref_rast_file = inputs["accessibility"]

        # Buffer household data with buf_rad
        geo_cluster = buffer_points(
            ref_rast_file.path,
            loc_csv_file.path,
            buf_rad=5000,
            lat_name="latitude",
            lon_name="longitude",
            fname_out=None,
        )

        # Cycle through variables, grab clustered data from the rasters
        df = geo_cluster.copy()
        all_data = {}
        for i in self.data_list:  # FIXME: oh, here's the vars....
            if i in self.multi_wave_data:
                temp = {}
                for w, year in self.waves.items():
                    temp[w] = geo_cluster.loc[geo_cluster["wave"] == w]
                    period = pd.Period(
                        year, "M"  # FIXME, should be monthly
                    )  # FIXME, hopefully waves have a single month
                    temp[w] = calc_cluster_data(
                        temp[w], inputs[period].path
                    )  # FIXME hopefully handles multiband
                    temp[w]["wave"] = w

                all_data[i] = pd.concat(temp)
                df = self.merge_dataframes(all_data[i], df, {"median": i}, on_wave=True)

            else:
                all_data[i] = calc_cluster_data(geo_cluster, inputs[i].path)
                df = self.merge_dataframes(all_data[i], df, {"median": i})

        df = df.drop(
            [
                "latitude",
                "longitude",
                "pointlatlon",
                "pointxy",
                "bufferxy",
                "buffer_latlon",
                "geometry",
                "geom_geojson",
            ],
            axis=1,
        )

        with self.output().open("w") as output:
            output.write(df)


@requires(ExtractClusterData)
class OutputIndependentVariables(Task):
    """
    """

    def output(self):
        return FinalTarget(
            path="household_economic_independent_variables.csv", task=self
        )

    def run(self):
        with self.input().open("r") as f:
            df = f.read()

        df.to_csv(self.output().path)

        # Upload to CKAN
        target = CkanTarget(
            dataset={"id": "8399cfb4-fb5f-4e71-93a9-db77ce3b74a4"},
            resource={"name": "raster_data"},
        )
        if target.exists():
            target.remove()
        target.put(file_path=self.output().path)


@requires(OutputIndependentVariables)
class ReadClusteredRasterData(ReadDataFrameTask):
    """
    Read in clustered raster data.
    """

    read_method = "read_csv"
    read_args = {"index_col": 0}

    def output(self):
        """
        Save dataframe as serialized object.
        """
        return IntermediateTarget(task=self)


@requires(ReadClusteredRasterData, CalculateConsumptionExpenditure)
class MergeRasterEconomicClusters(Task):  # tags="data assembly"
    """
    Description:
        Merge the raster data with the socioeconomic data for each cluster.
    Inputs:
        raster_clusters_data_file (Serialized DataFrame): dataframe with independent
        variables for each cluster. DataFrame Filepaths (Dictionary of CkanTargets):
        filepahts to household survey data containing dependent variable.
    Outputs:
        processed_data_file (csv): dataframe with independent and dependent variables
                                   for each cluster.
    User Defined Parameters:
        filename: the filepath at which to save the results.
        dep_var: the name of the dependent variable contained in the household survey
                 data. Default = `tc_imp`
        poverty_line: the poverty line of this dataset. Default = `8.7126`
    Literary References
    Methods:
    """

    dep_var = luigi.Parameter(default="consumption_expenditure", significant=False)
    poverty_line = luigi.FloatParameter(default=8.7126, significant=False)
    data_list = luigi.ListParameter(
        default=["accessibility", "population", "night_lights", "landcover"],
        significant=False,
    )

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        # TODO: Rewrite this part
        with self.input()[0].open("r") as f:
            df = f.read()

        with self.input()[1].open("r") as f:
            ce_dfs = f.read()

        ce_df = ce_dfs["consumption_expenditure"]

        # TODO: ask why averages taken
        # ce_df_clustered = (
        #     ce_df.groupby(["state", "ea", "wave"]).mean()[self.dep_var].reset_index()
        # )
        # ce_df_clustered["plinePPP"] = self.poverty_line
        #
        # df_final = ce_df_clustered.merge(df, on=["state", "ea", "wave"], how="right")
        df_final = ce_df.merge(df, on=["state", "ea", "wave"], how="right")

        with self.output().open("w") as output:
            output.write(df_final)


@requires(MergeRasterEconomicClusters)
class OutputTrainingData(Task):
    """
    """

    def output(self):
        # FIXME: why both CkanTarget and FinalTarget?
        return FinalTarget(
            f"economic_model_training_data_{self.PercentOfNormalRainfall}.csv"
        )

    def run(self):
        with self.input().open("r") as f:
            df = f.read()

        # Output CSV
        df.to_csv(self.output().path)

        # Upload to CKAN
        target = CkanTarget(
            dataset={"id": "8399cfb4-fb5f-4e71-93a9-db77ce3b74a4"},
            resource={
                "name": f"economic_model_training_data_{self.PercentOfNormalRainfall}"
            },
        )
        if target.exists():
            target.remove()
        target.put(self.output().path)
