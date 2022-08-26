import datetime
import os
import shutil
import warnings
from typing import Union

import geopandas as gpd
import pandas as pd
import rasterio
from luigi import Task
from luigi.parameter import DateIntervalParameter

from kiluigi.targets.delegating import FinalTarget, IntermediateTarget


class MaskDataToTime(Task):
    """
    Accepts a folder of data as input. The files should be:
    -geojson
    -raster
    -csv
    -excel
    -json
    -pickled dataframe

    This Task allows non-georeferenced data (csv, excel, json, dataframe)
    """

    time = DateIntervalParameter()
    time_column_warning = "'Time' parameter does not exist in dataset: {}"
    time_window_warning = "geojson file {} has no features that fall in the specified time window of {} to {}."

    def requires(self) -> Union[IntermediateTarget, FinalTarget]:
        """
        Allows input to be either IntermediateTarget or FinalTarget.
        Must be a filepath to a folder where all the input data is kept.
        """
        raise NotImplementedError

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/")

    def get_inputs(self):
        """
        Allow one specify input incase the upstream task outputs a dict of targets
        """
        return self.input()

    def run(self):
        input_path = self.get_inputs().path

        time_start, time_end = self.time.date_a, self.time.date_b

        # Check that the input is an existing filapth
        assert os.path.exists(input_path), f"{input_path} does not exist"

        # Create temporary output directory to write data to
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)

            # If the input_path is a folder, check that it is not empty and
            # create file_list that contains the filepaths of all its contents
            if os.path.isdir(input_path):
                assert os.listdir(input_path) != [], "The input folder is empty"
                assert os.path.exists(input_path), "The input does not exist"
                file_list = os.listdir(input_path)

            # Else if the input_path is a file, split off the file name and
            # create the file_list with only the single file as its contents
            elif os.path.isfile(input_path):
                assert os.path.exists(input_path), "The input does not exist"
                file_list = [os.path.split(input_path)[1]]

            # Cycle through input files and apply the time filter
            for fp in file_list:
                fp = os.path.join(input_path, fp)

                if fp.endswith(".tif"):
                    self.raster_time_filter(fp, time_start, time_end, tmpdir)

                else:
                    data = self.read_in_file(fp)
                    data_filt = self.dataframe_time_filter(data, time_start, time_end)
                    self.write_out_file(data_filt, fp, tmpdir, time_start, time_end)

    def apply_time_filter(self, x, time_start, time_end):
        """
        Return True if x is within the time range defined
        by time_start and time_end.
        """
        time_check = False
        try:
            if time_end and (time_start <= x <= time_end):
                time_check = True
            elif x == time_start:
                time_check = True
        except TypeError:
            if time_end and (time_start <= pd.Timestamp(x) <= time_end):
                time_check = True
            elif x == time_start:
                time_check = True

        return time_check

    def dataframe_time_filter(self, df, time_start, time_end):
        """
        For data that is read in as a dataframe, apply the timefiltering.
        Select only rows in the dataframe that fall in the desired time window.
        """

        # reinforce the dates to be Timestamp object
        if df["Time"].dtype == "datetime64[ns]":
            time_start = pd.Timestamp(time_start)
            time_end = pd.Timestamp(time_end)
        index_select = df["Time"].apply(
            lambda x: self.apply_time_filter(x, time_start, time_end)
        )
        df_filtered = df[index_select].copy()
        return df_filtered

    def datetime_schema(self, gdf):
        """
        For geojson files, define the datetime schema to write the file.
        """
        schema = gpd.io.file.infer_schema(gdf)
        schema["properties"]["Time"] = "datetime"
        return schema

    def read_in_file(self, fp):
        """
        Read in vector data with various possible extentions.
        """
        if fp.endswith(".geojson"):
            data = gpd.read_file(fp)
            # Convert time to datetime
            data["Time"] = data["Time"].apply(
                lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S")
            )
        elif fp.endswith(".csv"):
            data = pd.read_csv(fp)
        elif fp.endswith(".xlsx") | fp.endswith(".xls"):
            data = pd.read_excel(fp)
        elif fp.endswith(".json"):
            data = pd.read_json(fp)
        elif fp.endswith(".pickle") | fp.endswith(".fp"):
            data = pd.read_pickle(fp)

        # Check for time column
        assert "Time" in data.columns, self.time_column_warning.format(fp)
        return data

    def write_out_file(self, data_filt, fp, tmpdir, time_start, time_end):
        """
        Read in vector data with various possible extentions.
        """
        f = os.path.split(fp)[1]

        # Check if there is data inside the time filter window
        if data_filt.empty:
            warnings.warn(
                self.time_window_warning.format(fp, time_start, time_end), UserWarning
            )
        else:
            if fp.endswith(".geojson"):
                if data_filt.empty is False:
                    schema = self.datetime_schema(data_filt)
                    data_filt.to_file(
                        os.path.join(tmpdir, f), driver="GeoJSON", schema=schema
                    )

            elif fp.endswith(".csv"):
                if data_filt.empty is False:
                    data_filt.to_csv(os.path.join(tmpdir, f))

            elif fp.endswith(".xlsx") | fp.endswith(".xls"):
                if data_filt.empty is False:
                    data_filt.to_excel(os.path.join(tmpdir, f))

            elif fp.endswith(".json"):
                data_filt.to_json(os.path.join(tmpdir, f))

    def raster_time_filter(self, fp, time_start, time_end, tmpdir):
        """
        Time filter for raster files.
        Check the data file to ensure it has have the proper time tag property.
        If the time tag is in the desired time window, copy the file to the output directory.
        """
        data = rasterio.open(fp)
        f = os.path.split(fp)[1]

        assert (
            "Time" in data.tags().keys()
        ), "'Time' tag does not exist in dataset: {}".format(fp)

        # If the raster matches the time period,
        # copy it into the output folder
        raster_time = datetime.date.fromisoformat(data.tags()["Time"])
        if self.apply_time_filter(raster_time, time_start, time_end):
            shutil.copy(fp, os.path.join(tmpdir, f))
        else:
            warnings.warn(
                "raster file {} does not fall in the specified time window of {} to {}.".format(
                    fp, time_start, time_end
                ),
                UserWarning,
            )
        data.close()
