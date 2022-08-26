import pandas as pd

import luigi
from io import StringIO

from utils.test_utils import LuigiTestCase
from models.malnutrition_model.tasks import VarsStandardized_infer, MalnutInference

import os

"""
This unittest module will test the malnutrition module
"""

BASE_DIR = os.path.dirname(__file__)


class VarsStandardized_inferTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()

        self.smart_df2 = [
            {
                "NDVI_lag1": 0.25,
                "Population": 5000,
                "CPI": 120,
                "crop_per_capita": 0.30,
                "CHIRPS(mm)_lag3": 50,
                "CHIRPS(mm)_lag3_scenario": 25,
                "med_exp": 10,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Morobo",
                "ADMIN2NAME": "Morobo",
                "State": "Central Equatoria",
                "county_lower": "morobo",
            },
            {
                "NDVI_lag1": 0.1,
                "Population": 1000,
                "CPI": 150,
                "crop_per_capita": 0.05,
                "CHIRPS(mm)_lag3": 60,
                "CHIRPS(mm)_lag3_scenario": 30,
                "med_exp": 5,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Aweil West",
                "ADMIN2NAME": "Aweil West",
                "State": "Northern Bahr el Ghazal",
                "county_lower": "aweil west",
            },
            {
                "NDVI_lag1": 0.65,
                "Population": 10000,
                "CPI": 200,
                "crop_per_capita": 0.6,
                "CHIRPS(mm)_lag3": 50,
                "CHIRPS(mm)_lag3_scenario": 25,
                "med_exp": 12,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Juba",
                "ADMIN2NAME": "Juba",
                "State": "Central Equatoria",
                "county_lower": "juba",
            },
            {
                "NDVI_lag1": 0.35,
                "Population": 2500,
                "CPI": 200,
                "crop_per_capita": 0.45,
                "CHIRPS(mm)_lag3": 30,
                "CHIRPS(mm)_lag3_scenario": 15,
                "med_exp": 7,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Budi",
                "ADMIN2NAME": "Budi",
                "State": "Eastern Equatoria",
                "county_lower": "budi",
            },
            {
                "NDVI_lag1": 0.30,
                "Population": 7600,
                "CPI": 250,
                "crop_per_capita": 0.35,
                "CHIRPS(mm)_lag3": 60,
                "CHIRPS(mm)_lag3_scenario": 30,
                "med_exp": 4,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Mayom",
                "ADMIN2NAME": "Mayom",
                "State": "Unity",
                "county_lower": "mayom",
            },
        ]

    def test_output(self):
        task = VarsStandardized_infer()

        with task.input()[0].open("w") as f:
            f.write(pd.DataFrame(self.smart_df2))

        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)
        output = task.output().open("r").read()

        # check that there's NO missing values
        self.assertFalse(output.isnull().values.any())

        # Check expected columns
        expected_columns = [
            "NDVI_lag1",
            "Population",
            "CPI",
            "med_exp",
            "CHIRPS(mm)_lag3",
            "crop_per_capita",
            "CHIRPS(mm)_lag3_scenario",
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
            "ADMIN2NAME",
        ]

        for col in expected_columns:
            with self.subTest(col=col):
                self.assertIn(col, output.columns)

        # check that there's negative values in numeric column to reflect standard scaler transform
        standardized_columns = [
            "NDVI_lag1",
            "Population",
            "CPI",
            "crop_per_capita",
            "CHIRPS(mm)_lag3",
            "CHIRPS(mm)_lag3_scenario",
            "med_exp",
        ]

        # check columns are standardized by having some negative values
        for col in standardized_columns:
            with self.subTest(col=col):
                self.assertTrue((output[col] < 0).any())

        # python -m unittest models/malnutrition_model/tests/test_tasks.py


class MalnutInferenceTestCase(LuigiTestCase):
    """
    mock the required VarsStandardized_infer, and MalnutDF_infer
    """

    def setUp(self):
        super().setUp()

        self.norm_df = [
            {
                "NDVI_lag1": -1.53,
                "Population": -1.0,
                "CPI": -0.644,
                "crop_per_capita": -0.069,
                "CHIRPS(mm)_lag3": -0.60,
                "CHIRPS(mm)_lag3_scenario": -0.84,
                "med_exp": 1.5,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Morobo",
                "ADMIN2NAME": "Morobo",
                "State": "Central Equatoria",
                "county_lower": "morobo",
            },
            {
                "NDVI_lag1": -1.9,
                "Population": -0.956,
                "CPI": -0.65,
                "crop_per_capita": -0.08,
                "CHIRPS(mm)_lag3": 0.2,
                "CHIRPS(mm)_lag3_scenario": -0.05,
                "med_exp": -0.52,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Aweil West",
                "ADMIN2NAME": "Aweil West",
                "State": "Northern Bahr el Ghazal",
                "county_lower": "aweil west",
            },
            {
                "NDVI_lag1": 0.30,
                "Population": 1.1,
                "CPI": -0.66,
                "crop_per_capita": -0.061,
                "CHIRPS(mm)_lag3": -0.65,
                "CHIRPS(mm)_lag3_scenario": -0.88,
                "med_exp": 0.35,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Juba",
                "ADMIN2NAME": "Juba",
                "State": "Central Equatoria",
                "county_lower": "juba",
            },
            {
                "NDVI_lag1": -1.25,
                "Population": -0.71,
                "CPI": -0.60,
                "crop_per_capita": -0.065,
                "CHIRPS(mm)_lag3": -0.85,
                "CHIRPS(mm)_lag3_scenario": -0.90,
                "med_exp": -0.02,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Budi",
                "ADMIN2NAME": "Budi",
                "State": "Eastern Equatoria",
                "county_lower": "budi",
            },
            {
                "NDVI_lag1": -0.28,
                "Population": 0.75,
                "CPI": -0.26,
                "crop_per_capita": -0.075,
                "CHIRPS(mm)_lag3": -0.33,
                "CHIRPS(mm)_lag3_scenario": -0.89,
                "med_exp": -0.35,
                "Apr": 0,
                "Aug": 0,
                "Dec": 0,
                "Feb": 0,
                "Jan": 0,
                "Jul": 0,
                "Jun": 0,
                "Mar": 0,
                "May": 1,
                "Nov": 0,
                "Oct": 0,
                "Sep": 0,
                "GAM_rate": 0.001,
                "SAM_rate": 0.001,
                "Year": 2017,
                "Time": pd.Timestamp("20170501"),
                "Month": "May",
                "County": "Mayom",
                "ADMIN2NAME": "Mayom",
                "State": "Unity",
                "county_lower": "mayom",
            },
        ]

    def test_output(self):
        task = MalnutInference()

        with task.input()[0].open("w") as f:
            f.write(pd.DataFrame(self.norm_df))

        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)
        output = task.output().open("r").read()
        out_df = pd.read_csv(StringIO(output), sep=",")

        # test that predicted rate is less than 1

        self.assertTrue((out_df["gam_rate"] < 1).all())
        self.assertTrue((out_df["sam_rate"] < 1).all())

        # check that gam and sam cases are less than population
        case_cols = ["gam_baseline", "sam_baseline", "gam_scenario", "sam_scenario"]

        for col in case_cols:
            with self.subTest(col=col):
                self.assertTrue((out_df[col] <= out_df["Population"]).all())
