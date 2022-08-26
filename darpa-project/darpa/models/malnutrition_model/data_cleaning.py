import os
import pickle

import pandas as pd
from luigi.configuration import get_config
from luigi.util import requires
from sklearn.preprocessing import StandardScaler

from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget  # noqa: F401
from kiluigi.tasks import ExternalTask, Task, VariableInfluenceMixin  # noqa: F401
from models.malnutrition_model.functions.helper_func import retrieve_file
from models.malnutrition_model.functions.maln_utility_func import (
    county_expend,
    df_aggregate,
    estimate_pop,
)

CONFIG = get_config()
MALNUTRITION_OUTPUT_PATH = os.path.join(
    CONFIG.get("paths", "output_path"), "malnutrition_model"
)

# Check if output path exists, if not create it
if not os.path.exists(MALNUTRITION_OUTPUT_PATH):
    os.makedirs(MALNUTRITION_OUTPUT_PATH, exist_ok=True)


class CkanMalnVars(ExternalTask):

    """
    Task for pulling miscellaneous South Sudan files from ckan that are independent variables for malnutrition rate estimation
    """

    def output(self):

        return {
            "NDVI": CkanTarget(
                dataset={"id": "ca5e2bdb-b65d-47a6-83b9-025996108b39"},
                resource={"id": "497a5aa9-cd04-43ba-b35f-acd694750528"},
            ),
            "CHIRPS": CkanTarget(
                dataset={"id": "ca5e2bdb-b65d-47a6-83b9-025996108b39"},
                resource={"id": "3d404e4b-cc5b-4c6c-8156-bcc08bdff10c"},
            ),
            "CPI": CkanTarget(
                dataset={"id": "ca5e2bdb-b65d-47a6-83b9-025996108b39"},
                resource={"id": "3eab2b23-bcd0-4d6f-9820-066ce3820b0b"},
            ),
            "SSD_county_rates": CkanTarget(
                dataset={"id": "79e631b6-86f2-4fc8-8054-31c17eac1e3f"},
                resource={"id": "81df7a5f-1b24-41d1-8f9c-0e67623d58a7"},
            ),
            "crop": CkanTarget(
                dataset={"id": "ca5e2bdb-b65d-47a6-83b9-025996108b39"},
                resource={"id": "aca71453-727d-4577-bb0d-d2a63d0371d8"},
            ),
            "expenditure": CkanTarget(
                dataset={"id": "8399cfb4-fb5f-4e71-93a9-db77ce3b74a4"},
                resource={"id": "4c3c22f4-0c6e-4034-bee2-81c89d754dd5"},
            ),
        }


class CkanShpfiles(ExternalTask):

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


class SurveyData(ExternalTask):

    """
    Task for pulling South Sudan SMART surveys.
    """

    def output(self):

        return {
            "smart_surveys": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "75d749bd-20d2-49f0-a4ab-f07e9ec9cdf9"},
            )
        }


class TrainingDataset(ExternalTask):
    """
    Task for pulling cleaned, preprocessed data for So.Sudan and Ethiopia. Data are in the right format for training
    """

    def output(self):

        return {
            # "eth_select_df": CkanTarget(
            #  dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
            #  resource={"id": "021d6c62-149e-4c37-9857-27bf451872c6"},
            # ),
            # "ssd_select_df": CkanTarget(
            #  dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
            #  resource={"id": "24f581bc-3aa8-45e3-9b1c-5422b7c9e098"},
            # ),
            "eth_select_df_v3": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "e0a8eda9-aa59-4d7d-91ec-851194494cca"},
            ),
            "ssd_select_df_v3": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "f59a5a24-9c13-4230-9b8c-dd2cf34d0a34"},
            ),
        }


class CkanMalnVars_eth(ExternalTask):
    """
    Task for pulling cleaned, normalized data for Ethiopia, most(but not all) of the data are at admin2 levels
    """

    def output(self):

        return {
            "NDVI": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "ecc7ae3d-b507-4dcc-8f4b-e8e872225bb7"},
            ),
            "CHIRPS": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "8d8cb418-b22f-43ad-b2b8-0435fe5ecc9b"},
            ),
            "CPI": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "f5bbb854-0460-488f-a765-06f39f810a28"},
            ),
            "pop_census": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "545d6b55-716a-4159-939b-b11589e1d36e"},
            ),
            "crop": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "36a16953-2884-411b-88ad-8055d9d2d5d9"},
            ),
            "expenditure": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "825fed22-d15a-4dac-9ce2-301f2a80fbfd"},
            ),
        }


class crop_prod_admin0(ExternalTask):
    """
    import csv df of maize production data for South Sudan and Ethiopia
    """

    def output(self):
        return {
            # "crop_admin0": CkanTarget(
            #    dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
            #   resource={"id": "361f5f48-522b-463a-8421-aac65041d704"},
            #  ),
            "crop_dssat_admin2": CkanTarget(
                dataset={"id": "fecfeff6-32af-44fb-98ae-23fd875b7d28"},
                resource={"id": "e927fb8d-9f38-4683-9da9-76509b4d3855"},
            ),
        }


@requires(CkanMalnVars, CkanShpfiles, crop_prod_admin0)
class RetrvCkanData(Task):

    """
    retrieves data from CKAN and returns the dir path name dictionary
    """

    outfolder = MALNUTRITION_OUTPUT_PATH

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        ckan_data_dict = self.input()[0]  # returns dictionary
        ckan_shape_dict = self.input()[1]
        crop_df_dict = self.input()[2]

        ndvi_dir = ckan_data_dict[list(ckan_data_dict.keys())[0]].path
        chirps_dir = ckan_data_dict[list(ckan_data_dict.keys())[1]].path
        cpi_dir = ckan_data_dict[list(ckan_data_dict.keys())[2]].path
        ss_rates_dir = ckan_data_dict[list(ckan_data_dict.keys())[3]].path
        crop_dir = crop_df_dict[list(crop_df_dict.keys())[0]].path
        expend_dir = ckan_data_dict[list(ckan_data_dict.keys())[5]].path

        county_dir = ckan_shape_dict[list(ckan_shape_dict.keys())[0]].path
        pop_density_dir = ckan_shape_dict[list(ckan_shape_dict.keys())[1]].path

        retrieve_file(county_dir, ".zip", self.outfolder)

        county_path = os.path.join(self.outfolder, county_dir.split("/")[-2]) + "/"
        # print(crop_dir)
        path_names = {
            "ndvi_dir": ndvi_dir,
            "chirps_dir": chirps_dir,
            "cpi_dir": cpi_dir,
            "ss_rates_dir": ss_rates_dir,
            "crop_dir": crop_dir,
            "expend_dir": expend_dir,
            "county_dir": county_path,
            "pop_density_dir": pop_density_dir,
        }
        with self.output().open("w") as output:  # IntermediateTarget requires .open()
            output.write(path_names)


@requires(SurveyData)
class TrainDataset(Task):
    """
    imports training dataset from CKAN (smart surveys 2014-2018)
    """

    outfolder = MALNUTRITION_OUTPUT_PATH

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        survey_data_dict = self.input()
        survey_dir = survey_data_dict[list(survey_data_dict.keys())[0]].path
        retrieve_file(survey_dir, ".zip", self.outfolder)
        survey_path = os.path.join(self.outfolder, survey_dir.split("/")[-2]) + "/"
        smart_2014 = pd.read_excel(survey_path + "/smart_2014.xlsx")
        smart_2015 = pd.read_excel(survey_path + "/smart_2015.xlsx")
        smart_2016 = pd.read_excel(survey_path + "/smart_2016.xlsx")
        smart_2017 = pd.read_excel(survey_path + "/smart_2017.xlsx")
        smart_2018 = pd.read_excel(survey_path + "/smart_2018.xlsx")

        # fix the end date column missing value for 2015
        end_date = []
        for _i, row in smart_2015.iterrows():
            if pd.isnull(row["End date"]) is True:
                end_date.append(row["Date of Completion"])
            else:
                end_date.append(row["End date"])

        smart_2015["End date"] = end_date
        # select teh columns before concatenating the dataframes
        select_cols = [
            "Year",
            "State",
            "County",
            "End date",
            "GAM (WHZ <-2 and/or oedema) 6-59 mo",
            "SAM (WHZ <-3 and/or oedema) 6-59mo",
        ]

        smart_2014_vars = smart_2014[select_cols]
        smart_2015_vars = smart_2015[select_cols]
        smart_2016_vars = smart_2016[select_cols]
        smart_2017_vars = smart_2017[select_cols]
        smart_2018_vars = smart_2018[select_cols]
        smart_total = pd.concat(
            [
                smart_2014_vars,
                smart_2015_vars,
                smart_2016_vars,
                smart_2017_vars,
                smart_2018_vars,
            ]
        ).dropna()

        smart_total["Year"] = smart_total["Year"].astype(int)
        smart_total["Month"] = pd.to_datetime(smart_total["End date"]).dt.strftime("%b")
        # smart_total['county_lower']=smart_total['County']
        smart_total["month_int"] = pd.to_datetime(smart_total["End date"]).dt.month
        smart_total.rename(
            columns={
                "GAM (WHZ <-2 and/or oedema) 6-59 mo": "GAM_rate",
                "SAM (WHZ <-3 and/or oedema) 6-59mo": "SAM_rate",
            },
            inplace=True,
        )

        with self.output().open("w") as output:  # IntermediateTarget requires .open()
            output.write(smart_total)


@requires(RetrvCkanData, TrainDataset)
class MalnutDF_train(Task):

    """
    read all S.Sudan data from CKAN and cache folders, and puts them in dataframe
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        path_names = self.input()[0].open().read()
        smart_total = self.input()[1].open().read()
        smart_total["Country"] = "South Sudan"

        expend_df = county_expend(path_names["county_dir"], path_names["expend_dir"])
        ndvi_df = pd.read_csv(path_names["ndvi_dir"])
        chirps_df = pd.read_csv(path_names["chirps_dir"])
        cpi_df = retrieve_file(path_names["cpi_dir"], ".csv")
        crop_df = pd.read_csv(path_names["crop_dir"])
        pop_est_df = estimate_pop(path_names["ss_rates_dir"])
        # load the typo dictionary, specify path relative to PYTHONPATH env variable
        pkl_file = open("models/malnutrition_model/typo_dict.pkl", "rb")
        typo_dictionary = pickle.load(pkl_file)  # noqa: S301
        pkl_file.close()
        typo_list = list(typo_dictionary.keys())

        rev_cname = []
        for _i, row in smart_total.iterrows():
            if row["County"] in typo_list:
                rev_cname.append(typo_dictionary[row["County"]])
            else:
                rev_cname.append(row["County"].strip())

        smart_df = df_aggregate(
            smart_total,
            crop_df,
            pop_est_df,
            ndvi_df,
            chirps_df,
            expend_df,
            cpi_df,
            rev_cname,
        )

        month_dummy = pd.get_dummies(smart_df.Month)
        smart_df2 = pd.concat([smart_df, month_dummy], axis=1)

        with self.output().open("w") as output:
            output.write(smart_df2)


@requires(CkanMalnVars_eth, CkanShpfiles_eth, crop_prod_admin0)
class RetrvDataEth(Task):

    """
    retrieves independent variable data from CKAN for ethiopia
    """

    outfolder = MALNUTRITION_OUTPUT_PATH

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        ckan_data_dict = self.input()[0]  # returns dictionary
        ckan_shape_dict = self.input()[1]
        crop_df_dict = self.input()[2]

        ndvi_df_dir = ckan_data_dict[list(ckan_data_dict.keys())[0]].path
        chirps_df_dir = ckan_data_dict[list(ckan_data_dict.keys())[1]].path
        CPI_df_dir = ckan_data_dict[list(ckan_data_dict.keys())[2]].path
        pop_df_dir = ckan_data_dict[list(ckan_data_dict.keys())[3]].path
        crop_df_dir = crop_df_dict[list(crop_df_dict.keys())[0]].path
        expend_df_dir = ckan_data_dict[list(ckan_data_dict.keys())[5]].path

        county_dir = ckan_shape_dict[list(ckan_shape_dict.keys())[0]].path
        pop_density_dir = ckan_shape_dict[list(ckan_shape_dict.keys())[1]].path

        retrieve_file(county_dir, ".zip", self.outfolder)

        county_path = os.path.join(self.outfolder, county_dir.split("/")[-2]) + "/"

        path_names = {
            "ndvi_dir": ndvi_df_dir,
            "chirps_dir": chirps_df_dir,
            "cpi_dir": CPI_df_dir,
            "pop_dir": pop_df_dir,
            "crop_dir": crop_df_dir,
            "expend_dir": expend_df_dir,
            "county_dir": county_path,
            "pop_density_dir": pop_density_dir,
        }
        with self.output().open("w") as output:
            output.write(path_names)


@requires(TrainingDataset)
class RetrvDataFrames(Task):

    """
    retrieves data frames from CKAN for south sudan and ethiopia
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        training_data_dict = self.input()  # returns dictionary
        eth_df_dir = training_data_dict[list(training_data_dict.keys())[0]].path
        ssd_df_dir = training_data_dict[list(training_data_dict.keys())[1]].path

        path_names = {"eth_select_df_v3": eth_df_dir, "ssd_select_df_v3": ssd_df_dir}
        with self.output().open("w") as output:
            output.write(path_names)


@requires(RetrvDataFrames)
class VarsStandardized_train(Task):

    """
    Standardize the numeric variables, model was trained on standardscaler variables
    """

    def output(self):
        # NOTE: in breaking the 'rules' and having multiple outputs in one task, it's best practice
        # to use ExpiringLocalTarget() because it writes to each target atomically
        # (https://luigi.readthedocs.io/en/stable/tasks.html#task-output). if atomicity is not
        # a concern, stick to SerializerTarget()
        return {
            "standard_vars": IntermediateTarget(task=self, timeout=31536000),
            "scaler": FinalTarget(path="maln_scaler"),
        }

    def run(self):
        path_names = self.input().open().read()

        eth_select_df = pd.read_csv(path_names["eth_select_df_v3"])
        ss_select_df = pd.read_csv(path_names["ssd_select_df_v3"])
        # standardized the numeric values on training data

        ssd_train_df = ss_select_df[ss_select_df["Year"] < 2018]
        eth_train_df = eth_select_df[(eth_select_df["Year"] < 2014)]

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

        select_train_df = pd.concat([ssd_train_df, eth_train_df], axis=0)[
            select_vars[3:]
        ]
        scaler = StandardScaler()
        select_transform = scaler.fit_transform(select_train_df[select_vars[3:9]])
        select_train_df[
            [
                "NDVI_lag1",
                "Population",
                "CPI",
                "crop_per_capita",
                "CHIRPS(mm)_lag3",
                "med_exp",
            ]
        ] = select_transform

        with self.output()["scaler"].open("w") as scaler_output:
            try:
                self.output()["scaler"].remove()
            except FileNotFoundError:
                pass
            scaler_output.write(scaler)

        with self.output()["standard_vars"].open("w") as output:
            output.write(select_train_df)
