# FIXME: this entire suite of tasks only covers South Sudan
import bisect
import datetime

from luigi.util import requires
from kiluigi.tasks import Task, ExternalTask, ReadDataFrameTask
from kiluigi.targets import CkanTarget, IntermediateTarget
import pandas as pd

from ..demand_model.mappings import (
    ss_state_spelling,
    ss_hfs_options,
    wave4_units,
    item_labels,
    itemlabel_to_catlabel,
    adult_equivalence_conversions,
    rename_food_columns,
    rename_hhm_columns,
    replace_hhm_values,
    food_groupings,
)
from .data.mappings import month_to_date


class PullExchangeRateData(ExternalTask):
    """
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "e45e6f64-05ca-4783-b0eb-e93b7dd8a708"},
            resource={"id": "d212d140-ed6d-4da5-9b17-f2165fc4bf5b"},
        )


@requires(PullExchangeRateData)
class ReadExchangeRate(ReadDataFrameTask):
    """
    Read in exchange rate data.
    """

    read_method = "read_csv"
    read_args = {
        "date_parser": lambda x: datetime.date.fromisoformat(x),
        "parse_dates": ["period_date", "start_date"],
    }

    def output(self):
        """
        Save DataFrame as serialized object.
        """
        return IntermediateTarget(task=self)


class PullHFSFoodSection(ExternalTask):
    """
    Pull the First Wave waves of household survey data from CKAN.
    """

    def output(self):
        """
        Define CkanTarget resources to pull.
        """
        return {
            1: CkanTarget(
                dataset={"id": "0c943766-30e1-434a-bef3-cfa9f316429a"},
                resource={"id": "f848d36f-5ca8-4f79-a722-8590cf85a0da"},
            ),
            2: CkanTarget(
                dataset={"id": "848df607-d42d-4b0b-a960-8a3ce076ed11"},
                resource={"id": "3f65208f-1c33-493b-8732-6b8cba447ae9"},
            ),
            3: CkanTarget(
                dataset={"id": "644180c0-8679-48f5-a58f-cfdc5b8bb779"},
                resource={"id": "bc560082-412c-40ca-9f3c-5b9c3291d0df"},
            ),
            4: CkanTarget(
                dataset={"id": "677e56b6-4453-4485-a6f7-f57454a9e463"},
                resource={"id": "dff4b1ad-60ec-4cf8-a647-9478dba5284b"},
            ),
        }


@requires(PullHFSFoodSection)
class ReadHFSFoodSection(ReadDataFrameTask):
    """
    Read in all Food sections
    """

    read_method = "read_csv"
    read_args = {"index_col": 0}

    def output(self):
        """
        Save DataFrame as serialized object.
        """
        return IntermediateTarget(task=self)


class PullHFSHHQSection(ExternalTask):
    """
    Pull the all the hhm section data for all waves.
    """

    def output(self):
        return {
            1: CkanTarget(
                dataset={"id": "0c943766-30e1-434a-bef3-cfa9f316429a"},
                resource={"id": "0992a9c9-0611-467d-9248-2eee32fb67ef"},
            ),
            2: CkanTarget(
                dataset={"id": "848df607-d42d-4b0b-a960-8a3ce076ed11"},
                resource={"id": "5d83fa6d-b0aa-4cc4-a6e8-171d8355dc95"},
            ),
            3: CkanTarget(
                dataset={"id": "644180c0-8679-48f5-a58f-cfdc5b8bb779"},
                resource={"id": "0bb158b2-abd7-49fd-b98e-94d68fb53063"},
            ),
            4: CkanTarget(
                dataset={"id": "677e56b6-4453-4485-a6f7-f57454a9e463"},
                resource={"id": "fabb56fb-c054-4433-9372-d9d1bc511a02"},
            ),
        }


@requires(PullHFSHHQSection)
class ReadHFSHHQSection(ReadDataFrameTask):
    """
    Read in the hhq section data for all waves.
    """

    read_method = "read_csv"
    read_args = {"index_col": 0}

    def output(self):
        """
        Save dataframe as serialized object.
        """
        return IntermediateTarget(task=self)


@requires(ReadHFSFoodSection, ReadExchangeRate, ReadHFSHHQSection)
class NormalizeFoodPriceData(Task):
    """
    Normalize the food price data for all 4 waves of HFS data.
    """

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        with self.input()[0].open("r") as f:
            data = f.read()

        with self.input()[1].open("r") as f:
            ex_rate = f.read()

        with self.input()[2].open("r") as f:
            hhq = f.read()

        # Convert date in HHQ
        # FIXME: only year and month are in the data, not date
        for k in hhq.keys():
            if type(hhq[k].index) is not pd.core.indexes.range.RangeIndex:
                hhq[k] = hhq[k].reset_index()
            hhq[k]["date"] = hhq[k]["month"].apply(
                lambda x: datetime.datetime.fromisoformat(month_to_date[k][x])
            )

        # Merge date from HHQ to Food
        merge_cols = ["state", "ea", "hh"]
        for k in data.keys():
            if type(data[k].index) is not pd.core.indexes.range.RangeIndex:
                data[k] = data[k].reset_index()
            data[k] = data[k].merge(
                hhq[k][merge_cols + ["date"]], left_on=merge_cols, right_on=merge_cols
            )

        # Cycle through the input data, calculate the average exchange rates
        # and normalize the column names for each wave.
        ex_rate = ex_rate.rename(columns={"value": "exchange_rate"})
        if type(ex_rate.index) is not pd.core.indexes.range.RangeIndex:
            ex_rate = ex_rate.reset_index()

        for k in data:
            data[k] = (
                data[k]
                .merge(
                    ex_rate[["exchange_rate", "start_date"]],
                    left_on="date",
                    right_on="start_date",
                )
                .drop("start_date", axis=1)
            )
            data[k] = data[k].rename(columns=rename_food_columns[k])

        # Extra cleaning steps required for wave 4
        # Convert units
        data[4]["purc_unit_converstion"] = data[4]["purc_unit"].apply(
            lambda x: wave4_units.get(x)
        )
        data[4]["cons_unit_converstion"] = data[4]["cons_unit"].apply(
            lambda x: wave4_units.get(x)
        )
        # Convert to kg
        data[4]["purc_quant_kg"] = (
            data[4]["purc_quant"] * data[4]["purc_unit_converstion"]
        )
        data[4]["cons_quant_kg"] = (
            data[4]["cons_quant"] * data[4]["cons_unit_converstion"]
        )

        # Convert ssp prices to ssp_per_kg for waves 3 and 4
        for k in [3, 4]:
            data[k]["price_ssp_per_kg_cleaned"] = (
                data[k]["price_ssp"] / data[k]["purc_quant_kg"]
            )

        # Convert ssp prices to usd and create itemlabels and item categories
        # for all waves
        for k in data.keys():
            data[k]["price_usd_per_kg"] = (
                data[k]["price_ssp_per_kg_cleaned"] * data[k]["exchange_rate"]
            )
            data[k]["itemlabel"] = data[k]["itemid"].apply(lambda x: item_labels[x])
            data[k]["catlabel"] = data[k]["itemlabel"].apply(
                lambda x: itemlabel_to_catlabel[x]
            )

        with self.output().open("w") as output:
            output.write(data)


class PullHFSHHMSection(ExternalTask):
    """
    Pull the all the household members section data for all waves.
    """

    def output(self):
        return {
            1: CkanTarget(
                dataset={"id": "0c943766-30e1-434a-bef3-cfa9f316429a"},
                resource={"id": "7f5a6efb-5252-4c95-bf04-6f7b5cd1348b"},
            ),
            2: CkanTarget(
                dataset={"id": "848df607-d42d-4b0b-a960-8a3ce076ed11"},
                resource={"id": "ee6681e2-8dc5-4e96-bd36-43b38ce1310e"},
            ),
            3: CkanTarget(
                dataset={"id": "644180c0-8679-48f5-a58f-cfdc5b8bb779"},
                resource={"id": "f6e41ce2-fb57-4c58-aeb4-e4305db5edd4"},
            ),
            4: CkanTarget(
                dataset={"id": "677e56b6-4453-4485-a6f7-f57454a9e463"},
                resource={"id": "6ba4d182-3f3d-4adf-ba90-a3a5752b066e"},
            ),
        }


@requires(PullHFSHHMSection)
class ReadHFSHHMSection(ReadDataFrameTask):
    """
    Read in the household members section data for all waves.
    """

    read_method = "read_csv"
    read_args = {}

    def output(self):
        """
        Save DataFrame as serialized object.
        """
        return IntermediateTarget(task=self)


@requires(ReadHFSHHMSection)
class NormalizeHHMData(Task):
    """
    Normalize the household members data for all waves.
    """

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        with self.input().open("r") as f:
            data = f.read()

        for k in data.keys():
            # Rename Columns
            data[k] = data[k].rename(columns=rename_hhm_columns[k])
            # Replace coded values
            data[k] = data[k].replace(replace_hhm_values[k])

            # Convert age to a float
            data[k]["member_age_years"] = data[k]["member_age_years"].apply(
                pd.to_numeric, args=("coerce",)
            )
            # Replace missing ages with median age for that relation to hoh
            median_age_replace = (
                data[k]
                .groupby(["member_hoh_relation"])
                .median()["member_age_years"]
                .to_dict()
            )
            nan_index = data[k].loc[data[k]["member_age_years"].isnull()].index
            data[k].loc[nan_index, "member_age_years"] = (
                data[k]
                .loc[nan_index]
                .replace({"member_hoh_relation": median_age_replace})[
                    "member_hoh_relation"
                ]
            )
            # Drop any members that are missing gender, age, and relation to hoh
            # assume these are errors
            data[k] = data[k].dropna(
                subset=["member_gender", "member_age_years", "member_hoh_relation"]
            )

        with self.output().open("w") as output:
            output.write(data)


@requires(NormalizeHHMData)
class CalculateAdultEquivalence(Task):
    """
    Calculate Adult Equivalence.
    """

    def output(self):
        """
        Save DataFrame as serialized object.
        """
        return IntermediateTarget(task=self)

    def run(self):
        with self.input().open("r") as f:
            data = f.read()
        bins = adult_equivalence_conversions

        for k, df in data.items():
            data[k]["adult_equivalent"] = df.apply(
                lambda x: bins[x["member_gender"]][
                    list(bins[x["member_gender"]].keys())[
                        bisect.bisect(
                            list(bins[x["member_gender"]].keys()), x["member_age_years"]
                        )
                        - 1
                    ]
                ],
                axis=1,
            )

        with self.output().open("w") as output:
            output.write(data)


@requires(CalculateAdultEquivalence, NormalizeFoodPriceData)
class MergeHouseholdData(Task):
    """
    Merge the sections of the households survey. Specify which columns from each section
    you want to select. Create a unique houshold id for each household called 'hhid'.
    """

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        # Get the data
        inputs = self.input()
        data = {"hhm": inputs[0], "food": inputs[1]}

        cols = {
            "food": [
                "wave",
                "date",
                "state",
                "ea",
                "hh",
                "itemid",
                "itemlabel",
                "catlabel",
                "price_usd_per_kg",
                "cons_quant_kg",
            ],
            "hhm": [
                "wave",
                "state",
                "ea",
                "hh",
                "member_age_years",
                "member_gender",
                "adult_equivalent",
            ],
        }

        for k in data.keys():
            with data[k].open("r") as f:
                data[k] = f.read()
        hh_dfs = {}

        # Concatenate each section of the survey with the desired columns
        for k in data.keys():
            for i in data[k].keys():
                data[k][i]["wave"] = i
            hh_dfs[k] = pd.concat(
                [
                    data[k][1][list(cols[k])],
                    data[k][2][list(cols[k])],
                    data[k][3][list(cols[k])],
                    data[k][4][list(cols[k])],
                ]
            )
            hh_dfs[k] = hh_dfs[k].reset_index(drop=True)

        # Create unique hhid for each household
        for k, df in hh_dfs.items():
            df = df.replace({"state": ss_state_spelling["state"]})
            df["state_code"] = df["state"].copy()
            df.loc[df["wave"] == 1, "state_code"] = df.loc[
                df["wave"] == 1, "state_code"
            ].apply(lambda x: ss_hfs_options["code"][x])
            df.loc[df["wave"] != 1, "state"] = df.loc[df["wave"] != 1, "state"].apply(
                lambda x: ss_hfs_options["state"][x]
            )
            df["hhid"] = list(
                map(
                    lambda x, y, z, a: "_".join([str(x), str(y), str(z), str(a)]),
                    df["wave"],
                    df["state_code"],
                    df["ea"],
                    df["hh"],
                )
            )
            hh_dfs[k] = df.copy()

        with self.output().open("w") as output:
            output.write(hh_dfs)


@requires(MergeHouseholdData)
class CalculateConsumptionExpenditure(Task):
    """
    """

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        with self.input().open("r") as f:
            data = f.read()
        df = data["food"].copy()
        df["food_groups"] = df["catlabel"].apply(lambda x: food_groupings.get(x))
        df = df.dropna(subset=["food_groups"])

        # interpolate missing prices
        df["price_usd_per_kg"].mask(
            df["price_usd_per_kg"].isna(),
            df.groupby("itemid")["price_usd_per_kg"].transform("mean"),
            inplace=True,
        )
        df["price_usd_per_kg"].mask(
            df["price_usd_per_kg"].isna(),
            df.groupby("food_groups")["price_usd_per_kg"].transform("mean"),
            inplace=True,
        )
        assert not df["price_usd_per_kg"].isna().any()

        # expenditure per item
        df["expenditure"] = df["cons_quant_kg"] * df["price_usd_per_kg"]

        # expenditure per household
        total_agg_func = {
            "wave": "first",
            "state": "first",
            "ea": "first",
            "hh": "first",
            "expenditure": "sum",
        }
        total_df = (
            df.groupby(["hhid", "date"], as_index=False)
            .aggregate(total_agg_func)
            .rename(columns={"expenditure": "total_expenditure"})
        )

        # Get hh size in units of adult equivalence and merge into total_df
        df_hhm = data["hhm"].groupby("hhid")["adult_equivalent"].sum()
        # FIXME: hhid is not matching one-to-one, why?
        total_df = total_df.merge(df_hhm, left_on="hhid", right_index=True)

        # Calculate consumption expenditure per adult equivalent per day
        total_df["consumption_expenditure"] = (
            total_df["total_expenditure"] / total_df["adult_equivalent"] / 7
        )

        data_output = {}
        data_output["consumption_expenditure"] = total_df
        data_output["food"] = df
        data_output["food_groupings"] = food_groupings

        with self.output().open("w") as output:
            output.write(data_output)
