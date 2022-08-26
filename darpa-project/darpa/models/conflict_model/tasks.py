import logging
from functools import reduce

import luigi
import numpy as np
import pandas as pd
from luigi import ExternalTask, Task
from luigi.util import requires
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from imblearn.ensemble import EasyEnsembleClassifier
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget

logger = logging.getLogger("luigi-interface")


class PullDataFromCkan(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "8fdeaf93-8704-475b-b127-35d7567e5d8a"},
            resource={"id": "5001f527-32f2-49d6-8220-ebc36faf96a6"},
        )


@requires(PullDataFromCkan)
class DataProcessing(Task):
    peaceful_days = luigi.ChoiceParameter(
        default=0,
        choices=[0, 30, 60],
        var_type=int,
        description="The number of days without conflict before the next conflict incident. Choices are 0, 30, "
        "and 60 with 0 representing the default or "
        "observed duration of peaceful days. "
        "When the user choose 30 or 60, the value will be added on each number of the observed peaceful "
        "days which decreases the likelihood of conflict. "
        "However, it has to be noted that the model becomes less accurate for higher values of peaceful "
        "days (30 or 60) due to class imbalance in the training data. "
        "Therefore, leaving at default value is recommended when altering other parameters of the model.",
    )

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        df = pd.read_csv(f"{self.input().path}")
        eth1 = df[["ADMIN0", "ADMIN1", "ADMIN2", "conflict_frequency", "event_date"]]
        eth1 = eth1[eth1["conflict_frequency"] != 0]
        eth1 = (
            eth1.groupby(["ADMIN0", "ADMIN1", "ADMIN2", "event_date"])[
                "conflict_frequency"
            ]
            .sum()
            .reset_index()
        )
        eth1["event_date"] = eth1.groupby(["ADMIN0", "ADMIN1", "ADMIN2"])[
            "event_date"
        ].apply(lambda x: x.sort_values())
        eth1["event_date"] = pd.to_datetime(eth1.event_date)
        eth1["peaceful_days"] = eth1.groupby(["ADMIN0", "ADMIN1", "ADMIN2"])[
            "event_date"
        ].diff() / np.timedelta64(1, "D")
        eth1["peaceful_days"] = eth1["peaceful_days"].fillna(0)
        eth1.event_date = eth1.event_date.astype(str)
        eth1 = eth1[["ADMIN0", "ADMIN1", "ADMIN2", "event_date", "peaceful_days"]]
        eth2 = pd.merge(
            df, eth1, on=["ADMIN0", "ADMIN1", "ADMIN2", "event_date"], how="left"
        )

        eth2 = eth2.dropna(axis=0, subset=["ADMIN0", "ADMIN1", "ADMIN2"])

        eth2["conflict"] = 0
        eth2.loc[(eth2["peaceful_days"] > self.peaceful_days), "conflict"] = 1
        f_df_sum1 = (
            eth2.groupby(["ADMIN0", "ADMIN1", "ADMIN2", "month_year", "year"])[
                "peaceful_days"
            ]
            .mean()
            .reset_index()
        )
        f_df_sum2 = (
            eth2.groupby(["ADMIN0", "ADMIN1", "ADMIN2", "month_year", "year"])[
                "conflict", "fatalities", "conflict_frequency"
            ]
            .sum()
            .reset_index()
        )
        f_df_sum2["conflict_onset"] = 0
        a = np.array(f_df_sum2["conflict"].values.tolist())

        f_df_sum2["conflict_onset"] = np.where(a > 0, 1, a).tolist()
        dfs = [f_df_sum1, f_df_sum2]
        acled_final = reduce(
            lambda left, right: pd.merge(
                left,
                right,
                how="outer",
                on=["ADMIN0", "ADMIN1", "ADMIN2", "month_year", "year"],
            ),
            dfs,
        )
        acled_final["fatalities_per_event"] = (
            acled_final.fatalities / acled_final.conflict_frequency
        )

        acled_final = acled_final[
            [
                "ADMIN0",
                "ADMIN1",
                "ADMIN2",
                "month_year",
                "year",
                "conflict_frequency",
                "fatalities",
                "conflict_onset",
                "fatalities_per_event",
                "peaceful_days",
            ]
        ]
        df1 = (
            df.groupby(["ADMIN0", "ADMIN1", "ADMIN2", "month_year"])[
                "settlement_trend",
                "population_count",
                "population_density",
                "mean_rainfall",
                "ethnicity_count",
                "youth_bulge",
            ]
            .mean()
            .reset_index()
        )
        dfs = [acled_final, df1]
        df_final = reduce(
            lambda left, right: pd.merge(
                left,
                right,
                how="inner",
                on=["ADMIN0", "ADMIN1", "ADMIN2", "month_year"],
            ),
            dfs,
        )
        df_final["location"] = df_final.apply(
            lambda x: "%s_%s_%s" % (x["ADMIN0"], x["ADMIN1"], x["ADMIN2"]), axis=1
        )
        df_final.fatalities_per_event = df_final.fatalities_per_event.fillna(0)

        clean_target = df_final[["location", "month_year", "conflict_onset"]]
        df0 = clean_target.set_index(["month_year", "location"])  # index
        # pull out the groups, shift with lag step=1
        df0 = df0.unstack().shift(-12)
        df0 = df0.stack(dropna=False)

        df0 = df0.reset_index().sort_values("location").dropna()

        result = pd.merge(df_final, df0, how="inner", on=["location", "month_year"])

        result["year"] = result["year"].astype(int) + 1
        result = result.fillna(0).drop_duplicates()
        result["month"] = result["month_year"].apply(lambda x: x.split("-")[1])
        result["month_year"] = result.apply(
            lambda x: f"{x['year']}-{x['month']}", axis=1
        )

        end_date = "2020-02"
        mask = result["month_year"] < end_date
        train_test = result.loc[mask]

        start_date = "2020-01"

        mask = result["month_year"] > start_date
        current = result.loc[mask]

        with self.output().open("w") as out:
            out.write({"train_test": train_test, "current": current})


@requires(DataProcessing)
class ModelTraing(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        with self.input().open() as src:

            train_test = src.read()["train_test"]
        train1, test1 = train_test_split(train_test, test_size=0.33, random_state=1)

        train = train1.drop(["location", "month_year"], axis=1)
        test = test1.drop(["location", "month_year"], axis=1)

        X_train = train[
            [
                "conflict_onset_x",
                "fatalities_per_event",
                "mean_rainfall",
                "population_density",
                "peaceful_days",
                "youth_bulge",
            ]
        ]
        X_test = test[
            [
                "conflict_onset_x",
                "fatalities_per_event",
                "mean_rainfall",
                "population_density",
                "peaceful_days",
                "youth_bulge",
            ]
        ]

        y_train = train.conflict_onset_y
        y_test = test.conflict_onset_y

        estimator_ee = Pipeline(
            [("StandardScaller", StandardScaler()), ("RF", EasyEnsembleClassifier())]
        )

        estimator_ee.fit(X_train, y_train)
        print(estimator_ee.score(X_test, y_test))
        y_valid = estimator_ee.predict(X_test)
        print(classification_report(y_test, y_valid))
        with self.output().open("w") as out:
            out.write(estimator_ee)


@requires(ModelTraing, DataProcessing)
class Predict(Task):
    youth_bulge_percentage_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.05, 0.05],
        var_type=float,
        description="The share of population in the 15 to 29 age bracket on yearly basis. Choices are 0.0, -0.05, "
        "and 0.05 with 0 representing the default or observed share of youth population. The value -0.05 "
        "or 0.05 implies a reduction or increase in the observed share of youth population by 5 percent, "
        "respectively. An positive relationship is assumed with the onset of conflict due to the linkage "
        "of youth bulge with youth unemployment which increases the likelihood of conflict.",
    )
    mean_rainfall_percentage_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.50, 0.50],
        var_type=float,
        description="Mean monthly rainfall with choices 0.0, -0.05, and 0.05. The value 0.0 represent the default or "
        "observed average monthly rainfall in mm while values -0.5 or 0.5 implies a reduction or increase "
        "in the observed share of mean rainfall by 50 percent, respectively. Overall, an inverse "
        "relationship is assumed with the onset of conflict as a reduction in rainfall increases the "
        "likelihood of drought which translate into conflict due to competition over resources.",
    )
    fatalities_per_event_percentage_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.50, 0.50],
        var_type=float,
        description="Average number of fatalities per conflict event with choices 0.0, -0.5, and 0.5. "
        "The value 0.0 represent the default or observed average number of fatalities per event while "
        "values -0.5 or 0.5 implies a reduction or increase in the observed average number of fatalities "
        "per event by 50 percent, respectively. A positive relationship is assumed with the onset of "
        "conflict as increased intensity of conflict in the form of loss of human lives increases the "
        "likelihood of future conflict.",
    )

    def output(self):
        return FinalTarget(path="hoa_conflict_forecast.csv", task=self)

    def adjust_year(self, month):
        year, month = month.split("-")
        year = int(year) + 1
        return f"{year}-{month}"

    def run(self):
        with self.input()[0].open() as src:
            estimator_ee = src.read()
        with self.input()[1].open() as src:
            current = src.read()["current"]

        X_current = current[
            [
                "conflict_onset_x",
                "fatalities_per_event",
                "mean_rainfall",
                "population_density",
                "peaceful_days",
                "youth_bulge",
            ]
        ]

        X_current["youth_bulge"] = X_current["youth_bulge"] * (
            1 + self.youth_bulge_percentage_change
        )
        X_current["mean_rainfall"] = X_current["mean_rainfall"] * (
            1 + self.mean_rainfall_percentage_change
        )
        X_current["fatalities_per_event"] = X_current["fatalities_per_event"] * (
            1 + self.fatalities_per_event_percentage_change
        )
        y_forecast = estimator_ee.predict(X_current)

        df_forecast = pd.DataFrame(y_forecast)
        df_forecast.rename(columns={0: "conflict_onset_forecast"}, inplace=True)
        current = current[["location", "month_year"]].drop_duplicates()
        current[["ADMIN0", "ADMIN1", "ADMIN2"]] = current["location"].str.split(
            "_", n=3, expand=True
        )
        df_current = current.reset_index()
        df_forecast1 = df_current.join(df_forecast)
        df_forecast2 = df_forecast1[
            ["ADMIN0", "ADMIN1", "ADMIN2", "month_year", "conflict_onset_forecast"]
        ]

        # df_forecast2.loc[(df_forecast2['conflict_onset_forecast']
        # < 0.5), 'conflict_onset_forecast'] = 0.0
        df_forecast2["month_year"] = df_forecast2["month_year"].apply(
            lambda x: self.adjust_year(x)
        )

        with self.output().open("w") as out:
            df_forecast2.to_csv(out.name)
