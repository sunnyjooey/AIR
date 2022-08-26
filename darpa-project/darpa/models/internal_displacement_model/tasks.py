import luigi
import numpy as np
import pandas as pd
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from luigi import ExternalTask, Task
from luigi.util import requires
from merf.merf import MERF
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


class PullFinalDataset(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "255051cf-acf8-42ea-851d-a9850c6093c2"},
            resource={"id": "becb0212-5ff1-4323-9604-daf134c0223d"},
        )


@requires(PullFinalDataset)
class Train(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        df = pd.read_csv(self.input().path, index_col=0)
        date = "2021_Q1"
        mask = df["quarter_year"] == date
        current = df.loc[mask]

        train_test = df.loc[df["ADMIN0"] == "Ethiopia"]

        date = "2021_Q1"
        mask = train_test["quarter_year"] < date
        train_test = train_test.loc[mask]

        train, test = train_test_split(train_test, test_size=0.20, random_state=1)
        select_cols = [
            "fatalities_per_event",
            "peaceful_days",
            "settlement_trend",
            "drought_index",
            "mean_rainfall",
            "ethnicity_count",
            "youth_bulge",
        ]
        X_train = train[select_cols]
        Z_train = np.ones((len(X_train), 1))
        clusters_train = train["location"]
        y_train = train["log_idp_per_pop"]
        X_test = test[select_cols]
        Z_test = np.ones((len(X_test), 1))
        clusters_test = test["location"]
        y_test = test["log_idp_per_pop"]
        mrf = MERF()
        mrf.fit(X_train, Z_train, clusters_train, y_train)
        y_predict = mrf.predict(X_test, Z_test, clusters_test)

        print("r2 score of the model: ", r2_score(y_test, y_predict))
        print("mse of the model: ", mean_squared_error(y_test, y_predict))
        print("rmse of the model: ", np.sqrt(mean_squared_error(y_test, y_predict)))
        with self.output().open("w") as out:
            out.write((mrf, current))


class PullAdminData(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "4dbc3cc7-9474-49f2-bfd4-231e78401caa"},
            resource={"id": "f50b0363-e3fd-4020-808e-3a26a62511bb"},
        )


@requires(PullFinalDataset, Train, PullAdminData)
class Forecast(Task):
    peaceful_days_offset = luigi.ChoiceParameter(
        default=0,
        choices=[0, 30, 60],
        var_type=int,
        description="The number of days without conflict before the next conflict incident. Choices are 0, 30, and 60 "
        "with 0 representing the default or observed duration of peaceful days. When the user choose 30 or "
        "60, the value will be added on each number of the observed peaceful days which decreases the "
        "likelihood of conflict their by reducing the number of displaced population.",
    )
    ethnicity_count_offset = luigi.ChoiceParameter(
        default=0,
        choices=[0, 3, 6],
        var_type=int,
        description="The number of of ethnic groups living in each administration. Choices are 0, 3, and 6 with 0 "
        "representing the default or observed duration of peaceful days. When the user choose 3 or 6, the "
        "value will be added on each number of ethnic groups which might increase the likelihood of "
        "conflict there by increasing the number of displaced population due to competition over "
        "resources.",
    )
    fatalities_per_event_scenario = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.50, 0.50],
        var_type=float,
        description="Average number of fatalities per conflict event with choices 0.0, -0.5, and 0.5. The value 0.0 "
        "represent the default or observed average number of fatalities per event while values -0.5 or 0.5 "
        "implies a reduction or increase in the observed average number of fatalities per event by 50 "
        "percent, respectively. A positive relationship is assumed with the the number of displaced "
        "population due to conflict as increased intensity of conflict in the form of loss of human lives "
        "increases the likelihood of future displacement.",
    )
    drought_index_scenario = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.50, 0.50],
        var_type=float,
        description="mean evaporative stress index per quarter with choices 0.0, -0.05, and 0.05. The value 0.0 "
        "represent the default or observed average monthly drought while values -0.5 or 0.5 implies a "
        "worsening and improving drought situation by 50 percent, respectively. Overall, an inverse "
        "relationship is assumed with displacement as a increase in the drought level(decrease in the "
        "value of the drought index) increases the likelihood of drought which translate into conflict "
        "and displacement due to competition over resources.",
    )
    mean_rainfall_scenario = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.50, 0.50],
        var_type=float,
        description="Mean quarterly rainfall with choices 0.0, -0.05, and 0.05. The value 0.0 represent the default or "
        "observed average monthly rainfall in mm while values -0.5 or 0.5 implies a reduction or increase "
        "in the observed share of mean rainfall by 50 percent, respectively. Overall, an inverse "
        "relationship is assumed with the onset of conflict as a reduction in rainfall increases "
        "the likelihood of drought which translate into conflict due to competition over resources.",
    )

    def output(self):
        dst = (
            f"IDP_Output/internal_displacement_forecast_fatalities_per_event_scenario_"
            f"{self.fatalities_per_event_scenario}_drought_index_scenario_"
            f"{self.drought_index_scenario}_mean_rainfall_scenario_{self.mean_rainfall_scenario}.csv"
        )
        return FinalTarget(path=dst, task=self)

    @staticmethod
    def add_quarter(row):
        year, q = row.split("_")
        q_num = int(q[1]) + 1
        if q_num == 5:
            year = int(year) + 1
            q_num = 1
        return f"{year}_Q{q_num}"

    def run(self):
        # mrf, current = self.input()[1].open("r").read()
        data = self.input()[1].open("r").read()

        mrf, current = data
        X_current = current[
            [
                "fatalities_per_event",
                "peaceful_days",
                "population_density",
                "drought_index",
                "mean_rainfall",
                "ethnicity_count",
                "youth_bulge",
            ]
        ]
        clusters_current = current["location"]

        Z_current = np.ones((len(X_current), 1))
        X_current["fatalities_per_event"] = (
            X_current["fatalities_per_event"] * self.fatalities_per_event_scenario
        )

        # Apply scenario
        X_current["peaceful_days"] = X_current["peaceful_days"] + (
            self.peaceful_days_offset
        )
        X_current["ethnicity_count"] = X_current["ethnicity_count"] + (
            self.ethnicity_count_offset
        )

        X_current["fatalities_per_event"] = X_current["fatalities_per_event"] * (
            1 + self.fatalities_per_event_scenario
        )
        X_current["drought_index"] = X_current["drought_index"] * (
            1 + self.drought_index_scenario
        )
        X_current["mean_rainfall"] = X_current["mean_rainfall"] * (
            1 + self.mean_rainfall_scenario
        )

        y_forecast = mrf.predict(X_current, Z_current, clusters_current)

        current1 = current.reset_index()
        current1 = current.reset_index()
        y_forecast = pd.DataFrame(y_forecast)
        y_forecast = np.exp(y_forecast - 0.00005)
        forecast = pd.merge(y_forecast, current1, right_index=True, left_index=True)
        forecast.rename(
            columns={0: "forecasted_displaced_per_population"}, inplace=True
        )
        forecast["forecasted_displaced_total"] = 0
        forecast["forecasted_displaced_total"] = forecast[
            "forecasted_displaced_per_population"
        ] * (forecast["population_count"])
        forecast["quarter_year"] = forecast["quarter_year"].apply(
            lambda x: self.add_quarter(x)
        )
        replace_map = {
            "_Q1": "-01-01",
            "_Q2": "-04-01",
            "_Q3": "-07-01",
            "_Q4": "-10-01",
        }
        for k, v in replace_map.items():
            forecast["quarter_year"] = forecast["quarter_year"].str.replace(k, v)
        forecast["quarter_year"] = pd.to_datetime(forecast["quarter_year"])
        with self.output().open("w") as out:
            forecast.to_csv(out.name)
