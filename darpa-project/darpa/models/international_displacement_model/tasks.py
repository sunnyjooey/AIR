import luigi
import numpy as np
import pandas as pd
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from luigi import ExternalTask, Task
from luigi.util import requires
from merf.merf import MERF
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from imblearn.ensemble import EasyEnsembleClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


class PullFinalDataset(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "c4c4a27b-9056-4b9b-9978-6a53f3122fde"},
            resource={"id": "4ddf3315-1444-4869-a8a3-9a6d4ad538e2"},
        )


@requires(PullFinalDataset)
class PreProcess(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        df = pd.read_csv(self.input().path, index_col=0)
        df["year"] = df["year"].astype(str)
        end_year = "2021"
        mask = df["year"] < end_year
        training_data = df.loc[mask]
        end_year = "2021"
        mask = df["year"] == end_year
        forecast_data = df.loc[mask]
        training_data["log_refugee_count_y"] = np.log(training_data["refugees_y"])
        training_data["log_refugee_count_x"] = np.log(training_data["refugees_x"])
        train, test = train_test_split(
            training_data, test_size=0.25, random_state=90210
        )
        X_train = train[
            [
                "refugees_x",
                "o_population",
                "o_inflation",
                "o_displacement",
                "o_gdp_pcap",
                "d_population",
                "d_inflation",
                "d_displacement",
                "d_gdp_pcap",
                "share_border",
                "dstance_km",
            ]
        ]
        y_train = train.log_refugee_count_y.values

        y_train_binary = np.where(y_train > 0, 1, 0)

        estimator_ee = Pipeline(
            [("StandardScaller", StandardScaler()), ("RF", EasyEnsembleClassifier())]
        )
        estimator_ee.fit(X_train, y_train_binary)

        X_forecast = forecast_data[
            [
                "refugees_x",
                "o_population",
                "o_inflation",
                "o_displacement",
                "o_gdp_pcap",
                "d_population",
                "d_inflation",
                "d_displacement",
                "d_gdp_pcap",
                "share_border",
                "dstance_km",
            ]
        ]

        y_forecast_binary = estimator_ee.predict(X_forecast)
        y_forecast_binary1 = pd.DataFrame(y_forecast_binary)
        forecast_data1 = forecast_data.reset_index()
        forecast_data2 = forecast_data1.merge(
            y_forecast_binary1, left_index=True, right_index=True
        )
        value = 0
        train_f = train[train["refugees_y"] > value]
        test_f = test[test["refugees_y"] > value]
        mask = forecast_data2[0] > value
        forecast_f = forecast_data2.loc[mask]

        with self.output().open("w") as out:
            out.write({"train_f": train_f, "test_f": test_f, "forecast_f": forecast_f})


@requires(PreProcess)
class ModelTraining(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        with self.input().open() as src:
            train_f = src.read()["train_f"]
        with self.input().open() as src:
            test_f = src.read()["test_f"]
        with self.input().open() as src:
            forecast_f = src.read()["forecast_f"]

        select_cols = [
            "refugees_x",
            "o_population",
            "o_inflation",
            "o_displacement",
            "o_gdp_pcap",
            "d_population",
            "d_inflation",
            "d_displacement",
            "d_gdp_pcap",
            "share_border",
            "dstance_km",
        ]
        X_train = train_f[select_cols]
        Z_train = np.ones((len(X_train), 1))
        clusters_train = train_f["od"]
        y_train = train_f["log_refugee_count_y"]
        X_test = test_f[select_cols]
        Z_test = np.ones((len(X_test), 1))
        clusters_test = test_f["od"]
        y_test = test_f.log_refugee_count_y
        mrf = MERF()
        mrf.fit(X_train, Z_train, clusters_train, y_train)
        y_hat = mrf.predict(X_test, Z_test, clusters_test)
        print("r2 score of the model: ", r2_score(y_test, y_hat))
        print("mse of the model: ", mean_squared_error(y_test, y_hat))

        with self.output().open("w") as out:
            out.write({"mrf": mrf, "forecast_f": forecast_f})


@requires(ModelTraining)
class Forecast(Task):
    o_displacement_percentage_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.5, 0.5],
        var_type=float,
        description="The number of conflict induced internally displaced population at origin. Choices are 0.0, -0.5, "
        "and 0.5 with 0 representing the default or observed number of displaced population. The value -0.5"
        " or 0.05 implies a reduction or increase in the observed displaced population by 50 percent, "
        "respectively. A postive relationship is assumed with the international displacement where an "
        "increase in internal displacement directly contribute to the number of people crossing their "
        "country border.",
    )
    o_gdp_pcap_percentage_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.5, 0.5],
        var_type=float,
        description="The level of per capita income at origin. Choices are 0.0, -0.5, and 0.5 with 0 representing the "
        "default or observed number of per capita income. The value -0.5 or 0.05 implies a reduction or "
        "increase in the observed per capita income by 50 percent, respectively. An inverse relationship "
        "is assumed with the international displacement where an increase income reduce the number of "
        "people crossing their country borders due to better opportunities within their country border.",
    )

    d_displacement_percentage_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.5, 0.5],
        var_type=float,
        description="The number of conflict induced internally displaced population at destination. Choices are 0.0, "
        "-0.5, and 0.5 with 0 representing the default or observed number of displaced population. "
        "The value -0.5 or 0.05 implies a reduction or increase in the observed displaced population by "
        "50 percent, respectively. An inverse relationship is assumed with the international displacement "
        "where an increase in internal displacement at destination means that there is less opportunity for"
        " internationally displaced population to stay in areas where there is ongoing displacement at "
        "destination.",
    )
    d_gdp_pcap_percentage_change = luigi.ChoiceParameter(
        default=0.0,
        choices=[0.0, -0.5, 0.5],
        var_type=float,
        description="The level of per capita income at destination. Choices are 0.0, -0.5, and 0.5 with 0 representing "
        "the default or observed number of per capita income. The value -0.5 or 0.05 implies a reduction or "
        "increase in the observed per capita income by 50 percent, respectively. A positive relationship is "
        "assumed with the international displacement where an increase the level of income at destination "
        "attracting more people crossing destination's country border as people expect better opportunities"
        " at destination.",
    )

    def output(self):
        return FinalTarget(path="international_displacement_forecast.csv", task=self)

    def run(self):
        with self.input().open() as src:
            mrf = src.read()["mrf"]
        with self.input().open() as src:
            forecast_f = src.read()["forecast_f"]

        select_cols = [
            "refugees_x",
            "o_population",
            "o_inflation",
            "o_displacement",
            "o_gdp_pcap",
            "d_population",
            "d_inflation",
            "d_displacement",
            "d_gdp_pcap",
            "share_border",
            "dstance_km",
        ]
        X_forecast = forecast_f[select_cols]
        Z_forecast = np.ones((len(X_forecast), 1))
        clusters_forecast = forecast_f["od"]

        # Apply scenario
        X_forecast["o_displacement"] = X_forecast["o_displacement"] * (
            1 + self.o_displacement_percentage_change
        )
        X_forecast["o_gdp_pcap"] = X_forecast["o_gdp_pcap"] * (
            1 + self.o_gdp_pcap_percentage_change
        )
        X_forecast["d_displacement"] = X_forecast["d_displacement"] * (
            1 + self.d_displacement_percentage_change
        )
        X_forecast["d_gdp_pcap"] = X_forecast["d_gdp_pcap"] * (
            1 + self.d_gdp_pcap_percentage_change
        )

        y_forecast = mrf.predict(X_forecast, Z_forecast, clusters_forecast)
        df_f = forecast_f.reset_index()
        y_forecast = pd.DataFrame(y_forecast)
        y_forecast = np.exp(y_forecast)
        y_forecast.rename(columns={0: "forecasted_refugee_population"}, inplace=True)
        y_forecast["forecasted_refugee_population"] = y_forecast[
            "forecasted_refugee_population"
        ].astype(int)
        forecast = pd.merge(y_forecast, df_f, right_index=True, left_index=True)
        with self.output().open("w") as out:
            forecast.to_csv(out.name)
