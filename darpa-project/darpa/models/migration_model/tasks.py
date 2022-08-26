import numpy as np
import os

from sklearn.metrics import r2_score, mean_squared_error
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget  # noqa: F401
from kiluigi.tasks import ExternalTask, Task  # noqa: F401
from luigi.configuration import get_config
from luigi.util import requires

from models.migration_model.data_cleaning import languageMerge
from merf import MERF
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

CONFIG = get_config()
MIGRATION_OUTPUT_PATH = os.path.join(
    CONFIG.get("paths", "output_path"), "migration_model"
)


# Check if output path exists, if not create it
if not os.path.exists(MIGRATION_OUTPUT_PATH):
    os.makedirs(MIGRATION_OUTPUT_PATH, exist_ok=True)


@requires(languageMerge)
class merf_model(Task):

    """
    Train the data using Mixed Effect Random Forest model. Unfortunately the MERF model does not have a method for saving the trained parameters as pickle objects.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        df = self.input().open().read()
        df = df.rename(
            columns={
                "od": "od",
                "year": "year",
                "refugees": "refugees",
                "log_refugees": "log_refugees",
                "o_lag_idps": "o_idps_count_lag",
                "o_fatalities_conf_ratio": "o_conflict_fatalities_ratio",
                "o_pop": "o_population_count",
                "d_pop": "d_population_count",
                "oupop": "o_urban_population_percent",
                "dupop": "d_urban_population_percent",
                "oyouth_percent": "o_youth_population_percent",
                "o_cpi": "o_corruption_perception_index",
                "d_cpi": "d_corruption_perception_index",
                "opcgdp": "o_gdp_per_capita",
                "opolity2": "o_democracy_score",
                "dpolity2": "d_democracy_score",
                "opi": "o_consumer_price_index",
                "score_drr": "o_drought_risk_score",
                "dmai": "d_migrant_acceptance_index",
                "dlag_acceptance": "d_refugee_recognition_rate_lag",
                "olowincome": "o_low_income_country",
                "oeuropeandna": "o_europe_and_north_america",
                "dlowincome": "d_low_income_country",
                "deuropeandna": "d_europe_and_north_america",
                "distance": "od_distance_km",
                "neighbor": "od_common_border",
            }
        )

        df = df[
            [
                "od",
                "year",
                "refugees",
                "log_refugees",
                "o_idps_count_lag",
                "o_conflict_fatalities_ratio",
                "o_population_count",
                "d_population_count",
                "o_urban_population_percent",
                "d_urban_population_percent",
                "o_youth_population_percent",
                "o_corruption_perception_index",
                "d_corruption_perception_index",
                "o_gdp_per_capita",
                "o_democracy_score",
                "d_democracy_score",
                "o_consumer_price_index",
                "o_drought_risk_score",
                "d_migrant_acceptance_index",
                "d_refugee_recognition_rate_lag",
                "o_low_income_country",
                "o_europe_and_north_america",
                "d_low_income_country",
                "d_europe_and_north_america",
                "od_distance_km",
                "od_common_border",
            ]
        ]

        #  The model's performance is sensitive to the volume of the training data.
        train = df.loc[df["year"] != 2018]
        test = df.loc[df["year"] == 2018]
        input_vars = [
            "o_idps_count_lag",
            "o_population_count",
            "d_population_count",
            "o_urban_population_percent",
            "d_urban_population_percent",
            "o_youth_population_percent",
            "o_corruption_perception_index",
            "d_corruption_perception_index",
            "o_gdp_per_capita",
            "o_democracy_score",
            "d_democracy_score",
            "o_consumer_price_index",
            "o_drought_risk_score",
            "d_migrant_acceptance_index",
            "d_refugee_recognition_rate_lag",
            "o_low_income_country",
            "o_europe_and_north_america",
            "d_low_income_country",
            "d_europe_and_north_america",
            "od_distance_km",
            "od_common_border",
        ]
        X_train = train[input_vars]
        # Z_train is the design matrix
        Z_train = np.ones((len(X_train), 1))
        clusters_train = train["od"]
        y_train = train["log_refugees"]

        mrf = MERF(n_estimators=100, max_iterations=20)
        mrf.fit(X_train, Z_train, clusters_train, y_train)
        X_known = test[input_vars]
        Z_known = np.ones((len(X_known), 1))
        clusters_known = test["od"]
        y_known = test["log_refugees"]
        y_hat_known_merf = mrf.predict(X_known, Z_known, clusters_known)

        prediction = np.exp(y_hat_known_merf) - 1
        y_test_org = np.exp(y_known) - 1

        print("r2 score of the model: ", r2_score(y_test_org, prediction))
        print("mse of the model: ", mean_squared_error(y_test_org, prediction))
        print(
            "rmse of the model: ", np.sqrt(mean_squared_error(y_test_org, prediction))
        )

        X_whole_df = df[input_vars]
        Z_whole_df = np.ones((len(X_whole_df), 1))
        clusters_whole_df = df["od"]

        y_hat_pred = mrf.predict(X_whole_df, Z_whole_df, clusters_whole_df)
        pred_whole_df = np.exp(y_hat_pred) - 1
        prediction_df = df.copy()
        prediction_df = prediction_df[["year", "od"] + input_vars]
        prediction_df["migration_pred"] = pred_whole_df

        with self.output().open("w") as f:
            f.write(prediction_df)


@requires(merf_model, GlobalParameters)
class migration_pred_year(Task):

    """
    filters the prediction dataset based on year and country_level.
    """

    def output(self):
        return FinalTarget(path="migration_flow.csv", task=self, ACL="public-read")

    def run(self):
        # get the year values from global parameters
        dt_list = list(
            set(range(self.time.date_a.year, self.time.dates()[-1].year + 1))
        )

        prediction_df = self.input()[0].open().read()
        prediction_yr_df = prediction_df.loc[prediction_df["year"].isin(dt_list)]
        # select rows that contains the country of interest (e.g. Ethiopia in parameter country_level)
        prediction_yr_df = prediction_yr_df.loc[
            prediction_yr_df["od"].str.contains(self.country_level)
        ]
        with self.output().open("w") as f:
            prediction_yr_df.to_csv(f, index=False)
