import datetime
import logging

import fiona
import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import pulp
from geopy.distance import great_circle
from luigi import ExternalTask, Task
from luigi.util import inherits, requires
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.ensemble import ExtraTreesRegressor as Estimator

# from sklearn.linear_model import LinearRegression as Estimator
from sklearn.metrics import r2_score
from sklearn.model_selection import RandomizedSearchCV, StratifiedShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, PowerTransformer
from sklearn.preprocessing import StandardScaler as Scaler

from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from .data_pre import MaskPopulationData, PullCrudeOilPrice
from .mapping import commodity_type_map
from .utils import nested_to_record, raster_t_df

RELATIVEPATH = "models/logistic_model/tasks/"

logger = logging.getLogger("luigi-interface")


class PullTrainingData(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"id": "9ee6ca82-17d7-4e60-8057-fefe4d4e7f75"},
        )


@requires(PullTrainingData)
class Train(Task):
    x_vars = luigi.ListParameter(
        default=[
            "distance_calc",
            "acled_mean",
            "year",
            "night_lights_max",
            "origin_pop_mean",
            "dest_pop_mean",
            "dem_std",
            "Dcountry",
            "Ocountry",
            "month",
            "cargo_type",
            "crude_oil_price",
            "border_crossing",
            "surface_condition_good",
            "surface_condition_fair",
            "surface_condition_bad",
            "surface_condition_excellent",
            "surface_condition_poor",
            "surface_condition_very_poor",
        ],
        significant=False,
    )

    def get_data(self):
        df = gpd.read_file(self.input().path)
        df["distance_calc"] = df[
            ["distance_calc", "O_dist_t_node", "D_dist_t_node"]
        ].sum(axis=1)
        return df

    def output(self):
        return IntermediateTarget(task=self, timeout=315360000)

    def run(self):
        data = self.get_data()
        country_list = [
            "Kenya",
            "Djibouti",
            "Ethiopia",
            "Uganda",
            "Sudan",
            "South Sudan",
            "Rwanda",
            "Tanzania",
            "Burundi",
            "Somalia",
        ]
        data = data.loc[
            (data["Ocountry"].isin(country_list))
            & (data["Dcountry"].isin(country_list))
        ]

        data["cargo_type"] = data["cargo_type"].replace(commodity_type_map)

        dependent_var = "rate USD/MT"

        cat_vars = ["Dcountry", "Ocountry", "cargo_type", "month", "border_crossing"]

        num_columns = list(set(self.x_vars) - set(cat_vars))

        # Drop missing data
        var_list = [dependent_var] + list(self.x_vars)
        if "distance_calc" not in var_list:
            var_list = var_list + ["distance_calc"]
        df = data[var_list]
        df = df.dropna(subset=list(df.columns))

        # Drop outliers
        df["rate"] = df[dependent_var] / df["distance_calc"]
        df = df.loc[
            (df["rate"] >= df["rate"].quantile(0.20))
            & (df["rate"] <= df["rate"].quantile(0.80))
        ]

        # Stratified split used due to the importance of distance in determining the cost of transport
        df["distance_cat"] = pd.cut(df["distance_calc"], bins=5)

        split = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
        var_list = [dependent_var] + list(self.x_vars)
        for train_index, test_index in split.split(df, df["distance_cat"]):
            train_set = df[var_list].iloc[train_index].copy()
            test_set = df[var_list].iloc[test_index].copy()

        preprocessor = ColumnTransformer(
            [
                ("numeric", Scaler(), num_columns),
                (
                    "unordered",
                    OneHotEncoder(categories="auto", handle_unknown="ignore"),
                    cat_vars,
                ),
            ]
        )
        # QuantileTransformer(output_distribution='normal')
        estimator = TransformedTargetRegressor(Estimator(), PowerTransformer())
        pipeline = Pipeline([("preprocessor", preprocessor), ("estimator", estimator)])

        parameters = {
            "estimator__regressor__n_estimators": [100, 150, 300, 400, 500],
            "estimator__regressor__criterion": ["mse"],
            "estimator__regressor__max_depth": [None],
            "estimator__regressor__min_samples_split": [2, 4, 8, 10, 12],
            "estimator__regressor__min_samples_leaf": [1, 3, 5, 7, 9, 11],
            "estimator__regressor__min_weight_fraction_leaf": [0.0],
            "estimator__regressor__max_features": ["auto", "sqrt", "log2"],
            "estimator__regressor__max_leaf_nodes": [None],
            "estimator__regressor__min_impurity_decrease": [0.0],
        }

        param_search = RandomizedSearchCV(
            estimator=pipeline,
            param_distributions=parameters,
            n_iter=100,
            cv=3,
            verbose=2,
            random_state=42,
            n_jobs=-1,
            scoring="neg_mean_squared_error",
        )

        y_train = train_set[dependent_var]
        X_train = train_set.drop(dependent_var, axis=1)
        param_search.fit(X_train, y_train)

        best_random = param_search.best_estimator_

        y_test = test_set[dependent_var]
        X_test = test_set.drop(dependent_var, axis=1)
        y_pred = best_random.predict(X_test)
        r2 = r2_score(y_test, y_pred)
        logger.info(f"The R Squared is {r2}")
        with self.output().open("w") as out:
            out.write(best_random)


class PullInferenceData(ExternalTask):
    admin1_name = luigi.Parameter(default="gambela")

    def output(self):
        if self.admin1_name == "gambela":
            return CkanTarget(
                dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
                resource={"id": "dd4581eb-7fb2-4b71-9773-39a251aee792"},
            )
        else:
            raise NotImplementedError


@requires(Train, PullInferenceData, PullCrudeOilPrice)
@inherits(GlobalParameters)
class PredictFreightRate(Task):
    cargo_type = luigi.ChoiceParameter(
        default="bagged grain", choices=["bagged grain", "break bulk", "containers"]
    )

    minimize_var = luigi.ChoiceParameter(
        default="rate USD/MT", choices=["rate USD/MT", "distance_calc"]
    )

    @staticmethod
    def get_date(row):
        date = datetime.date(int(row["year"]), int(row["month"]), 1)
        return date

    def add_crude_oil_price(self, df, crude_df):
        df["date"] = df.apply(lambda row: self.get_date(row), axis=1)
        df["date"] = pd.to_datetime(df["date"])
        df = df.merge(crude_df, on="date", how="left")
        df = df.drop(["date", "Change"], axis=1)
        df["border_crossing"] = 0
        df["Dcountry"] = "Ethiopia"
        df.loc[df["Dcountry"] != df["Ocountry"], "border_crossing"] = 1
        return df

    def output(self):
        try:
            return FinalTarget(
                "logistic_model/freight_rate.geojson", task=self, ACL="public-read"
            )
        except (ValueError, TypeError):
            return FinalTarget("logistic_model/freight_rate.geojson", task=self)

    def optimal_route(self, df):
        df = df[df[self.minimize_var] == df[self.minimize_var].min()]
        if df.shape[0] > 1:
            df = df.head(1)
        return df

    def run(self):
        with self.input()[0].open() as src:
            model = src.read()

        if self.admin1_name == "gambela":
            df = gpd.read_file(self.input()[1].path)
        else:
            df = pd.read_csv(self.input()[1].path)

        df["distance_calc"] = df[
            ["distance_calc", "O_dist_t_node", "D_dist_t_node"]
        ].sum(axis=1)
        # TODO: Delete this
        crude_df = pd.read_excel(self.input()[2].path)
        crude_df = crude_df.rename(
            columns={"Month": "date", "Price": "crude_oil_price"}
        )
        crude_df["date"] = pd.to_datetime(crude_df["date"])

        df = df.reset_index(drop=True)
        # df["speed"] = df["google_dist"] / df["travel_time"]
        df = df.rename(
            columns={"country_dest": "Dcountry", "country_origin": "Ocountry"}
        )
        cargo_df = pd.concat(
            [
                pd.DataFrame({"cargo_type": np.repeat([i], df.shape[0])})
                for i in [self.cargo_type]
            ]
        )

        for i, month in enumerate(
            pd.date_range(self.time.date_a, self.time.date_b, freq="M")
        ):
            temp = cargo_df.merge(df, left_index=True, right_index=True)
            temp["month"] = month.month
            temp["year"] = month.year
            temp = self.add_crude_oil_price(temp, crude_df)
            temp = temp.dropna(subset=self.x_vars)
            X = temp[list(self.x_vars)]
            temp["rate USD/MT"] = model.predict(X)

            if i == 0:
                out_df = temp.copy()
            else:
                out_df = pd.concat([out_df, temp])

        group_vars = ["Dlatitude", "Dlongitude", "cargo_type", "month", "year"]
        groups = out_df.groupby(group_vars)
        out_df = pd.concat([self.optimal_route(df) for _, df in groups])
        try:
            out_df = gpd.GeoDataFrame(out_df, geometry="geometry")
        except (ValueError, KeyError):
            out_df = gpd.GeoDataFrame(
                out_df, geometry=gpd.points_from_xy(out_df.Dlongitude, out_df.Dlatitude)
            )
        with self.output().open("w") as out:
            with fiona.Env():
                out_df.to_file(out.name, driver="GeoJSON")


@requires(PredictFreightRate, MaskPopulationData)
class CalculateDistance(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        pop_df = raster_t_df(
            src_file=self.input()[1].path, value_var="population", value=None
        )
        pop_df = pop_df.loc[pop_df["population"] > 3]
        pop_df = pop_df.drop(["row", "col"], axis=1)
        pop_df = pop_df.reset_index(drop=True)
        with self.input()[0].open() as src:
            dc_df = gpd.read_file(src.read())

        dc_df = dc_df.reset_index(drop=True)

        pop_id_set = set(pop_df.index)
        pos_id_set = set(dc_df.index)

        logger.info("Calculating distance")
        distance_map = {
            i: {
                j: great_circle(
                    (pop_df.loc[i, "y"], pop_df.loc[i, "x"]),
                    (dc_df.loc[j, "Dlatitude"], dc_df.loc[j, "Dlongitude"]),
                ).km
                for j in pos_id_set
            }
            for i in pop_id_set
        }
        for i in distance_map:
            assert all(i >= 0 for i in distance_map[i]), "Missing or negative distnace"
        with self.output().open("w") as out:
            out.write((pop_df, dc_df, distance_map, pop_id_set, pos_id_set))


@requires(CalculateDistance)
class OPtimalPOSLocation(Task):

    max_walking_dist = luigi.FloatParameter(default=5)
    walking_speed = luigi.FloatParameter(default=4)
    daily_wage = luigi.FloatParameter(default=2)
    quantity = luigi.FloatParameter(default=0.01)
    quantity_depend_on_pop = luigi.BoolParameter(default=False)
    limit_pos_num = luigi.BoolParameter(default=False)
    pos_num = luigi.IntParameter(200)
    population_percent = luigi.FloatParameter(default=0.1)
    output_assigned_route = luigi.BoolParameter(default=False)
    setting_up_pos_cost = luigi.FloatParameter(default=50)

    def output(self):
        pos_path = "logistic_model/optimal_pos.csv"
        route_path = "logistic_model/optimal_route.csv"
        out_map = {"pos": pos_path}
        if self.output_assigned_route:
            out_map.update(route=route_path)
        try:
            return {
                k: FinalTarget(path=v, task=self, ACL="public-read")
                for k, v in out_map.items()
            }
        except TypeError:
            return {k: FinalTarget(path=v, task=self) for k, v in out_map.items()}

    def walking_cost(self, dist):
        cost = (dist * 2) * (1 / self.walking_speed) * (self.daily_wage / 8)
        return cost

    def get_optimal_pos(
        self,
        walking_cost_map,
        population_map,
        warehouse_pos_cost_map,
        quantity_map,
        distance_limit_map,
        access_pos,
        pop_id_set,
        pos_id_set,
        distance_map,
    ):
        # Creates the prob variable to contain the problem data
        model = pulp.LpProblem("Aid_Distribution_Problem", pulp.LpMinimize)

        logger.info("Creating route variables")
        # Creates the problem variables for the flow on the routes from POS to population point
        route_list = [
            (i, j)
            for i in pop_id_set
            for j in pos_id_set
            if distance_map[i][j] <= self.max_walking_dist
        ]

        flow = pulp.LpVariable.dicts(
            "Route", route_list, cat=pulp.LpBinary, lowBound=0, upBound=1
        )

        logger.info("Creating POS variables")
        optimal_pos = pulp.LpVariable.dicts(
            "pos", pos_id_set, cat=pulp.LpBinary, lowBound=0, upBound=1,
        )

        logger.info("Setting objective function")
        # The objective function is added to prob first
        model += (
            pulp.lpSum(
                walking_cost_map[i][j] * population_map[i] * flow[(i, j)]
                for (i, j) in route_list
            )
            + pulp.lpSum(
                warehouse_pos_cost_map[(j)] * quantity_map[(i)] * flow[(i, j)]
                for (i, j) in route_list
            )
            + pulp.lpSum(optimal_pos[j] * self.setting_up_pos_cost for j in pos_id_set)
        )

        logger.info("Adding walking distance constraint")
        for (i, j) in route_list:
            model += (
                flow[(i, j)] <= optimal_pos[j] * distance_limit_map[i][j],
                f"distance_constraint {j, i}",
            )

        # Flow constraint
        logger.info("Adding flow constraint")
        route_list_set = set(route_list)
        for i in pop_id_set:
            model += (
                pulp.lpSum(flow[(i, j)] for j in pos_id_set if (i, j) in route_list_set)
                == access_pos[i],
                f"flow_constraint_{i}",
            )

        if self.limit_pos_num:
            logger.info("Number of POS constraint")
            model += (
                pulp.lpSum(optimal_pos[j] for j in pos_id_set) <= self.pos_num,
                "pos_num_constraint",
            )

        model.solve()

        logger.info(pulp.LpStatus[model.status])
        objective = pulp.value(model.objective)
        pos_df = self.df_from_dict(optimal_pos, ["pos", "selected"], objective, True)
        route_df = self.df_from_dict(flow, ["route", "assigned"], objective, True)
        return (route_df, pos_df)

    @staticmethod
    def df_from_dict(data, col_names, obj=None, get_value=False):
        if get_value:
            data = {k: v.varValue for k, v in data.items()}
        df = pd.DataFrame([data])
        df = df.T
        df = df.reset_index()
        df.columns = col_names
        if obj:
            df["total_cost"] = obj
        return df

    def run(self):
        with self.input().open() as src:
            pop_df, dc_df, distance_map, pop_id_set, pos_id_set = src.read()

        logger.info("Calculating walking cost")
        walking_cost_map = {
            i: {j: self.walking_cost(distance_map[i][j]) for j in pos_id_set}
            for i in pop_id_set
        }

        logger.info("Calculating distance limit")
        distance_limit_map = {
            i: {
                j: 1 if distance_map[i][j] <= self.max_walking_dist else 0
                for j in pos_id_set
            }
            for i in pop_id_set
        }

        logger.info("Calculating possible POS  coverage")
        access_pos = {
            i: 1 if sum(distance_limit_map[i].values()) >= 1 else 0 for i in pop_id_set
        }

        assert (pop_df["population"] >= 0).all(), "Missing or negative population"
        pop_df["population"] = pop_df["population"] * self.population_percent
        population_map = pop_df["population"].to_dict()

        assert (dc_df["rate USD/MT"] >= 0).all(), "Missing or negative freight rate"
        warehouse_pos_cost_map = dc_df["rate USD/MT"].to_dict()

        pop_df["quantity"] = self.quantity
        if self.quantity_depend_on_pop:
            pop_df["quantity"] = pop_df["quantity"] * pop_df["population"]

        assert (pop_df["quantity"] >= 0).all(), "Missing or negative quantity"
        quantity_map = pop_df["quantity"].to_dict()

        route_df, pos_df = self.get_optimal_pos(
            walking_cost_map,
            population_map,
            warehouse_pos_cost_map,
            quantity_map,
            distance_limit_map,
            access_pos,
            pop_id_set,
            pos_id_set,
            distance_map,
        )
        walking_cost_map = nested_to_record(walking_cost_map)
        walking_df = self.df_from_dict(walking_cost_map, ["route", "walking_cost"])
        route_df = route_df.merge(walking_df, on="route", how="outer")
        route_df["pop_index"], route_df["pos_index"] = zip(*route_df["route"])
        route_df = route_df.loc[route_df["assigned"] == 1]
        route_df = route_df.set_index("pop_index")
        pop_df = pop_df.merge(route_df, right_index=True, left_index=True, how="left")

        pos_df = pos_df.set_index("pos")
        dc_df = dc_df.merge(pos_df, right_index=True, left_index=True, how="left")
        dc_df["pos_index"] = dc_df.index
        dc_df = pd.DataFrame(dc_df)
        dc_df = dc_df.drop("geometry", axis=1)
        dc_df["timestamp"] = self.time.date_a
        dc_df["country"] = self.country_level
        dc_df = dc_df.rename(columns={"Dlatitude": "lat", "Dlongitude": "lng"})

        pop_df = pop_df.merge(
            dc_df[["pos_index", "rate USD/MT", "lat", "lng"]],
            on="pos_index",
            how="left",
        )

        uncovered_pop_per = (
            pop_df.loc[pd.isnull(pop_df["pos_index"]), "population"].sum()
            / pop_df["population"].sum()
        )

        for df in [pop_df, dc_df]:
            df["uncovered_pop_percent"] = uncovered_pop_per
        var_list = [
            "country",
            "lat",
            "lng",
            "timestamp",
            "selected",
            "total_cost",
            "uncovered_pop_percent",
        ]
        dc_df = dc_df[var_list]
        dc_df["admin1"] = self.admin1_name
        dc_df["max_walking_dist_scenario"] = self.max_walking_dist
        dc_df["walking_speed_scenario"] = self.walking_speed
        dc_df["daily_wage_scenario"] = self.daily_wage
        dc_df["population_percent_scenario"] = self.population_percent
        dc_df["cargo_type_scenario"] = self.cargo_type
        dc_df["setting_up_pos_cost_scenario"] = self.setting_up_pos_cost
        with self.output()["pos"].open("w") as out:
            dc_df.to_csv(out.name, index=False)
        if self.output_assigned_route:
            with self.output()["route"].open("w") as out:
                pop_df.to_csv(out.name, index=False)


@requires(PredictFreightRate)
class UploadFreightRateToCkan(Task):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"name": f"Freight rate {self.task_id}"},
        )

    def run(self):
        self.output().put(self.input().path)
