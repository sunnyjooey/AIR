import json
import zipfile

import geopandas as gpd
import luigi
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from luigi import ExternalTask, Task
from luigi.configuration import get_config as luigi_config
from luigi.util import requires
from mpl_toolkits.axes_grid1 import make_axes_locatable
from rasterstats import zonal_stats
from sklearn.linear_model import LinearRegression

from kiluigi.targets import CkanTarget, IntermediateTarget
from kiluigi.tasks import ReadDataFrameTask
from models.malnutrition_model.pipeline import FullResPopRaster
from models.population_model.pipeline import CreateFullResPopRaster

CACHE_DIR = luigi_config().get("core", "cache_dir")
mpl.rcParams["svg.fonttype"] = "none"


@requires(FullResPopRaster)
class CalculateTherapeuticNeed(luigi.Task):
    def output(self):
        return IntermediateTarget(f"therapeutic_needs_stats_{self.year}.txt")

    def run(self):
        with rasterio.open(self.input().path, "r") as mal_rast:
            mal_rast.bounds
            mal_rast.crs

            # mal_array = mal_rast.read(1)

        therapeutic_needs_stats = {"gam": 4588, "sam": 78888}

        with open(self.output().path, "w") as jsonfile:
            jsonfile.write(json.dumps(therapeutic_needs_stats))


class DownloadSudanAdminShapefiles(ExternalTask):
    """
    Get all South Sudan county admin boundary shapefiles from CKAN.
    """

    def output(self):
        return {
            "Conuty": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "0cf2b13b-3c9f-4f4e-8074-93edc01ab1bd"},
            )
        }


@requires(DownloadSudanAdminShapefiles)
class ExtractAdminShapefile(Task):
    """
    Unzip shapefiles and read in vector data as geopandas dataframes.
    """

    def output(self):
        return IntermediateTarget("ss_admin", task=self, timeout=1000)

    def run(self):

        # Cycle through input files to unzip
        for v in self.input().values():
            fp = v.path
            shp_fn = fp.split("/")[-1].replace("zip", "shp").capitalize()
            zip_ref = zipfile.ZipFile(fp, "r")
            zip_ref.extractall()

            try:
                file = zip_ref.extract(shp_fn)
            except KeyError:
                shp_fn = shp_fn.split(".")[0].upper() + "." + shp_fn.split(".")[1]
                file = zip_ref.extract(shp_fn)

        self.output().put(file)


class PullCostModelData(ExternalTask):
    """
    https://data.kimetrica.com/dataset/4178f84b-c40e-4aa8-ae8f-bea71beffffa/resource/b6bd40f5-8f6c-4629-b8c7-91bd27ccad7f/download/analogous-cost-model.xlsx
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "4178f84b-c40e-4aa8-ae8f-bea71beffffa"},
            resource={"id": "b6bd40f5-8f6c-4629-b8c7-91bd27ccad7f"},
        )


@requires(PullCostModelData)
class ReadCostModelData(ReadDataFrameTask):

    read_method = "read_excel"
    timeout = 60

    def output(self):
        return IntermediateTarget("cost_model_data", task=self, timeout=self.timeout)


@requires(ReadCostModelData)
class CreateCostModel(Task):

    timeout = 60

    def output(self):
        return IntermediateTarget("cost_model", task=self, timeout=self.timeout)

    def run(self):
        df = self.input().get()
        df = df.dropna(
            subset=["intervention_type", "number_of_recipients", "total_cost"]
        )

        # Create additional variables
        df["ln_total_cost"] = np.log(df["total_cost"])
        df["year"] = df["intervention_from_date"].apply(lambda x: str(x.year))

        # Split the data into Cash and Food
        data = {}
        data["Cash Transfer"] = df.loc[
            df["intervention_type"] == "Cash transfer"
        ].copy()
        data["Food Distribution"] = df.loc[
            df["intervention_type"] == "Free food distribution"
        ].copy()

        # Define variables
        cont_var = ["number_of_recipients", "duration_in_days"]
        cat_var = ["country", "year"]
        dep_var = "ln_total_cost"

        models = {}
        for k, d in data.items():
            # Encode categorical data
            cat_data = pd.get_dummies(d[cat_var])
            # Define the dependent variables
            var_df = pd.concat([d[cont_var], cat_data], axis=1)
            # Split data into testing and trainig
            X = np.array(var_df)
            y = np.array(d[dep_var])
            # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0, random_state=None)

            # Run regression
            reg = LinearRegression()
            reg.fit(X, y)

            models[k] = {"fit": reg, "X": X, "y": y}

        output_dict = {
            "models": models,
            "cat_var": cat_var,
            "cont_var": cont_var,
            "dep_var": dep_var,
            "columns": list(var_df.columns),
        }

        f, axes = plt.subplots(ncols=2, figsize=[14, 6])

        for k, i in zip(data.keys(), [0, 1]):
            y = np.exp(models[k]["y"])
            y_pred = np.exp(models[k]["fit"].predict(models[k]["X"]))
            r_score = models[k]["fit"].score(models[k]["X"], models[k]["y"])

            # Set up Axes
            axes[i].set_aspect("equal")
            min_val = np.min(np.concatenate([y, y_pred]))
            max_val = np.max(np.concatenate([y, y_pred]))
            axes[i].set_xlim([min_val * 0.7, max_val * 1.5])
            axes[i].set_ylim([min_val * 0.7, max_val * 1.5])

            (diag_line,) = axes[i].plot(
                axes[i].get_xlim(), axes[i].get_ylim(), ls="--", c="Grey", alpha=0.5
            )

            axes[i].plot(y, y_pred, "o", color="#003a96", ms=6)
            axes[i].set_xscale("log")
            axes[i].set_yscale("log")
            axes[i].set_title(k, fontsize=13)
            axes[i].set_ylabel("Predicted Total Cost ($)", fontsize=12)
            axes[i].set_xlabel("Actual Total Cost ($)", fontsize=14)
            axes[i].text(
                min_val,
                max_val,
                "R2 = {0:.2f} \n Base Cost = ${1:,.2f} \n N = {2:.0f}".format(
                    r_score,
                    np.exp(models[k]["fit"].intercept_),
                    models[k]["X"].shape[0],
                ),
                horizontalalignment="left",
                verticalalignment="top",
                fontsize=12,
            )

        self.output().put(output_dict)


@requires(CreateCostModel)
class PlotCostModel(Task):
    def output(self):
        return IntermediateTarget("cost_model.png", task=self)

    def run(self):
        models = self.input().get()["models"]

        f, axes = plt.subplots(ncols=2, figsize=[14, 6])

        for k, i in zip(models.keys(), [0, 1]):
            y = np.exp(models[k]["y"])
            y_pred = np.exp(models[k]["fit"].predict(models[k]["X"]))
            r_score = models[k]["fit"].score(models[k]["X"], models[k]["y"])

            # Set up Axes
            axes[i].set_aspect("equal")
            min_val = np.min(np.concatenate([y, y_pred]))
            max_val = np.max(np.concatenate([y, y_pred]))
            axes[i].set_xlim([min_val * 0.7, max_val * 1.5])
            axes[i].set_ylim([min_val * 0.7, max_val * 1.5])

            (diag_line,) = axes[i].plot(
                axes[i].get_xlim(), axes[i].get_ylim(), ls="--", c="Grey", alpha=0.5
            )

            axes[i].plot(y, y_pred, "o", color="#003a96", ms=6)
            axes[i].set_xscale("log")
            axes[i].set_yscale("log")
            axes[i].set_title(k, fontsize=13)
            axes[i].set_ylabel("Predicted Total Cost ($)", fontsize=12)
            axes[i].set_xlabel("Actual Total Cost ($)", fontsize=14)
            axes[i].text(
                min_val,
                max_val,
                "R2 = {0:.2f} \n Base Cost = ${1:,.2f} \n N = {2:.0f}".format(
                    r_score,
                    np.exp(models[k]["fit"].intercept_),
                    models[k]["X"].shape[0],
                ),
                horizontalalignment="left",
                verticalalignment="top",
                fontsize=12,
            )
        plt.savefig(self.output().path, bbox_inches="tight", dpi=300)


class ConvertMalnutritionToCountyLevel(Task):
    """
    Mask raster data with county level rasters.
    """

    year = luigi.IntParameter()
    # Parameter for selecting only 0-5 year olds
    population_fraction = luigi.FloatParameter(default=0.16)

    def requires(self):
        return {
            "malnutrition": FullResPopRaster(year=self.year),
            "population": CreateFullResPopRaster(year=self.year),
            "admin": ExtractAdminShapefile(),
        }

    def output(self):
        return IntermediateTarget("county_stats", task=self, timeout=100)

    def run(self):
        raster_files = {
            "malnutrition": self.input()["malnutrition"],
            "population": self.input()["population"],
        }
        shp_fn = self.input()["admin"].get()
        counties = gpd.read_file(shp_fn)["County"]

        data = {}
        for k, f in raster_files.items():
            data[k] = pd.DataFrame(index=counties, columns=[k])
            temp = zonal_stats(shp_fn, f.path, stats=["sum"])
            for i, j in zip(counties, np.arange(0, len(temp))):
                data[k].loc[i, k] = temp[j]["sum"]
        df = data["malnutrition"].merge(
            data["population"], left_index=True, right_index=True
        )
        df["population"] = df["population"] * self.population_fraction
        df["percent"] = df["malnutrition"] / df["population"]

        self.output().put(df)


@requires(ExtractAdminShapefile, ConvertMalnutritionToCountyLevel)
class PlotMalnutritionData(Task):
    """
    """

    scenario = luigi.Parameter(default="Normal")
    year = luigi.IntParameter()

    def output(self):
        return IntermediateTarget(
            "map_plot_{year}.png".format(year=self.year), task=self
        )

    def run(self):
        shp_fn = self.input()[0].get()
        df = self.input()[1].get()
        gdf = gpd.read_file(shp_fn)
        df = gdf.merge(df, left_on="County", right_index=True)

        f, ax = plt.subplots(figsize=[10, 10])
        df.plot(ax=ax, cmap="bone", column="malnutrition")
        ax.set_aspect("equal")
        ax.set_title(self.scenario, fontsize=16)

        divider = make_axes_locatable(ax)
        cax = divider.append_axes("bottom", size="5%", pad=0.50)
        vmin = np.min(df["malnutrition"])
        vmax = np.max(df["malnutrition"])
        sm = plt.cm.ScalarMappable(
            cmap="bone", norm=mpl.colors.Normalize(vmin=vmin, vmax=vmax)
        )
        sm._A = []
        cbar1 = f.colorbar(sm, cax=cax, orientation="horizontal")
        cbar1.ax.set_xlabel("Population in Need", fontsize=13, labelpad=15)

        plt.savefig(self.output().path, bbox_inches="tight")


class CalculateInterventionCost(Task):
    """
    Mask raster data with county level rasters.
    """

    coverage_rate = luigi.FloatParameter(default=1.0)
    intervention_type = luigi.ChoiceParameter(
        choices=["Cash Transfer", "Food Distribution"],
        var_type=str,
        default="Cash Transfer",
    )
    duration_days = luigi.IntParameter(default=365)
    household_size = luigi.FloatParameter(default=6)
    year = luigi.IntParameter()

    def requires(self):
        return {
            "data": ConvertMalnutritionToCountyLevel(year=self.year),
            "model": CreateCostModel(),
        }

    def output(self):
        filename = "intervention_cost_{coverage_rate}_{intervention_type}_{duration_days}_{household_size}_{year}.csv"
        params = {
            "coverage_rate": self.coverage_rate,
            "intervention_type": self.intervention_type,
            "duration_days": self.duration_days,
            "household_size": self.household_size,
            "year": self.year,
        }
        return IntermediateTarget(filename.format(**params), task=self)

    def run(self):
        model_output = self.input()["model"].get()
        model = model_output["models"][self.intervention_type]["fit"]

        df = pd.DataFrame(columns=model_output["columns"], index=[0]).fillna(0)

        pop_data = self.input()["data"].get()
        in_need = np.sum(pop_data["malnutrition"]) * self.coverage_rate

        df.loc[0, "number_of_recipients"] = in_need * self.household_size
        df.loc[0, "duration_in_days"] = self.duration_days
        df.loc[0, "country_South Sudan"] = 1
        df.loc[0, "year_2017"] = 1

        X = np.array(df)
        cost = np.exp(model.predict(X))

        output_dict = {
            "Cost": cost[0],
            "Intervention Type": self.intervention_type,
            "Number of Children": in_need * self.household_size,
            "Number of Households": in_need,
        }

        output_df = pd.DataFrame(output_dict, index=[0])

        output_df.to_csv(self.output().path)
