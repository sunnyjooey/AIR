import json
import os
from functools import reduce

import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
from kiluigi.targets import CkanTarget, ExpiringLocalTarget
from luigi import ExternalTask, LocalTarget, Task
from luigi.configuration import get_config
from luigi.util import requires
from models.accessibility_model.tasks import TravelTimeToTowns
from models.demand_model.mappings import (
    county_code_to_rasterize,
    state_code_to_rasterize,
)
from models.demand_model.tasks import CreateCaloricNeedsSurface
from models.economic_model.output import Combine as OutputEconomicModelResults
from models.hydrology_model.tasks import RainfallSimulation
from models.market_price_model.tasks import RasterizePrices
from shapely.ops import cascaded_union
from utils.geospatial_tasks.functions.geospatial import (
    convert_multiband_raster_to_csv_points,
    downsample_raster,
    rasterize_vectorfile,
)

try:
    with open(
        # it's always safer to work with absolute paths. __file__ is the current filename
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data/raster_proj.json"
        )
    ) as json_data:
        REF_PROJ = json.load(json_data)
except (NameError, FileNotFoundError):
    with open(
        # it's always safer to work with absolute paths. __file__ is the current filename
        os.path.join(os.getcwd(), "models/economic_model/data/raster_proj.json")
    ) as json_data:
        REF_PROJ = json.load(json_data)

CONFIG = get_config()
OUTPUT_PATH = CONFIG.get("paths", "output_path")


class AdminShapefilesCKAN(ExternalTask):
    """
    Pull state, county borders from CKAN
    """

    def output(self):
        return {
            "states": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "c323f572-3f97-4246-b8a7-8a92eaa5636b"},
            ),
            "counties": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "0cf2b13b-3c9f-4f4e-8074-93edc01ab1bd"},
            ),
        }


@requires(AdminShapefilesCKAN)
class RasterizeAdminShapefile(Task):
    def output(self):
        return [
            LocalTarget(os.path.join(OUTPUT_PATH, "rasterized_state.tif")),
            LocalTarget(os.path.join(OUTPUT_PATH, "rasterized_county.tif")),
        ]

    def run(self):
        # Get Shapefile Data
        countyShpZipPath = self.input()["counties"].path
        os.system(  # noqa S605
            "unzip -o -d {} {}".format(OUTPUT_PATH, countyShpZipPath)  # noqa S605
        )  # noqa: S605
        shp_fn_county = os.path.join(OUTPUT_PATH, "County.shp")

        # Chnage states to numbers
        gdf = gpd.read_file(shp_fn_county)
        gdf = gpd.GeoDataFrame(
            gdf.groupby("County", as_index=False).agg({"geometry": cascaded_union})
        )
        gdf = gdf.replace({"County": county_code_to_rasterize})
        gdf.to_file(shp_fn_county)

        stateShpZipPath = self.input()["states"].path
        os.system(  # noqa S605
            "unzip -o -d {} {}".format(OUTPUT_PATH, stateShpZipPath)  # noqa S605
        )  # noqa: S605
        shp_fn_state = os.path.join(OUTPUT_PATH, "State.shp")

        # Chnage states to numbers
        gdf = gpd.read_file(shp_fn_state)
        gdf = gpd.GeoDataFrame(
            gdf.groupby("State", as_index=False).agg({"geometry": cascaded_union})
        )
        gdf = gdf.replace({"State": state_code_to_rasterize})
        gdf.to_file(shp_fn_state)

        # Rasterize Shapefile
        rasterize_vectorfile(
            REF_PROJ, shp_fn_state, "State", fname_out=self.output()[0].path
        )
        rasterize_vectorfile(
            REF_PROJ, shp_fn_county, "County", fname_out=self.output()[1].path
        )


@requires(
    RasterizeAdminShapefile,
    RasterizePrices,
    OutputEconomicModelResults,
    CreateCaloricNeedsSurface,
    RainfallSimulation,
    TravelTimeToTowns,
)
class RunWorldModelersSystem(Task):

    dscale = luigi.FloatParameter(default=0.1)

    def output(self):
        targets = [
            # Needs
            ExpiringLocalTarget(
                os.path.join(OUTPUT_PATH, "needs_superset.csv"), timeout=30
            ),
            # Prices
            ExpiringLocalTarget(
                os.path.join(OUTPUT_PATH, "prices_superset.csv"), timeout=30
            ),
            # Soil Moisture
            ExpiringLocalTarget(
                os.path.join(OUTPUT_PATH, "soil_moisture.csv"), timeout=30
            ),
            # inundation
            ExpiringLocalTarget(
                os.path.join(OUTPUT_PATH, "inundation_extent.csv"), timeout=30
            ),
            # Travel Time
            ExpiringLocalTarget(
                os.path.join(OUTPUT_PATH, "travel_time_to_towns.csv"), timeout=30
            ),
        ]
        return targets

    def convert_raster(self, src_fname, admin_df, z):
        """
        Function to downscale, convert to csv of points, and merge with admin names.
        """
        if self.dscale != 1:
            src_fname = downsample_raster(src_fname, dscale=self.dscale)
        df = convert_multiband_raster_to_csv_points(src_fname, drop_na=False)
        df.columns = ["X", "Y", z]
        df = df.merge(admin_df, left_on=["X", "Y"], right_on=["X", "Y"], how="right")
        df = df.replace({-9999.0: np.nan})
        #         Remove downscaled raster
        if self.dscale != 1:
            os.remove(src_fname)

        return df

    def read_excel_map(self, excel_map):
        temp = {}
        for k, v in excel_map.items():
            temp[k] = pd.read_excel(v)
        df = pd.concat(temp)
        return df

    def run(self):
        files = {
            "admin": self.input()[0],
            "prices": [f.path for f in self.input()[1][2]],  # list of raster filepaths
            "econ": self.input()[2][3].path,  # raster filepath for cons exp
            "needs": self.input()[3].path,
            "soil": self.input()[4]["soil_moisture"].path,
            "inundation": self.input()[4]["inundation"].path,
            "travel_time": self.input()[5]["travel_time"].path,
        }

        state_admin_fn = downsample_raster(
            files["admin"][0].path, dscale=self.dscale, resampling="mode"
        )
        county_admin_fn = downsample_raster(
            files["admin"][1].path, dscale=self.dscale, resampling="mode"
        )
        state_admin_df = convert_multiband_raster_to_csv_points(
            state_admin_fn, drop_na=True
        )
        state_admin_df.columns = ["X", "Y", "State"]
        county_admin_df = convert_multiband_raster_to_csv_points(
            county_admin_fn, drop_na=True
        )
        county_admin_df.columns = ["X", "Y", "County"]
        admin_df = state_admin_df.merge(
            county_admin_df, left_on=["X", "Y"], right_on=["X", "Y"]
        )

        inverted_dict = {v: k for k, v in state_code_to_rasterize.items()}
        admin_df = admin_df.replace({"State": inverted_dict})
        inverted_dict = {v: k for k, v in county_code_to_rasterize.items()}
        admin_df = admin_df.replace({"County": inverted_dict})
        final_output = {}
        data_files = [f for f in list(files.keys()) if f != "admin"]

        # If this is a baseline run, just output baseline stats
        if self.PercentOfNormalRainfall == 1:

            # Downsample all the rasters and convert to csv
            for k in data_files:
                if type(files[k]) is str:
                    final_output[k] = self.convert_raster(
                        files[k], admin_df, "baseline"
                    )

                elif type(files[k]) is list:
                    final_output[k] = [
                        self.convert_raster(f, admin_df, "baseline") for f in files[k]
                    ]
                elif type(files[k]) is dict:
                    final_output[k] = self.read_excel_map(files[k])

        # If this is a scenario run, merge the scenario with the baseline
        elif self.PercentOfNormalRainfall != 1:

            # Do the scenario
            scenario = {}
            # Downsample all the rasters and convert to csv
            for k in data_files:
                if type(files[k]) is str:
                    scenario[k] = self.convert_raster(files[k], admin_df, "scenario")
                elif type(files[k]) is list:
                    scenario[k] = [
                        self.convert_raster(f, admin_df, "scenario") for f in files[k]
                    ]
                elif type(files[k]) is dict:
                    scenario[k] = self.read_excel_map(files[k])

            # Do the baseline
            baseline = {}
            # Convert the filenames to the baseline filenames
            for k in data_files:
                if type(files[k]) is str:
                    files[k] = (
                        files[k].split(f"_{self.PercentOfNormalRainfall}")[0]
                        + "_1.0.tif"
                    )
                elif type(files[k]) is list:
                    files[k] = [
                        f.split(f"_{self.PercentOfNormalRainfall}")[0] + "_1.0.tif"
                        for f in files[k]
                    ]
                elif type(files[k]) is dict:
                    files[k] = {
                        k: v.split(f"_{self.PercentOfNormalRainfall}")[0] + "_1.0.xlsx"
                        for k, v in files[k].items()
                    }

            # Downsample all the rasters and convert to csv
            for k in data_files:
                if type(files[k]) is str:
                    baseline[k] = self.convert_raster(files[k], admin_df, "baseline")
                elif type(files[k]) is list:
                    baseline[k] = [
                        self.convert_raster(f, admin_df, "baseline") for f in files[k]
                    ]
                elif type(files[k]) is dict:
                    baseline[k] = self.read_excel_map(files[k])

            # Merge baseline and scenario
            for k in data_files:
                if type(baseline[k]) is list:
                    final_output[k] = [None] * len(baseline[k])
                    for i in range(len(baseline[k])):
                        colname = (
                            files["prices"][i].split("_rasterized")[0].split("/")[-1]
                        )
                        baseline[k][i] = baseline[k][i].rename(
                            columns={"baseline": f"{colname}_baseline"}
                        )
                        scenario[k][i] = scenario[k][i].rename(
                            columns={"scenario": f"{colname}_scenario"}
                        )
                        final_output[k][i] = baseline[k][i].merge(
                            scenario[k][i],
                            left_on=["X", "Y", "State", "County"],
                            right_on=["X", "Y", "State", "County"],
                        )
                elif k == "crop":
                    final_output[k] = pd.concat([baseline[k], scenario[k]])
                else:
                    final_output[k] = baseline[k].merge(
                        scenario[k],
                        left_on=["X", "Y", "State", "County"],
                        right_on=["X", "Y", "State", "County"],
                    )

        # Merge lists into one dataframe
        for k in final_output.keys():
            if type(final_output[k]) is list:
                final_output[k] = reduce(
                    lambda x, y: pd.merge(x, y, on=["X", "Y", "State", "County"]),
                    final_output[k],
                )

        #         Merge econ and needs, rename columns
        final_output["econ"] = final_output["econ"].rename(
            columns={
                "baseline": "consumption_expenditure_baseline",
                "scenario": "consumption_expenditure_scenario",
            }
        )
        final_output["needs"] = final_output["needs"].rename(
            columns={"baseline": "in_need_baseline", "scenario": "in_need_scenario"}
        )
        final_output["needs"] = final_output["needs"].merge(
            final_output["econ"],
            left_on=["X", "Y", "State", "County"],
            right_on=["X", "Y", "State", "County"],
        )

        for k in final_output.keys():
            final_output[k] = final_output[k].set_index("State")

        # Output files locally and then to CKAN
        targets = {
            "prices": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "5d018019-7b77-458a-9e4d-cfa5b18519e4"},
            ),
            "needs": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "27b63201-e831-49c3-9927-92235174f0ec"},
            ),
            "soil": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "233e06b7-cc8e-4b0c-8450-f6fca4fe70e4"},
            ),
            "inundation": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "361d1cfe-50a6-4f4d-a404-2835aa2e172b"},
            ),
            "travel_time": CkanTarget(
                dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
                resource={"id": "	d82af432-f269-41ac-ba51-bdc782a6ff09"},
            ),
        }

        final_output["needs"].to_csv(self.output()[0].path)
        final_output["prices"].to_csv(self.output()[1].path)
        final_output["soil"].to_csv(self.output()[2].path)
        final_output["inundation"].to_csv(self.output()[3].path)
        final_output["travel_time"].to_csv(self.output()[4].path)

        # Upload to CKAN
        targets["needs"].put(self.output()[0].path)
        targets["prices"].put(self.output()[1].path)
        targets["soil"].put(self.output()[2].path)
        targets["inundation"].put(self.output()[3].path)
        targets["travel_time"].put(self.output()[4].path)
