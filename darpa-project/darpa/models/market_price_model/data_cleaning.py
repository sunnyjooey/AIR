import datetime
import os
import re

from datetime import datetime as dt
import shutil
import zipfile

# from ftplib import FTP  # noqa: S402 - ignore security check
from time import strptime

# from typing import List, Any
from itertools import product


import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from luigi.configuration import get_config

from luigi.util import requires
from shapely.geometry import mapping
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.ops import cascaded_union

from kiluigi.targets import CkanTarget, IntermediateTarget
from kiluigi.tasks import ExternalTask, Task
from models.market_price_model.utils import (
    datetime_schema,
    merge_with_shapefile,
    merge_varying_geo_with_shapefile,
    str_to_num,
    convert_prices_df_to_dict,
    uganda_geo_cleaning,
    sudan_geo_cleaning,
    somalia_geo_cleaning,
    merge_global_oil_prices,
    merge_fat_kenya_varying_geo_with_shapefile,
)
from utils.geospatial_tasks.functions import geospatial

# from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from models.market_price_model.Country_Choice import CountryParameters

from models.malnutrition_model.functions.helper_func import retrieve_file

from models.market_price_model.mappings import ss_admin_conversion

CONFIG = get_config()

COUNTRY_CHOICES = {
    "SS": "South Sudan",
    "ETH": "Ethiopia",
    "DJ": "Djibouti",
    "UG": "Uganda",
    "KE": "Kenya",
    "SD": "Sudan",
    "SO": "Somalia",
    "ER": "Eritrea",
}


class TabularFilesCKAN(ExternalTask):
    """
    Pull price and exchange rate data from CKAN
    """

    def output(self):
        return {
            "ss_prices": CkanTarget(
                dataset={"id": "023b0560-4058-49a4-8317-7b3535dbd4e0"},
                resource={"id": "25abce10-9bd2-4cca-bfe5-e538829fbe66"},
            ),
            "exchange_rates": CkanTarget(
                dataset={"id": "e45e6f64-05ca-4783-b0eb-e93b7dd8a708"},
                resource={"id": "a7458102-58ac-48e2-9f2f-7e2482445479"},
            ),
            "acled_africa": CkanTarget(
                dataset={"id": "fe67ac03-e905-4553-98bd-b368a1273184"},
                resource={"id": "63e1c2e2-8632-4950-a7a2-2843870c3954"},
            ),
            "adj_country_prices": CkanTarget(
                dataset={"id": "758d5563-2490-4e9e-a39e-0ca0174673ba"},
                resource={"id": "4bd5d3dc-f82d-49e5-86aa-8ca7a003a02d"},
            ),
            "ss_diesel_prices": CkanTarget(
                dataset={"id": "6e8ce492-be55-4d94-9716-5f96183b748f"},
                resource={"id": "9a05f92b-2efa-41cd-997f-66bcf99e0afe"},
            ),
            "ss_petrol_prices": CkanTarget(
                dataset={"id": "6e8ce492-be55-4d94-9716-5f96183b748f"},
                resource={"id": "1854bbfc-003a-4e56-9cc0-206962ceed75"},
            ),
            "commodity_groups": CkanTarget(
                dataset={"id": "7a389c34-25b4-44ed-99a6-6c28bcb62913"},
                resource={"id": "b63dad85-013b-44d9-b291-ea8a0945c87f"},
            ),
            "crop_production": CkanTarget(
                dataset={"id": "ca5e2bdb-b65d-47a6-83b9-025996108b39"},
                resource={"id": "aca71453-727d-4577-bb0d-d2a63d0371d8"},
            ),
            "eth_prices": CkanTarget(
                dataset={
                    "id": "d6e341d4-e68e-457a-b3af-eaf41d66f20f"
                },  # Old version from 2019 model
                resource={"id": "41370776-965a-4d64-805b-9fd955881681"},
            ),
            "eth_crop_production": CkanTarget(
                dataset={"id": "9ffe6d58-f60f-4ece-ac4c-79ad9c52b144"},
                resource={"id": "05034af2-1894-49c3-8bfe-7b10e1406079"},
            ),
            "eth_diesel_prices": CkanTarget(
                dataset={"id": "ddf2f709-c5a7-40c0-88c6-04df9c26bca3"},
                resource={"id": "bdaa9425-0b7c-43e9-b4b7-e7564971db1f"},
            ),
        }


class PriceFilesCKAN(ExternalTask):
    """
    Pull price data from CKAN
    """

    def output(self):
        return {
            "ss_prices": CkanTarget(
                dataset={
                    "id": "023b0560-4058-49a4-8317-7b3535dbd4e0"
                },  # 2019 from CKAN
                resource={"id": "25abce10-9bd2-4cca-bfe5-e538829fbe66"},
            ),
            "eth_prices": CkanTarget(
                dataset={
                    "id": "d6e341d4-e68e-457a-b3af-eaf41d66f20f"
                },  # Old version from 2019 model
                resource={"id": "41370776-965a-4d64-805b-9fd955881681"},
            ),
            "dj_price_WFP": CkanTarget(
                dataset={"id": "6d40f3eb-b1c8-48b2-b692-94b39d54d1b3"},
                # resource={"id": "e4c75b87-382e-47cb-9de4-6c9a19eedc91"}, # Old version till 2021
                resource={"id": "19951c35-6ec2-4584-b055-302713d81d11"},
            ),
            "dj_price_FEWS": CkanTarget(
                dataset={"id": "023b0560-4058-49a4-8317-7b3535dbd4e0"},
                resource={
                    "id": "25abce10-9bd2-4cca-bfe5-e538829fbe66"
                },  # Wrong path, don't use for now.
            ),
            "ss_prices_flat": CkanTarget(
                dataset={
                    "id": "023b0560-4058-49a4-8317-7b3535dbd4e0"
                },  # Downloaded from KDW
                resource={"id": "8c582fbc-ed2b-41d4-9744-8172ace55a5a"},
            ),
            ########################################################################
            # WFP data
            "eth_prices_wfp": CkanTarget(
                dataset={"id": "6b0f8623-81b3-4640-98f1-3ce45c285a65"},  # Ethiopia
                resource={"id": "33eb1086-6a64-43b2-acff-4c54e4d0558f"},
            ),
            "ke_prices": CkanTarget(
                dataset={"id": "6b0f8623-81b3-4640-98f1-3ce45c285a65"},  # Kenya
                resource={"id": "bc73307d-cb96-4808-9cab-251b77ccbc95"},
            ),
            "ug_prices": CkanTarget(
                dataset={"id": "6b0f8623-81b3-4640-98f1-3ce45c285a65"},  # Uganda
                resource={"id": "d7c3e0ba-7038-43ae-887e-a2470d6067aa"},
            ),
            "sd_prices": CkanTarget(
                dataset={"id": "6b0f8623-81b3-4640-98f1-3ce45c285a65"},  # Sudan
                resource={"id": "d3e69a0a-1df9-4dce-8ec8-0809216ece03"},
            ),
            "so_prices": CkanTarget(
                dataset={"id": "6b0f8623-81b3-4640-98f1-3ce45c285a65"},  # Somalia
                resource={"id": "e3421a0e-d179-4f88-bc66-c02570df322d"},
            ),
            "er_prices": CkanTarget(
                dataset={"id": "6b0f8623-81b3-4640-98f1-3ce45c285a65"},  # Eritrea
                resource={"id": "1701c055-320a-4284-9cb1-94d0cafa1b31"},
            ),
        }


class AdminxBoundaries(ExternalTask):
    """
    Pull admin borders from CKAN
    """

    def output(self):
        return {
            "eth": CkanTarget(
                dataset={"id": "d07b30c6-4909-43fa-914b-b3b435bef314"},  # ETH
                resource={"id": "d4804e8a-5146-48cd-8a36-557f981b073c"},
                # resource={"id": "227c0592-8fe9-44b8-bc47-1e5134c2a582"}, # Single file
            ),
            "ss": CkanTarget(
                dataset={
                    "id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"
                },  # SS county level
                resource={"id": "0cf2b13b-3c9f-4f4e-8074-93edc01ab1bd"},
            ),
            "dj": CkanTarget(
                dataset={"id": "cf5f891c-6fec-4509-9e3a-0ac69bb1b6e7"},  # Djibouti
                resource={"id": "412a2c5b-cee2-4e44-bd2a-2d8511834c8e"},
            ),
            "sd": CkanTarget(
                dataset={"id": "c055fdcb-9820-429d-8aac-440f84fd02ef"},  # Sudan
                resource={"id": "9ed3b234-63ca-401b-92a5-a1145291bde3"},
            ),
            "ke": CkanTarget(
                dataset={"id": "dd7275ad-4391-4fbb-a732-783d565ebfc1"},  # Kenya
                resource={"id": "8dae2ece-4ddc-43f3-ab90-847e01c8bfbd"},
            ),
            "ug": CkanTarget(
                dataset={"id": "dcd8b1bd-f6bd-4422-82b4-97d53337f68e"},  # Uganda
                resource={"id": "f93640e7-5543-4f75-9a53-d6d091692d34"},
            ),
            "so": CkanTarget(
                dataset={"id": "abbde93b-d00c-4e45-9a00-a958096a9b12"},  # Somalia
                resource={"id": "5cc93072-b77b-4973-a1d0-2d9a93e96e6b"},
            ),
            "er": CkanTarget(
                dataset={"id": "785eb6ef-87f5-4a62-aa90-b89b835e8805"},  # Eritrea
                resource={"id": "04ed5da4-1baf-449e-9c15-b2f564dda80b"},
            ),
        }


@requires(AdminxBoundaries, CountryParameters)
class ExtractBorderShapefile(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 365)

    def run(self):
        outpath = CONFIG.get("core", "cache_dir")
        ckan_path_names = self.input()[0]

        if self.country_level == COUNTRY_CHOICES["SS"]:
            adminx_shp_zip_path = ckan_path_names["ss"].path
            retrieve_file(adminx_shp_zip_path, ".zip", outpath)
            adminx_shp_path = (
                os.path.join(outpath, adminx_shp_zip_path.split("/")[-2]) + "/"
            )
            adminx_shp = gpd.read_file(os.path.join(adminx_shp_path, "County.shp"))
            adminx_shp = adminx_shp.rename(
                columns={"County": "adminx", "State": "admin1"}
            )
            adminx_shp = adminx_shp.groupby(["adminx", "admin1"], as_index=False).agg(
                {"geometry": cascaded_union}
            )

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            adminx_shp_zip_path = ckan_path_names["eth"].path
            retrieve_file(adminx_shp_zip_path, ".zip", outpath)
            adminx_shp_path = (
                os.path.join(outpath, adminx_shp_zip_path.split("/")[-2]) + "/"
            )
            adminx_shp = gpd.read_file(
                os.path.join(adminx_shp_path, "Ethiopia_adm2_uscb_2016.shp")
            )
            adminx_shp = adminx_shp.rename(columns={"admin2": "adminx"})

            print(adminx_shp)

            adminx_shp = adminx_shp[["adminx", "admin1", "geometry"]]
            adminx_shp.rename(
                index={
                    "Zone 01": "Zone 1 ",
                    "Zone  02": "Zone 2",
                    "Zone  03": "Zone 3",
                    "Zone 04": "Zone 4",
                    "Zone 05": "Zone 5",
                },
                inplace=True,
            )
            adminx_shp = (
                adminx_shp.explode().reset_index().rename(columns={0: "geometry"})
            )
            adminx_shp = adminx_shp[["adminx", "admin1", "geometry"]]

        elif self.country_level == COUNTRY_CHOICES["DJ"]:
            admin_shp_zip_path = ckan_path_names["dj"].path
            retrieve_file(admin_shp_zip_path, ".zip", outpath)
            admin_shp_path = (
                os.path.join(outpath, admin_shp_zip_path.split("/")[-2]) + "/"
            )
            admin_shp = gpd.read_file(
                os.path.join(admin_shp_path, "dji_admbnda_adm2_fews_v2.shp")
            )
            admin_shp = admin_shp.rename(columns={"admin2": "adminx"})
            adminx_shp = admin_shp.groupby("adminx", as_index=False).agg(
                {"geometry": cascaded_union}
            )
            adminx_shp = adminx_shp[["adminx", "admin1", "geometry"]]

        elif self.country_level == COUNTRY_CHOICES["KE"]:
            adminx_shp_zip_path = ckan_path_names["ke"].path
            retrieve_file(adminx_shp_zip_path, ".zip", outpath)
            adminx_shp_path = (
                os.path.join(outpath, adminx_shp_zip_path.split("/")[-2]) + "/"
            )
            adminx_shp = gpd.read_file(os.path.join(adminx_shp_path, "District.shp"))
            adminx_shp = adminx_shp.rename(
                columns={"FIRST_DIST": "adminx", "FIRST_COUN": "admin1"}
            )
            adminx_shp = adminx_shp.groupby(["adminx", "admin1"], as_index=False).agg(
                {"geometry": cascaded_union}
            )
            adminx_shp = adminx_shp[["adminx", "admin1", "geometry"]]

        elif self.country_level == COUNTRY_CHOICES["UG"]:
            adminx_shp_zip_path = ckan_path_names["ug"].path
            retrieve_file(adminx_shp_zip_path, ".zip", outpath)
            adminx_shp_path = (
                os.path.join(outpath, adminx_shp_zip_path.split("/")[-2]) + "/"
            )
            adminx_shp = gpd.read_file(
                os.path.join(adminx_shp_path, "uga_admbnda_adm2_ubos_20200824.shp")
            )
            adminx_shp = adminx_shp.rename(
                columns={"ADM2_EN": "adminx", "ADM1_EN": "admin1"}
            )
            adminx_shp = adminx_shp.groupby(["adminx", "admin1"], as_index=False).agg(
                {"geometry": cascaded_union}
            )
            adminx_shp = adminx_shp[["adminx", "admin1", "geometry"]]

        elif self.country_level == COUNTRY_CHOICES["SD"]:
            adminx_shp_zip_path = ckan_path_names["sd"].path
            retrieve_file(adminx_shp_zip_path, ".zip", outpath)
            adminx_shp_path = (
                os.path.join(outpath, adminx_shp_zip_path.split("/")[-2]) + "/"
            )
            adminx_shp = gpd.read_file(os.path.join(adminx_shp_path, "sud_pop3.shp"))
            adminx_shp = adminx_shp.rename(
                columns={"County_EN": "adminx", "STATE_EN": "admin1"}
            )
            adminx_shp = adminx_shp.groupby(["adminx", "admin1"], as_index=False).agg(
                {"geometry": cascaded_union}
            )

            adminx_shp = adminx_shp[["adminx", "admin1", "geometry"]]

        elif self.country_level == COUNTRY_CHOICES["SO"]:
            adminx_shp_zip_path = ckan_path_names["so"].path
            retrieve_file(adminx_shp_zip_path, ".zip", outpath)
            adminx_shp_path = (
                os.path.join(outpath, adminx_shp_zip_path.split("/")[-2]) + "/"
            )
            adminx_shp = gpd.read_file(os.path.join(adminx_shp_path, "SOM_adm2.shp"))
            adminx_shp = adminx_shp.rename(
                columns={"NAME_2": "adminx", "NAME_1": "admin1"}
            )
            adminx_shp = adminx_shp.groupby(["adminx", "admin1"], as_index=False).agg(
                {"geometry": cascaded_union}
            )

            adminx_shp = adminx_shp[["adminx", "admin1", "geometry"]]

        elif self.country_level == COUNTRY_CHOICES["ER"]:
            adminx_shp_zip_path = ckan_path_names["er"].path
            retrieve_file(adminx_shp_zip_path, ".zip", outpath)
            adminx_shp_path = (
                os.path.join(outpath, adminx_shp_zip_path.split("/")[-2]) + "/"
            )
            adminx_shp = gpd.read_file(os.path.join(adminx_shp_path, "eri_adm2.shp"))
            adminx_shp = adminx_shp.rename(columns={"ADM2_NAME": "adminx"})
            adminx_shp = adminx_shp.groupby(["adminx", "admin1"], as_index=False).agg(
                {"geometry": cascaded_union}
            )

        else:
            raise NotImplementedError

        with self.output().open("w") as dst:
            dst.write(adminx_shp)


class PullCHIRPSCKAN(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "ec53b367-881c-4cdb-a9a6-2abb31dfe82f"},
            resource={"id": "60a21a4b-9564-4fa1-84e4-cdcba137df00"},
        )


class PullGlobalCrudeOilPrices(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "2a36b35c-86c3-4db2-852c-0d291b6898b4"},
            resource={"id": "e27500f9-65f0-408d-9271-4b52340a1b42"},
        )


@requires(TabularFilesCKAN)
class PrepareExchangeRates(Task):
    """
    Clean monthly exchange rate data, convert to match formatting of price data
    """

    def load_exchange_rates(self, exchg_rate_dir):
        exchange_rates = {}
        for f in os.listdir(exchg_rate_dir):
            name = f.split(".")[0]
            df = pd.read_csv(os.path.join(exchg_rate_dir, f))
            df["Time"] = df["start_date"].apply(lambda d: pd.to_datetime(d))
            exchange_rates[name] = df
        return exchange_rates

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 30)

    def run(self):
        exchange_rates_zip_path = self.input()["exchange_rates"].path
        outpath = CONFIG.get("core", "cache_dir")

        retrieve_file(exchange_rates_zip_path, ".zip", outpath)
        exchange_rates_path = (
            os.path.join(outpath, exchange_rates_zip_path.split("/")[-2]) + "/"
        )

        exchange_rates_dir = os.path.join(exchange_rates_path, "exchangeRates")
        exchange_rates = self.load_exchange_rates(exchange_rates_dir)

        with self.output().open("w") as output:
            output.write(exchange_rates)


@requires(
    TabularFilesCKAN,
    PrepareExchangeRates,
    ExtractBorderShapefile,
    PriceFilesCKAN,
    PullGlobalCrudeOilPrices,
)
class PrepareCommodityPriceData(Task):
    """
    Cleans monthly price data for food commodities, petrol, and diesel; applies exchange rates
    to convert to 2011 USD. Returns panel where each entity is state (county)
    """

    def explode(self, indata):
        indf = gpd.GeoDataFrame.from_file(indata)
        outdf = gpd.GeoDataFrame(columns=indf.columns)
        for _i, row in indf.iterrows():
            if type(row.geometry) == Polygon:
                outdf = outdf.append(row, ignore_index=True)
            if type(row.geometry) == MultiPolygon:
                multdf = gpd.GeoDataFrame(columns=indf.columns)
                recs = len(row.geometry)
                multdf = multdf.append([row] * recs, ignore_index=True)
                for geom in range(recs):
                    multdf.loc[geom, "geometry"] = row.geometry[geom]
                outdf = outdf.append(multdf, ignore_index=True)
        return outdf

    def clean_price_data(self, df):

        data_start_ind = np.where(df[0] == "Year")[0][0]
        prices = df.iloc[data_start_ind:, :]
        prices.columns = prices.iloc[0, :]
        prices.drop(labels=prices.index[0], axis=0, inplace=True)

        if self.country_level == COUNTRY_CHOICES["ETH"]:
            prices["Month"] = prices["Month"].apply(lambda x: strptime(x, "%B").tm_mon)
            names = df.loc[df[0] == "State", 2:].values.tolist()[0]

            prices["Day"] = 1
            prices["Time"] = pd.to_datetime(prices[["Year", "Month", "Day"]])
            prices.drop(labels=["Year", "Month", "Day"], axis=1, inplace=True)
            prices.set_index("Time", inplace=True)
            prices.sort_index(inplace=True)

        elif self.country_level == COUNTRY_CHOICES["SS"]:
            prices["Month"] = prices["Month"].apply(lambda x: strptime(x, "%b").tm_mon)
            names = df.loc[df[0] == "County", 2:].values.tolist()[0]

            prices["Day"] = 1
            prices["Time"] = pd.to_datetime(prices[["Year", "Month", "Day"]])
            prices.drop(labels=["Year", "Month", "Day"], axis=1, inplace=True)
            prices.set_index("Time", inplace=True)
            prices.sort_index(inplace=True)

        prices.columns = names
        prices.replace({0: None}, inplace=True)
        prices = prices.applymap(str_to_num)

        null_count = prices.isna().values.sum()
        print(null_count)
        print(" ")

        prices.interpolate(method="linear", axis=0, inplace=True)
        null_count = prices.isna().values.sum()
        print(null_count)
        print(" ")
        prices = prices.T.reset_index()
        prices = prices.groupby("index").mean().T

        prices = prices.reset_index().melt(
            id_vars=["Time"],
            value_vars=prices.columns,
            var_name="Geography",
            value_name="price",
        )

        prices = prices.replace({"Geography": ss_admin_conversion})
        prices["Time"] = pd.to_datetime(prices["Time"])
        return prices

    def clean_price_data_wfp(self, df):
        cols = list(df.iloc[0, :])
        df.columns = cols
        df = df.drop(df.index[0:2])
        df = df.reset_index()
        df.rename(columns={"date": "Time"}, inplace=True)
        df.rename(columns={"admin2": "Geography"}, inplace=True)
        df["usdprice"] = df["usdprice"].astype(float)
        commodities = df["commodity"].unique()

        final_df = []
        for i in commodities:
            print(i)
            print("Null Counts Before & After Linear Interpolation")
            print(" ")
            df_temp = df[df["commodity"] == i].pivot_table(
                index="Time", columns=["Geography"], values="usdprice"
            )
            df_temp.sort_index(inplace=True)
            null_count = df_temp.isna().values.sum()
            print(null_count)
            print(" ")
            df_temp.interpolate(method="linear", axis=1, inplace=True)
            null_count = df_temp.isna().values.sum()
            print(null_count)
            print(" ")
            df_temp = df_temp.reset_index().melt(
                id_vars=["Time"], var_name="Geography", value_name="Price"
            )
            df_temp["Commodity"] = i
            df_temp["Time"] = pd.to_datetime(df_temp["Time"])
            df_temp["Time"] = df_temp["Time"].apply(
                lambda x: dt.combine(x, dt.min.time())
            )
            df_temp["Time"] = df_temp["Time"].apply(
                lambda x: x.strftime("%Y-%m-01T%H:%M:%S")
            )
            final_df.append(df_temp)
        final_df = pd.concat(final_df)

        return final_df

    def apply_exchange_rates(self, price_df, exchange_rate_df, commodity, ss=False):
        merged_df = price_df.merge(exchange_rate_df, on="Time")
        if ss:
            merged_df[f"p_{commodity}"] = merged_df["price"].multiply(
                merged_df["value"]
            )
        else:
            merged_df[f"p_{commodity}"] = merged_df["price_per_kg"].multiply(
                merged_df["value"]
            )
            merged_df[f"p_{commodity}"] = merged_df[f"p_{commodity}"].astype(float)
        merged_df = merged_df[["Geography", "Time", f"p_{commodity}"]]

        return merged_df

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 30)

    def run(self):
        # Reading commodity prices from the excel sheets
        if self.country_level == COUNTRY_CHOICES["ETH"]:
            sheet_names = pd.ExcelFile(self.input()[0]["eth_prices"].path).sheet_names
        elif self.country_level == COUNTRY_CHOICES["SS"]:
            sheet_names = pd.ExcelFile(self.input()[0]["ss_prices"].path).sheet_names
        elif self.country_level in [
            COUNTRY_CHOICES["KE"],
            COUNTRY_CHOICES["UG"],
            COUNTRY_CHOICES["SD"],
            COUNTRY_CHOICES["SO"],
            COUNTRY_CHOICES["ER"],
        ]:
            pass
        else:
            raise NotImplementedError

        with self.input()[1].open("r") as f:
            exchange_rates = f.read()

        with self.input()[2].open("r") as src:
            adminx_boundaries = src.read()
            # Aggregating the shape file to only create one row per unique geographic region
            # Need to update with better logic
            # adminx_boundaries = adminx_boundaries.sort_index().groupby("adminx").first()
            # adminx_boundaries = adminx_boundaries.reset_index()
        adminx_boundaries.drop(columns="admin1", inplace=True)

        if self.country_level == COUNTRY_CHOICES["ETH"]:

            # COMMODITY PRICES
            prices = pd.read_excel(
                self.input()[0]["eth_prices"].path, sheet_name=sheet_names, header=None
            )
            prices = {f"food_{k}": self.clean_price_data(v) for k, v, in prices.items()}
            prices = {
                k: self.apply_exchange_rates(
                    v, exchange_rates["Ethiopia"], commodity=k, ss=True
                )
                for k, v in prices.items()
            }

            count_before_merge = 0
            for b in prices.values():
                count_before_merge += b.shape[0]

            prices = {
                k: merge_with_shapefile(v, adminx_boundaries, gdf_on="adminx")
                for k, v in prices.items()
            }

            count_after_merge = 0
            for m in prices.values():
                count_after_merge += m.shape[0]

            print(" ")
            print("{0} Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

            # DIESEL PRICES
            diesel_prices = pd.read_csv(
                self.input()[0]["eth_diesel_prices"].path, header=None
            )
            diesel_prices = self.clean_price_data(diesel_prices)
            diesel_prices = self.apply_exchange_rates(
                diesel_prices, exchange_rates["Ethiopia"], commodity="diesel", ss=True
            )

            count_before_merge = diesel_prices.shape[0]

            diesel_prices["Geography"] = diesel_prices["Geography"].str.replace(
                "  ", " "
            )
            diesel_prices["Geography"] = diesel_prices["Geography"].apply(
                lambda x: str(x).title()
            )

            diesel_prices = merge_with_shapefile(
                diesel_prices, adminx_boundaries, gdf_on="adminx"
            )

            count_after_merge = diesel_prices.shape[0]

            print(" ")
            print("{0} Diesel Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

            prices["diesel"] = diesel_prices

        elif self.country_level == COUNTRY_CHOICES["SS"]:

            # FOOD PRICES
            prices = pd.read_excel(
                self.input()[0]["ss_prices"].path, sheet_name=sheet_names, header=None
            )
            prices = {f"food_{k}": self.clean_price_data(v) for k, v, in prices.items()}
            prices = {
                k: self.apply_exchange_rates(
                    v, exchange_rates["South Sudan"], commodity=k, ss=True
                )
                for k, v in prices.items()
            }

            count_before_merge = 0
            for b in prices.values():
                count_before_merge += b.shape[0]

            for k in prices:
                prices[k]["Geography"] = prices[k]["Geography"].str.replace("  ", " ")
                prices[k]["Geography"] = prices[k]["Geography"].str.strip()
                prices[k]["Geography"] = prices[k]["Geography"].apply(
                    lambda x: str(x).title()
                )
                prices[k] = prices[k].replace("Lafon", "Lopa")
                prices[k] = prices[k].replace("Panyijiar", "Panyijar")

            prices_geo = []
            for k, v in prices.items():
                raw = list(prices[k]["Geography"].unique())
                prices_geo.append(raw)

            prices = {
                k: merge_with_shapefile(v, adminx_boundaries, gdf_on="adminx")
                for k, v in prices.items()
            }

            count_after_merge = 0
            for m in prices.values():
                count_after_merge += m.shape[0]

            print(" ")
            print("{0} Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

            # DIESEL PRICES
            diesel_prices = pd.read_csv(
                self.input()[0]["ss_diesel_prices"].path, header=None
            )
            diesel_prices = self.clean_price_data(diesel_prices)

            diesel_prices = self.apply_exchange_rates(
                diesel_prices,
                exchange_rates["South Sudan"],
                commodity="diesel",
                ss=True,
            )

            count_before_merge = diesel_prices.shape[0]
            prices_geo = list(diesel_prices["Geography"].unique())

            diesel_prices = diesel_prices.replace("Lafon", "Lopa")
            diesel_prices = diesel_prices.replace("Panyijiar", "Panyijar")
            diesel_prices = diesel_prices.replace("Wullu", "Wulu")
            diesel_prices = diesel_prices.replace("Abiemhom", "Abiemnhom")

            diesel_prices = merge_with_shapefile(
                diesel_prices, adminx_boundaries, gdf_on="adminx"
            )

            count_after_merge = diesel_prices.shape[0]

            print(" ")
            print("{0} Diesel Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

            prices["diesel"] = diesel_prices

            # PETROL PRICES
            petrol_prices = pd.read_csv(
                self.input()[0]["ss_petrol_prices"].path, header=None
            )
            petrol_prices = self.clean_price_data(petrol_prices)
            petrol_prices = self.apply_exchange_rates(
                petrol_prices,
                exchange_rates["South Sudan"],
                commodity="petrol",
                ss=True,
            )

            count_before_merge = petrol_prices.shape[0]
            prices_geo = list(petrol_prices["Geography"].unique())
            petrol_prices = petrol_prices.replace("Lafon", "Lopa")
            petrol_prices = petrol_prices.replace("Panyijiar", "Panyijar")
            petrol_prices = petrol_prices.replace("Wullu", "Wulu")
            petrol_prices = petrol_prices.replace("Abiemhom", "Abiemnhom")

            petrol_prices = merge_with_shapefile(
                petrol_prices, adminx_boundaries, gdf_on="adminx"
            )

            count_after_merge = petrol_prices.shape[0]

            print(" ")
            print("{0} Petrol Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

            prices["petrol"] = petrol_prices

        elif self.country_level == COUNTRY_CHOICES["KE"]:

            # FOOD PRICES
            prices = pd.read_csv(self.input()[3]["ke_prices"].path, header=None)
            prices = self.clean_price_data_wfp(prices)
            prices["Geography"] = prices["Geography"].replace("Uasin Gishu", "Eldoret")

            count_before_merge = prices.shape[0]
            prices_geo_before_merge = list(prices["Geography"].unique())
            prices_temp = merge_with_shapefile(
                prices, adminx_boundaries, gdf_on="adminx"
            )

            prices_geo_after_merge = list(prices_temp["Geography"].unique())
            prices_geo_diff = list(
                set(prices_geo_before_merge) - set(prices_geo_after_merge)
            )
            prices = merge_varying_geo_with_shapefile(
                prices, adminx_boundaries, prices_geo_diff, gdf_on="adminx"
            )
            count_after_merge = prices.shape[0]

            print(" ")
            print("{0} Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

            # CRUDE OIL PRICES
            gop = pd.read_csv(self.input()[4].path, header=None)
            prices = merge_global_oil_prices(prices, gop, adminx_boundaries)
            # Commodities Without Enough Data. Removing for data storage/ speed
            remove_commodities = [
                "Meat (goat)",
                "Meat (beef)",
                "Salt",
                "Milk (cow, fresh)",
                "Maize flour",
                "Bananas",
                "Milk (UHT)",
                "Cooking fat",
                "Kale",
                "Tomatoes",
                "Cabbage",
                "Onions (red)",
                "Meat (camel)",
                "Rice",
                "Wheat flour",
                "Sugar",
                "Milk (camel, fresh)",
                "Milk (cow, pasteurized)",
                "Bread",
            ]
            prices = prices[~prices["Commodity"].isin(remove_commodities)]
            prices = convert_prices_df_to_dict(prices)

        elif self.country_level == COUNTRY_CHOICES["UG"]:

            # FOOD PRICES
            prices = pd.read_csv(self.input()[3]["ug_prices"].path, header=None)
            prices = self.clean_price_data_wfp(prices)
            prices = uganda_geo_cleaning(prices)

            count_before_merge = prices.shape[0]
            prices = merge_with_shapefile(prices, adminx_boundaries, gdf_on="adminx")
            count_after_merge = prices.shape[0]

            # CRUDE OIL PRICES
            gop = pd.read_csv(self.input()[4].path, header=None)
            prices = merge_global_oil_prices(prices, gop, adminx_boundaries)

            prices = convert_prices_df_to_dict(prices)

            print(" ")
            print("{0} Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

        elif self.country_level == COUNTRY_CHOICES["SD"]:

            # FOOD, GAS, & DIESEL PRICES
            prices = pd.read_csv(self.input()[3]["sd_prices"].path, header=None)
            prices = self.clean_price_data_wfp(prices)
            prices = sudan_geo_cleaning(prices)

            count_before_merge = prices.shape[0]
            prices = merge_with_shapefile(prices, adminx_boundaries, gdf_on="adminx")
            count_after_merge = prices.shape[0]

            print(" ")
            print("{0} Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

            # CRUDE OIL PRICES
            gop = pd.read_csv(self.input()[4].path, header=None)
            prices = merge_global_oil_prices(prices, gop, adminx_boundaries)

            # Commodities Without Enough Data. Removing for data storage/ speed
            remove_commodities = ["Exchange rate (unofficial)"]
            prices = prices[~prices["Commodity"].isin(remove_commodities)]
            prices = convert_prices_df_to_dict(prices)

        elif self.country_level == COUNTRY_CHOICES["SO"]:

            # FOOD, GAS, & DIESEL PRICES
            prices = pd.read_csv(self.input()[3]["so_prices"].path, header=None)
            prices = self.clean_price_data_wfp(prices)
            prices = somalia_geo_cleaning(prices)

            count_before_merge = prices.shape[0]
            prices = merge_with_shapefile(prices, adminx_boundaries, gdf_on="adminx")
            count_after_merge = prices.shape[0]

            print(" ")
            print("{0} Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

            # CRUDE OIL PRICES
            gop = pd.read_csv(self.input()[4].path, header=None)
            prices = merge_global_oil_prices(prices, gop, adminx_boundaries)
            # Commodities Without Enough Data. Removing for data storage/ speed
            remove_commodities = ["Sorghum", "Sugar (white)"]
            prices = prices[~prices["Commodity"].isin(remove_commodities)]
            prices = convert_prices_df_to_dict(prices)

        elif self.country_level == COUNTRY_CHOICES["ER"]:

            # FOOD, GAS, & DIESEL PRICES
            prices = pd.read_csv(self.input()[3]["er_prices"].path, header=None)
            prices = self.clean_price_data_wfp(prices)

            count_before_merge = prices.shape[0]
            prices = merge_with_shapefile(prices, adminx_boundaries, gdf_on="adminx")
            count_after_merge = prices.shape[0]

            print(" ")
            print("{0} Prices".format(self.country_level))
            print(" ")
            print("# prices rows before merge: ", count_before_merge)
            print(" ")
            print("# prices rows after merge: ", count_after_merge)
            print(" ")

            # CRUDE OIL PRICES
            gop = pd.read_csv(self.input()[4].path, header=None)
            prices = merge_global_oil_prices(prices, gop, adminx_boundaries)

            print(prices["Geography"].unique())
            print(prices["Commodity"].unique())
            prices = convert_prices_df_to_dict(prices)

        else:
            raise NotImplementedError

        for k, v in prices.items():
            # check that each file has expected columns
            expected_columns = ["Geography", "Time", f"p_{k}", "geometry"]
            missing_cols = set(expected_columns) - set(v.columns)
            assert set(expected_columns) == set(v.columns), f"{missing_cols} missing"

            # check that all prices non-negative
            assert (v[f"p_{k}"].fillna(0) >= 0).all(), "Prices must be non-negative"

        with self.output().open("w") as output:
            output.write(prices)


@requires(TabularFilesCKAN, ExtractBorderShapefile)
class PrepareFatalitiesData(Task):
    """
    Returns panel of fatalities by state by month.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 7)

    def run(self):

        conflict_data = pd.read_excel(self.input()[0]["acled_africa"].path, header=0)
        conflict = conflict_data.loc[conflict_data["COUNTRY"] == self.country_level]

        conflict["MONTH"] = conflict["EVENT_DATE"].apply(lambda x: x.month)
        conflict["DAY"] = 1
        conflict["Time"] = pd.to_datetime(conflict[["YEAR", "MONTH", "DAY"]])

        fatalities = conflict.groupby(["ADMIN2", "Time"], as_index=False).agg(
            {"FATALITIES": "sum"}
        )
        fatalities.columns = ["Geography", "Time", "fatalities"]

        with self.input()[1].open("r") as src:
            adminx_boundaries = src.read()

        datelist = pd.date_range("01-07-1997", "01-12-2019", freq="MS").tolist()
        temp_df = pd.DataFrame(
            list(product(datelist, adminx_boundaries["adminx"].unique())),
            columns=["Start", "adminx"],
        )
        out_df = temp_df.merge(adminx_boundaries, on="adminx")
        out_df = out_df.rename(columns={"Start": "Time", "adminx": "Geography"})
        if self.country_level == COUNTRY_CHOICES["KE"]:
            fat_geo_before_merge = list(fatalities["Geography"].unique())
            merged_temp = merge_with_shapefile(fatalities, adminx_boundaries, "adminx")
            fat_geo_after_merge = list(merged_temp["Geography"].unique())
            fat_geo_diff = list(set(fat_geo_before_merge) - set(fat_geo_after_merge))
            merged = merge_fat_kenya_varying_geo_with_shapefile(
                fatalities, adminx_boundaries, fat_geo_diff
            )
            if "GEO_for_join" in adminx_boundaries.columns:
                adminx_boundaries = adminx_boundaries.drop(columns="GEO_for_join")
        else:
            merged = merge_with_shapefile(fatalities, adminx_boundaries, "adminx")

        fatal_df = pd.DataFrame(merged.drop(columns="geometry"))
        fatal_df.drop_duplicates(
            subset=["Geography", "Time"], keep="first", inplace=True
        )
        fatal_df = fatal_df.set_index(["Geography", "Time"])
        out_df = out_df.set_index(["Geography", "Time"])

        merged_fatal = fatal_df.merge(out_df, on=["Geography", "Time"], how="outer")
        merged_fatal = merged_fatal.reset_index()

        merged_fatal.drop_duplicates(
            subset=["Geography", "Time"], keep="first", inplace=True
        )
        merged_fatal["fatalities"].fillna(0, inplace=True)
        merged_fatal.drop(columns=["geometry", "admin1_x", "admin1_y"], inplace=True)

        merged_fatal.columns = ["Geography", "Time", "fatalities"]

        merged_fatalities = merge_with_shapefile(
            merged_fatal, adminx_boundaries, "adminx"
        )
        if self.country_level == COUNTRY_CHOICES["SD"]:
            keep_admin = [
                "Gedaref",
                "El Damazeen",
                "Dongola",
                "El Fasher",
                "Jebel Awlya" "El Salam",
                "Kassala",
                "Kosti",
                "Nyala",
                "Zalingei",
            ]
            merged_fatalities = merged_fatalities[
                merged_fatalities["Geography"].isin(keep_admin)
            ]
        elif self.country_level == COUNTRY_CHOICES["SO"]:
            keep_admin = [
                "Afgooye",
                "Afmadow",
                "Balcad",
                "Bander-Beyla",
                "Baydhabo",
                "Beled Weyn",
                "Boorama",
                "Bosaaso",
                "Buulo Burdo",
                "Dhuusamareeb",
                "Diinsoor",
                "Dolow",
                "Eyl",
                "Gaalkacayo",
                "Garoowe",
                "Hargeysa",
                "Jariiban",
                "Jawhar",
                "Kismaayo",
                "Lascaanod",
                "Marka",
                "Qardho",
                "Xudur",
                "Qoryooley",
            ]
            merged_fatalities = merged_fatalities[
                merged_fatalities["Geography"].isin(keep_admin)
            ]

        merged_fatalities.drop(columns="admin1", inplace=True)

        expected_cols = ["Geography", "Time", "fatalities", "geometry"]
        assert set(expected_cols) == set(
            merged_fatalities.columns
        ), f"{set(expected_cols) - set(merged_fatalities.columns)} missing"
        assert (
            merged_fatalities["fatalities"] >= 0
        ).all(), "Fatalities must be non-negative"
        print(merged_fatalities["Geography"].unique())

        with self.output().open("w") as output:
            output.write({"Fatalities": merged_fatalities})


# @requires(ScrapeRainfallData, ExtractBorderShapefile)
@requires(PullCHIRPSCKAN, ExtractBorderShapefile)
class PrepareRainfallData(Task):
    """
    Takes raw rainfall rasters and produces monthly estimates at state level
    """

    @staticmethod
    def extract_date(s):
        year, month = re.search(r"chirps-v2\.0\.(.*)\.tif", s).group(1).split(".")
        return datetime.datetime(int(year), int(month), 1)

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 365)

    def run(self):
        with self.input()[1].open("r") as src:
            adminx_boundaries = src.read()
        outfolder = os.path.join(
            CONFIG.get("paths", "output_path"), "marketprice_model_chirps_dir"
        )

        with zipfile.ZipFile(self.input()[0].path, "r") as zip_ref:
            zip_ref.extractall(outfolder)

        sorted_files = {}
        file_list = os.listdir(outfolder)
        for key in file_list:
            sorted_files[key] = outfolder + "/" + key

        chirps_dates = [self.extract_date(d) for d in sorted_files.keys()]
        chirps_dates_no_dup = []
        for num in chirps_dates:
            if num not in chirps_dates_no_dup:
                chirps_dates_no_dup.append(num)

        rainfall = pd.DataFrame(
            index=chirps_dates_no_dup, columns=adminx_boundaries["adminx"]
        )
        i = 0
        for v in sorted_files.values():
            if ".gz" not in v:
                with rasterio.open(v) as src:
                    rf = adminx_boundaries["geometry"].apply(
                        lambda x: geospatial.calculate_mask_stats(
                            src, mapping(x), 0, 0
                        )["mean"]
                    )
                rainfall.iloc[i] = rf.values
                i += 1
        rainfall = rainfall.reset_index().melt(
            id_vars=["index"],
            value_vars=rainfall.columns,
            var_name="Geography",
            value_name="rainfall",
        )
        rainfall.rename(columns={"index": "Time"}, inplace=True)
        rainfall["Time"] = pd.to_datetime(rainfall["Time"])
        rainfall["rainfall"] = rainfall["rainfall"].astype(float)
        if self.country_level == COUNTRY_CHOICES["KE"]:
            rainfall["Geography"] = rainfall["Geography"].apply(
                lambda x: str(x).title()
            )
            keep_admin = [
                "Kilifi",
                "Baringo",
                "Kisumu East",
                "Mandera East",
                "Nairobi East",
                "Tana River",
                "Eldoret East",
                "Wajir North",
                "Garissa",
                "Kisumu West",
                "Mandera West",
                "Nairobi North",
                "Turkana Central",
                "Eldoret West",
                "Wajir South",
                "Kajiado Central",
                "Kitui",
                "Marsabit",
                "Nairobi West",
                "Turkana North",
                "Isiolo",
                "Wajir West",
                "Kajiado North",
                "Mandera Central",
                "Mombasa",
                "Nakuru",
                "Turkana South",
            ]
            rainfall = rainfall[rainfall["Geography"].isin(keep_admin)]
        elif self.country_level == COUNTRY_CHOICES["SD"]:
            keep_admin = [
                "Gedaref",
                "El Damazeen",
                "Dongola",
                "El Fasher",
                "Jebel Awlya" "El Salam",
                "Kassala",
                "Kosti",
                "Nyala",
                "Zalingei",
            ]
            rainfall = rainfall[rainfall["Geography"].isin(keep_admin)]
        elif self.country_level == COUNTRY_CHOICES["SO"]:
            keep_admin = [
                "Afgooye",
                "Afmadow",
                "Balcad",
                "Bander-Beyla",
                "Baydhabo",
                "Beled Weyn",
                "Boorama",
                "Bosaaso",
                "Buulo Burdo",
                "Dhuusamareeb",
                "Diinsoor",
                "Dolow",
                "Eyl",
                "Gaalkacayo",
                "Garoowe",
                "Hargeysa",
                "Jariiban",
                "Jawhar",
                "Kismaayo",
                "Lascaanod",
                "Marka",
                "Qardho",
                "Xudur",
                "Qoryooley",
            ]
            rainfall = rainfall[rainfall["Geography"].isin(keep_admin)]
        else:
            pass

        merged = merge_with_shapefile(rainfall, adminx_boundaries, "adminx")
        merged.drop(columns="admin1", inplace=True)

        expected_cols = ["Geography", "Time", "rainfall", "geometry"]
        assert set(expected_cols) == set(
            merged.columns
        ), f"{set(expected_cols) - set(merged.columns)} missing"
        assert (merged["rainfall"] >= 0).all(), "Rainfall values must be non-negative"

        with self.output().open("w") as output:
            output.write({"Rainfall": merged})


@requires(TabularFilesCKAN, ExtractBorderShapefile)
class PrepareCropProductionData(Task):
    """
    Clean Climis crop data
    """

    def clean_crop_data(self, crop_data_dir):

        files = sorted(os.listdir(crop_data_dir))
        all_dfs = []
        for f in files:
            year = int(f.split("_")[2].split(".")[0])
            df = pd.read_csv(os.path.join(crop_data_dir, f))
            empty_rows = np.where(df["State/County"].isnull())[0]
            state_rows = empty_rows + 1
            state_rows = state_rows[:-1]
            keep_cols = [
                "State/County",
                " {} Net Cereal production (Tonnes)".format(year),
            ]
            df = df.loc[state_rows, keep_cols]
            df.columns = ["State", "Net Cereal Production (Tonnes)"]
            df["State"] = df["State"].apply(lambda s: s.strip())
            df["year"] = year
            month_dfs = []
            for m in np.arange(1, 13):
                new_df = df.copy()
                new_df["month"] = m
                new_df["day"] = 1
                new_df["Time"] = pd.to_datetime(new_df[["year", "month", "day"]])
                new_df.drop(["year", "month", "day"], axis=1, inplace=True)
                month_dfs.append(new_df)

            all_dfs.append(pd.concat(month_dfs))
        combined = pd.concat(all_dfs)
        combined["Net Cereal Production (Tonnes)"] = combined[
            "Net Cereal Production (Tonnes)"
        ].apply(lambda x: str_to_num(x))
        combined.rename(
            columns={"Net Cereal Production (Tonnes)": "crop_prod"}, inplace=True
        )

        combined = combined.replace(
            {
                "State": {
                    "Northern Bahr El Ghazal": "Northern Bahr el Ghazal",
                    "Western Bahr El Ghazal": "Western Bahr el Ghazal",
                }
            }
        )

        combined.rename(columns={"State": "Geography"}, inplace=True)

        return combined

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):

        if self.country_level == COUNTRY_CHOICES["SS"]:

            crop_zip_path = self.input()[0]["crop_production"].path
            outpath = CONFIG.get("core", "cache_dir")

            retrieve_file(crop_zip_path, ".zip", outpath)
            crop_path = os.path.join(outpath, crop_zip_path.split("/")[-2]) + "/"

            cleaned = self.clean_crop_data(crop_path)

        elif self.country_level == COUNTRY_CHOICES["KE"]:

            crop_zip_path = self.input()[0]["crop_production"].path
            outpath = CONFIG.get("core", "cache_dir")

            retrieve_file(crop_zip_path, ".zip", outpath)
            crop_path = os.path.join(outpath, crop_zip_path.split("/")[-2]) + "/"

            cleaned = self.clean_crop_data(crop_path)

        elif self.country_level == COUNTRY_CHOICES["ETH"]:
            crop_prod = pd.read_csv(
                self.input()[0]["eth_crop_production"].path, header=0
            )
            crop_df = pd.DataFrame(
                crop_prod,
                columns=["Zone", "Year", " Category", "Crop", "Production in quintal"],
            )
            crop_df = crop_df.rename(
                columns={"Production in quintal": "crop_prod", " Category": "Category"}
            )

            crop_df = crop_df[
                crop_df["crop_prod"].notnull() & (crop_df["Category"] == "Cereals")
            ]
            crop_df["crop_prod"] = pd.to_numeric(
                crop_df["crop_prod"], downcast="float", errors="coerce"
            )
            crop_df = crop_df[crop_df["crop_prod"] >= 5000]

            cleaned = (
                crop_df.groupby(["Zone", "Year", "Category"])["crop_prod"]
                .agg("sum")
                .reset_index()
            )
            cleaned = cleaned[["Zone", "Year", "crop_prod"]]
            cleaned["Year"] = cleaned.Year.str[5:]

            all_dfs = []
            month_dfs = []
            for m in np.arange(1, 13):
                new_df = cleaned.copy()
                new_df["month"] = m
                new_df["day"] = 1
                new_df["Time"] = pd.to_datetime(new_df[["Year", "month", "day"]])
                new_df.drop(["Year", "month", "day"], axis=1, inplace=True)
                month_dfs.append(new_df)

            all_dfs.append(pd.concat(month_dfs))
            combined = pd.concat(all_dfs)
            combined.rename(columns={"Zone": "Geography"}, inplace=True)
            cleaned = combined
        else:
            raise NotImplementedError

        with self.input()[1].open("r") as src:
            adminx_boundaries = src.read()

        merged = merge_with_shapefile(cleaned, adminx_boundaries, "adminx")

        expected_cols = ["Geography", "Time", "crop_prod", "geometry"]
        assert set(expected_cols) == set(
            merged.columns
        ), f"{set(expected_cols) - set(merged.columns)} missing"
        assert (merged["crop_prod"] >= 0).all(), "Crop production must be non-negative"

        with self.output().open("w") as output:
            output.write({"Crop Production": merged})


@requires(
    PrepareCommodityPriceData,
    PrepareRainfallData,
    PrepareFatalitiesData,
    # PrepareCropProductionData,
)
class WriteCleanedPriceModelData(Task):
    """
    This task consolidates all of the data used as input to the price model and writes it to a single directory.
    Requires all preprocessing tasks.
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=60 * 60 * 24 * 5)

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            for prep_task in self.input():
                with prep_task.open("r") as src:
                    d = src.read()
                    for k, v in d.items():
                        # write vector files
                        if isinstance(v, gpd.GeoDataFrame):
                            schema = datetime_schema(v)
                            print(f"{k}.geojson")
                            v.to_file(
                                os.path.join(tmpdir, f"{k}.geojson"),
                                driver="GeoJSON",
                                schema=schema,
                            )
                        # write rasters (actually, just copy them over)
                        elif isinstance(v, str) and v.endswith(".tif"):
                            shutil.copy(v, os.path.join(tmpdir, k))
                        else:
                            raise TypeError(
                                "Object must be GeoDataFrame or path to .tif file"
                            )
