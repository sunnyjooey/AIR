import datetime

import geopandas as gpd
import pandas as pd
from fuzzywuzzy import process
from kiluigi.targets import CkanTarget, IntermediateTarget, LocalTarget
from kiluigi.tasks import ExternalTask, Task
from luigi.util import inherits, requires

from models.economic_model.data.mappings import (
    month_to_date,
    ss_hfs_options,
    ss_state_spelling,
)

# What is this pointing too?
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from .mappings import (
    DHS_toilet_facility_map,
    DHS_water_source_map,
    dji_2012_label,
    drinking_water_classification,
    ethiopia_month_name_map,
    french_t_english,
    region_code_uganda,
    sanitation_classification,
    source_drinking_water_uganda_2009,
    source_drinking_water_uganda_2010_2011,
    source_drinking_water_uganda_2013_2015,
    source_drinking_water_uganda_2018_2019,
    source_drinking_water_w2_w3_ss,
    source_drinking_water_w4_ss,
    toilet_facilities_shared,
    toilet_facilities_uganda_2009,
    toilet_facilities_uganda_2010_2011,
    toilet_facilities_uganda_2013_2015,
    toilet_facilities_uganda_2018_2019,
    toilet_facilities_w2_w3_ss,
    toilet_facilities_w4_ss,
    wave_1_label,
    wave_3_label,
)
from .utils import read_data, to_gregorian


@inherits(GlobalParameters)
class PullAdminShapefile(ExternalTask):
    def output(self):
        if self.country_level == "Ethiopia":
            return CkanTarget(
                dataset={"id": "c3d86261-d32b-44eb-b8d1-b34ea30a8d07"},
                resource={"id": "db916f78-ec37-417b-88a2-9f42d832490e"},
            )
        elif self.country_level == "South Sudan":
            return CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "0cf2b13b-3c9f-4f4e-8074-93edc01ab1bd"},
            )
        elif self.country_level == "Djibouti":
            return CkanTarget(
                dataset={"id": "cf5f891c-6fec-4509-9e3a-0ac69bb1b6e7"},
                resource={"id": "1a6ca068-2012-44ef-b875-6d036a1f0d6b"},
            )
        # Use Admin Level 4
        elif self.country_level == "Uganda":
            return CkanTarget(
                dataset={"id": "dcd8b1bd-f6bd-4422-82b4-97d53337f68e"},
                resource={"id": "1071ec3c-4f76-463d-90c5-09a40a3cd09a"},
            )
        else:
            raise NotImplementedError


@inherits(GlobalParameters)
class PullAdminShapefileAdmin1(ExternalTask):
    def output(self):
        if self.country_level == "Uganda":
            return CkanTarget(
                dataset={"id": "dcd8b1bd-f6bd-4422-82b4-97d53337f68e"},
                resource={"id": "a85d679f-4ac4-4169-87ef-3c4aaea7c423"},
            )
        else:
            raise NotImplementedError


@inherits(GlobalParameters)
class PUllLSMSDataFromCkan(ExternalTask):
    def output(self):
        if self.country_level == "South Sudan":
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
        elif self.country_level == "Ethiopia":
            return {
                1: CkanTarget(
                    dataset={"id": "11b204a4-eb4e-47a5-91ea-8d0de0987e05"},
                    resource={"id": "85736a2d-1b1d-47bb-bc23-53c25603189a"},
                ),
                2: CkanTarget(
                    dataset={"id": "061040a2-2895-486d-8188-12b87e4cdaff"},
                    resource={"id": "d9d8bf6f-12ab-42af-b3b4-f789594dc5aa"},
                ),
                3: CkanTarget(
                    dataset={"id": "4981da3a-4b99-4692-b1c0-aae3bf1fd49d"},
                    resource={"id": "61080eca-9a15-4da6-bbfb-fb035de9b3f0"},
                ),
                4: CkanTarget(
                    dataset={"id": "5c94778c-2ec1-4f48-8337-384fe3cb3d6d"},
                    resource={"id": "e17fedce-e142-44dc-9b3b-59ca74c1f43a"},
                ),
            }
        elif self.country_level == "Djibouti":
            return {
                2012: CkanTarget(
                    dataset={"id": "4d1b8cfd-ec45-4e33-a138-308f68a13a40"},
                    resource={"id": "7e55ec6c-fec5-4b5f-8e2b-c3cc6eae2404"},
                ),
                2017: CkanTarget(
                    dataset={"id": "19b59bd6-c421-4b62-8231-32cd32fd127a"},
                    resource={"id": "d65f8ad4-b6f2-4ef8-8f43-e5d1b4a0ba3d"},
                ),
            }
        elif self.country_level == "Uganda":
            return {
                2009: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "602fe139-0408-4129-9b44-98a682c8c6ac"},
                ),
                2010: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "08c2de51-dfdd-4454-b271-245b37e878ec"},
                ),
                2011: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "ee73d93f-4c60-4ef1-b082-5935c81a28f2"},
                ),
                2013: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "5dd35608-7c51-4838-be3a-29d6f0addfd7"},
                ),
                2015: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "75085891-b988-45c0-a242-277af9a1d63c"},
                ),
                2018: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "ceb659f7-cc54-4ec7-8dda-6371d693cef0"},
                ),
                2019: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "083f2993-a606-4a28-aaec-48b9f989bf35"},
                ),
            }
        else:
            raise NotImplementedError


@inherits(GlobalParameters)
class PullHouseHoldGeoVars(ExternalTask):
    def output(self):
        if self.country_level == "Ethiopia":
            return {
                1: CkanTarget(
                    dataset={"id": "11b204a4-eb4e-47a5-91ea-8d0de0987e05"},
                    resource={"id": "3a6d92ed-38a9-41d8-b541-3661999ea59e"},
                ),
                2: CkanTarget(
                    dataset={"id": "061040a2-2895-486d-8188-12b87e4cdaff"},
                    resource={"id": "d6d8c643-6a99-448d-bee4-372270e47605"},
                ),
                3: CkanTarget(
                    dataset={"id": "4981da3a-4b99-4692-b1c0-aae3bf1fd49d"},
                    resource={"id": "75c42bf6-af89-40a4-aa82-8bb156d9f307"},
                ),
            }
        elif self.country_level == "South Sudan":
            return LocalTarget("output/EA_gps.csv")
        else:
            raise NotImplementedError


@inherits(GlobalParameters)
class PullInterviewDate(ExternalTask):
    def output(self):
        if self.country_level == "Ethiopia":
            return {
                1: CkanTarget(
                    dataset={"id": "11b204a4-eb4e-47a5-91ea-8d0de0987e05"},
                    resource={"id": "f9d214db-439d-4f2c-8251-3860b820c238"},
                ),
                2: CkanTarget(
                    dataset={"id": "061040a2-2895-486d-8188-12b87e4cdaff"},
                    resource={"id": "63fd1592-a934-4d65-856d-becd5e9ee402"},
                ),
                3: CkanTarget(
                    dataset={"id": "4981da3a-4b99-4692-b1c0-aae3bf1fd49d"},
                    resource={"id": "261bca15-7c5c-488c-aae2-a1893c5c102b"},
                ),
                4: CkanTarget(
                    dataset={"id": "5c94778c-2ec1-4f48-8337-384fe3cb3d6d"},
                    resource={"id": "68a348fa-655d-4b7f-8ebc-6072f2493d36"},
                ),
            }
        elif self.country_level == "Uganda":
            return {
                2009: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "379da6c3-2dec-412d-8421-5abc71c766aa"},
                ),
                2010: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "172996e3-ce85-4826-b98e-37ed488ead4b"},
                ),
                2011: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "6891d207-e93a-4f34-95dc-a5bf713b9de2"},
                ),
                2013: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "36b3e57c-d7d3-4eab-b15d-8822ea67f088"},
                ),
                2015: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "19ff65a1-a4a6-4f01-a811-c41bb48c1849"},
                ),
                2018: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "3a211ced-759b-407b-869c-367eabb7c11a"},
                ),
                2019: CkanTarget(
                    dataset={"id": "bad371ed-de4b-4c7c-be02-c9780d16a377"},
                    resource={"id": "79a1977c-b455-4eef-893c-9301d5ed6785"},
                ),
            }
        else:
            raise NotImplementedError


@requires(PullHouseHoldGeoVars, PullAdminShapefile)
class AdminNamesToHouseHoldvars(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    @staticmethod
    def df_to_gdf(df, lng, lat):
        df = df.dropna(subset=[lng, lat])
        df = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df[lng], df[lat]), crs="EPSG:4326",
        )
        return df

    def merge_admin_names(self, df, admin_df, lng, lat):
        gdf = self.df_to_gdf(df, lng, lat)
        gdf = gpd.sjoin(gdf, admin_df, how="left", op="intersects")
        return gdf

    def run(self):
        if self.country_level == "Ethiopia":
            data_map = {k: read_data(v.path) for k, v in self.input()[0].items()}
            data_map[1] = data_map[1].rename(
                columns={"LAT_DD_MOD": "lat_dd_mod", "LON_DD_MOD": "lon_dd_mod"}
            )
            admin_df = gpd.read_file(f"zip://{self.input()[1].path}")
            # admin_df = admin_df.drop(["R_CODE", "Z_CODE", "W_CODE"], axis=1)
            data_map = {
                k: self.merge_admin_names(v, admin_df, "lon_dd_mod", "lat_dd_mod")
                for k, v in data_map.items()
            }
        elif self.country_level == "South Sudan":
            df = pd.read_csv(self.input()[0].path)
            admin_df = gpd.read_file(f"zip://{self.input()[1].path}")
            admin_df = admin_df.drop("State", 1)
            data_map = self.merge_admin_names(df, admin_df, "longitude", "latitude")
        else:
            raise NotImplementedError
        with self.output().open("w") as out:
            out.write(data_map)


@requires(PUllLSMSDataFromCkan)
class NormalizeLSMSData(Task):
    """
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        src_map = self.input()
        data_map = {k: read_data(v.path) for k, v in src_map.items()}
        if self.country_level == "South Sudan":
            # Drop existing sanitation column
            for wave in data_map:
                try:
                    data_map[wave] = data_map[wave].drop("sanitation", axis=1)
                except KeyError:
                    pass

            wash_columns = {
                "D_9_toilet": "sanitation",
                "D_17_toilet": "sanitation",
                "C_20_toilet": "sanitation",
                "D_5_1_drink_source": "drinking_water",
                "D_10_drink_source": "drinking_water",
                "C_10_water_home": "drinking_water",
            }
            for wave in data_map:
                data_map[wave] = data_map[wave].rename(columns=wash_columns)
            for wave in [2, 3]:
                data_map[wave]["sanitation"] = data_map[wave]["sanitation"].replace(
                    toilet_facilities_w2_w3_ss
                )
                data_map[wave]["drinking_water"] = data_map[wave][
                    "drinking_water"
                ].replace(source_drinking_water_w2_w3_ss)

            data_map[4]["sanitation"] = data_map[4]["sanitation"].replace(
                toilet_facilities_w4_ss
            )
            data_map[4]["drinking_water"] = data_map[4]["drinking_water"].replace(
                source_drinking_water_w4_ss
            )
            # Fix others
            data_map[4].loc[
                data_map[4]["sanitation"] == "Other (specify)", "sanitation"
            ] = data_map[4]["C_20_toilet_ot"]
            data_map[4].loc[
                data_map[4]["drinking_water"] == "Other (specify)", "drinking_water"
            ] = data_map[4]["C_10_water_home_ot"]
            # Classify others
            for wave in data_map:
                data_map[wave]["sanitation"] = data_map[wave]["sanitation"].replace(
                    sanitation_classification
                )
                data_map[wave]["drinking_water"] = data_map[wave][
                    "drinking_water"
                ].replace(drinking_water_classification)
            # Shared facility
            data_map[4].loc[
                data_map[4]["C_21_share_facility"] == 1.0, "sanitation"
            ] = "Limited service"

        elif self.country_level == "Ethiopia":
            data_map[1] = data_map[1].replace(wave_1_label)
            data_map[3] = data_map[3].replace(wave_3_label)
            try:
                data_map[4].loc[
                    data_map[4]["s10aq12"] == "16. Other(Specify)", "s10aq12"
                ] = data_map[4]["s10aq12_os"]
            except KeyError:
                pass
            wash_columns = {
                "hh_s9q10": "sanitation",
                "s10aq12": "sanitation",
                "hh_s9q13": "drinking_water",
                "s10aq21": "drinking_water",
            }
            for k in data_map:
                data_map[k] = data_map[k].rename(columns=wash_columns)
                data_map[k]["sanitation"] = data_map[k]["sanitation"].replace(
                    sanitation_classification
                )
                data_map[k]["drinking_water"] = data_map[k]["drinking_water"].replace(
                    drinking_water_classification
                )
            for k in [3, 4]:
                if k == 4:
                    var = "s10aq15"
                else:
                    var = "hh_s9q10b"

                data_map[k].loc[
                    (data_map[k][var] == 1)
                    & (data_map[k]["sanitation"] == "At least basic"),
                    "sanitation",
                ] = "Limited service"
        elif self.country_level == "Djibouti":
            data_map[2012] = data_map[2012].replace(dji_2012_label)
            data_map[2017] = data_map[2017].replace(french_t_english)
            for k in data_map:
                data_map[k] = data_map[k].rename(
                    columns={
                        "IM01": "region",
                        "q05_28": "sanitation",
                        "CL14": "sanitation",
                        "CL09": "drinking_water",
                        "q05_21": "drinking_water",
                        "q05_29": "private_shared",
                        "CL15": "private_shared",
                    }
                )
                data_map[k]["sanitation"] = data_map[k]["sanitation"].replace(
                    sanitation_classification
                )
                data_map[k]["drinking_water"] = data_map[k]["drinking_water"].replace(
                    drinking_water_classification
                )
                data_map[k].loc[
                    (data_map[k]["private_shared"] == "Sharing")
                    & (data_map[k]["sanitation"] == "At least basic"),
                    "sanitation",
                ] = "Limited service"

        elif self.country_level == "Uganda":
            for k in data_map:
                if k == 2009:
                    # SANITATION
                    data_map[k]["H9q22"] = data_map[k]["H9q22"].replace(
                        toilet_facilities_uganda_2009
                    )
                    data_map[k] = data_map[k].rename(columns={"H9q22": "sanitation"})
                    data_map[k]["sanitation"] = data_map[k]["sanitation"].replace(
                        sanitation_classification
                    )
                    # WATER
                    data_map[k]["H9q07"] = data_map[k]["H9q07"].replace(
                        source_drinking_water_uganda_2009
                    )
                    data_map[k] = data_map[k].rename(
                        columns={"H9q07": "drinking_water"}
                    )
                    data_map[k]["drinking_water"] = data_map[k][
                        "drinking_water"
                    ].replace(drinking_water_classification)
                elif k in (2010, 2011):
                    # SANITATION
                    data_map[k]["h9q22"] = data_map[k]["h9q22"].replace(
                        toilet_facilities_uganda_2010_2011
                    )
                    data_map[k] = data_map[k].rename(columns={"h9q22": "sanitation"})
                    data_map[k]["sanitation"] = data_map[k]["sanitation"].replace(
                        sanitation_classification
                    )
                    # WATER
                    data_map[k]["h9q7"] = data_map[k]["h9q7"].replace(
                        source_drinking_water_uganda_2010_2011
                    )
                    data_map[k] = data_map[k].rename(columns={"h9q7": "drinking_water"})
                    data_map[k]["drinking_water"] = data_map[k][
                        "drinking_water"
                    ].replace(drinking_water_classification)

                elif k in (2013, 2015):
                    # SANITATION
                    data_map[k]["h9q22"] = data_map[k]["h9q22"].replace(
                        toilet_facilities_uganda_2013_2015
                    )
                    data_map[k]["h9q22a"] = data_map[k]["h9q22a"].replace(
                        toilet_facilities_shared
                    )
                    data_map[k]["h9q22_combined"] = (
                        data_map[k]["h9q22"].astype(str)
                        + ", "
                        + data_map[k]["h9q22a"].astype(str)
                    )
                    data_map[k] = data_map[k].rename(
                        columns={"h9q22_combined": "sanitation"}
                    )
                    data_map[k]["sanitation"] = data_map[k]["sanitation"].replace(
                        sanitation_classification
                    )
                    # WATER
                    data_map[k]["h9q7"] = data_map[k]["h9q7"].replace(
                        source_drinking_water_uganda_2013_2015
                    )
                    data_map[k] = data_map[k].rename(columns={"h9q7": "drinking_water"})
                    data_map[k]["drinking_water"] = data_map[k][
                        "drinking_water"
                    ].replace(drinking_water_classification)
                elif k in (2018, 2019):
                    # SANITATION
                    data_map[k]["h9q22"] = data_map[k]["h9q22"].replace(
                        toilet_facilities_uganda_2018_2019
                    )
                    data_map[k]["h9q22a"] = data_map[k]["h9q22a"].replace(
                        toilet_facilities_shared
                    )
                    data_map[k]["h9q22_combined"] = (
                        data_map[k]["h9q22"].astype(str)
                        + ", "
                        + data_map[k]["h9q22a"].astype(str)
                    )
                    data_map[k] = data_map[k].rename(
                        columns={"h9q22_combined": "sanitation"}
                    )
                    data_map[k]["sanitation"] = data_map[k]["sanitation"].replace(
                        sanitation_classification
                    )
                    # WATER
                    data_map[k]["h9q07"] = data_map[k]["h9q07"].replace(
                        source_drinking_water_uganda_2018_2019
                    )
                    data_map[k] = data_map[k].rename(
                        columns={"h9q07": "drinking_water"}
                    )
                    data_map[k]["drinking_water"] = data_map[k][
                        "drinking_water"
                    ].replace(drinking_water_classification)
                else:
                    raise NotImplementedError
        else:
            raise NotImplementedError
        with self.output().open("w") as out:
            out.write(data_map)


@inherits(GlobalParameters)
class LSMSWASHData(Task):
    def requires(self):
        lsms = self.clone(NormalizeLSMSData)
        admin = self.clone(AdminNamesToHouseHoldvars)
        dates = self.clone(PullInterviewDate)
        shape = self.clone(PullAdminShapefile)
        shapeAdmin1 = self.clone(PullAdminShapefileAdmin1)
        if self.country_level == "Ethiopia":
            return lsms, admin, dates
        elif self.country_level == "South Sudan":
            return lsms, admin
        elif self.country_level == "Djibouti":
            return lsms, shape
        elif self.country_level == "Uganda":
            return lsms, shape, dates, shapeAdmin1

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def data_prep(self):
        with self.input()[0].open() as src:
            df_map = src.read()
        try:
            with self.input()[1].open() as src:
                #
                admin_df_map = src.read()
        except AttributeError:
            admin_df_map = gpd.read_file(f"zip://{self.input()[1].path}")
            admin_df_map2 = gpd.read_file(f"zip://{self.input()[3].path}")
            admin_df_map = admin_df_map.rename(columns={"date": "admin_df_map_date"})
            admin_df_map2 = admin_df_map2.rename(columns={"date": "admin_df_map_date"})

        if self.country_level == "Ethiopia":
            date_df = {k: read_data(v.path) for k, v in self.input()[2].items()}
            date_df[2]["hh_saq13_b"] = date_df[2]["hh_saq13_b"].apply(
                lambda x: ethiopia_month_name_map.get(x)
            )
            for wave in [1, 2, 3]:
                date_df[wave] = date_df[wave].dropna(
                    subset=["hh_saq13_c", "hh_saq13_b", "hh_saq13_a"]
                )
                date_df[wave]["month"] = date_df[wave].apply(
                    lambda row: to_gregorian(
                        int(row["hh_saq13_c"]),
                        int(row["hh_saq13_b"]),
                        int(row["hh_saq13_a"]),
                    ),
                    axis=1,
                )
            data_map = {}
            for wave in [1, 2, 3]:
                if wave == 1:
                    merge_var = "household_id"
                else:
                    merge_var = "household_id2"
                keep_cols = [merge_var, "sanitation", "drinking_water"]
                data_map[wave] = admin_df_map[wave].merge(
                    df_map[wave][keep_cols], on=merge_var, how="left"
                )
                keep_cols = [merge_var, "month"]
                data_map[wave] = data_map[wave].merge(
                    date_df[wave][keep_cols], on=merge_var, how="left"
                )

            df = pd.concat(data_map)
            df = df.reset_index()
            df = df.rename(columns={"level_0": "wave"})
            # We should rename column name month to date
        elif self.country_level == "Uganda":
            date_df = {k: read_data(v.path) for k, v in self.input()[2].items()}
            # Removing duplicates from my shapefile by selecting the first row with a duplicated ADM4 name
            admin_df_map = admin_df_map.sort_index().groupby("ADM4_EN").first()
            admin_df_map = admin_df_map.reset_index()

            admin_df_map3 = admin_df_map.sort_index().groupby("ADM2_EN").first()
            admin_df_map3 = admin_df_map3.reset_index()
            # admin 2: District Name, admin 3: County/ municipality, admin 4: subcounty admin 5: Parish Code
            for wave in df_map.keys():
                # for wave in [2009]:
                if wave == 2009:
                    # lsms_hhid = "Hhid"
                    # dates_hhid = "HHID"
                    # h1bq2a == Day, h1bq2b == Month, h1bq2c == Year, h1aq1_05 == district code, h1aq3 == subcounty code, h1aq2 country/municipality code
                    # https://microdata.worldbank.org/index.php/catalog/1001/data-dictionary/F135?file_name=2009_GSEC1
                    date_df[wave] = date_df[wave].rename(
                        columns={
                            "HHID": "hhid",
                            "h1bq2a": "day",
                            "regurb": "subreg",
                            "h1bq2b": "month",
                            "h1aq1": "admin2",
                            "h1aq2b": "admin3",
                            "h1aq3b": "admin4",
                            "h1aq4b": "admin5",
                        }
                    )
                    df_map[wave] = df_map[wave].rename(columns={"Hhid": "hhid"})
                    # alt_hh_id = "HHID_parent"

                elif wave == 2010:
                    # lsms_hhid = "HHID"
                    # dates_hhid = "HHID"
                    date_df[wave] = date_df[wave].rename(
                        columns={
                            "HHID": "hhid",
                            "regurb": "subreg",
                            "h1aq1": "admin2",
                            "h1aq2b": "admin3",
                            "h1aq3b": "admin4",
                            "h1aq4b": "admin5",
                        }
                    )
                    df_map[wave] = df_map[wave].rename(columns={"HHID": "hhid"})
                    # alt_hh_id = ""

                elif wave == 2011:
                    # lsms_hhid = "HHID"
                    # dates_hhid = "HHID"
                    date_df[wave] = date_df[wave].rename(
                        columns={
                            "HHID": "hhid",
                            "sregion": "subreg",
                            "h1aq1": "admin2",
                            "h1aq2": "admin3",
                            "h1aq3": "admin4",
                            "h1aq4": "admin5",
                        }
                    )
                    df_map[wave] = df_map[wave].rename(columns={"HHID": "hhid"})
                    # alt_hh_id = "HH_2005"

                elif wave == 2013:
                    # lsms_hhid = "HHID"
                    # dates_hhid = "HHID"
                    date_df[wave]["region"].replace(region_code_uganda, inplace=True)
                    date_df[wave] = date_df[wave].rename(
                        columns={
                            "HHID": "hhid",
                            "sregion": "subreg",
                            "h1aq1a": "admin2",
                            "h1aq3a": "admin3",
                            "h1aq3b": "admin4",
                            "h1aq4b": "admin5",
                        }
                    )
                    df_map[wave] = df_map[wave].rename(columns={"HHID": "hhid"})
                    # alt_hh_id = "HHID_old"

                elif wave == 2015:
                    # lsms_hhid = "hhid"
                    # dates_hhid = "hh"
                    date_df[wave]["region"].replace(region_code_uganda, inplace=True)
                    date_df[wave] = date_df[wave].rename(
                        columns={
                            "hh": "hhid",
                            "sregion": "subreg",
                            "district_name": "admin2",
                            "subcounty_name": "admin4",
                            "village_name": "admin5",
                        }
                    )
                    date_df[wave]["admin3"] = date_df[wave]["admin2"]
                    # alt_hh_id = "hh"

                elif wave == 2018:
                    # lsms_hhid = "hhid"
                    # dates_hhid = "hhid"
                    date_df[wave]["region"].replace(region_code_uganda, inplace=True)
                    date_df[wave] = date_df[wave].rename(
                        columns={
                            "distirct_name": "admin2",
                            "county_name": "admin3",
                            "subcounty_name": "admin4",
                            "parish_name": "admin5",
                        }
                    )
                    # alt_hh_id = "t0_hhid"

                else:
                    # lsms_hhid = "hhid"
                    # dates_hhid = "hhid"
                    date_df[wave]["region"].replace(region_code_uganda, inplace=True)
                    date_df[wave] = date_df[wave].rename(
                        columns={
                            "s1aq03a": "admin4",
                            "s1aq02a": "admin3",
                            "district": "admin2",
                            "s1aq04a": "admin5",
                        }
                    )
                    # alt_hh_id = "hhidold"

                date_df[wave]["date"] = pd.to_datetime(
                    date_df[wave][["year", "month", "day"]],
                    format="%d-%m-%Y",
                    errors="coerce",
                )
                date_df[wave]["admin4"] = date_df[wave]["admin4"].apply(
                    lambda x: str(x).title()
                )

                subcountry_replace_list = "|".join(
                    ["T. Council", "Tc", "T.Council", "T/C", "TC", "T.C."]
                )
                subcountry_replace_list2 = "|".join(["/", "-", "  "])
                date_df[wave]["admin4"] = date_df[wave]["admin4"].str.replace("0", "o")
                date_df[wave]["admin4"] = date_df[wave]["admin4"].str.replace(
                    "Islands", "Island"
                )
                date_df[wave]["admin4"] = date_df[wave]["admin4"].str.replace(".", "")
                date_df[wave]["admin4"] = date_df[wave]["admin4"].str.replace("'", "")
                date_df[wave]["admin4"] = date_df[wave]["admin4"].str.replace(
                    "Councilouncil", "Council"
                )
                date_df[wave]["admin4"] = date_df[wave]["admin4"].str.replace(
                    subcountry_replace_list, "Town Council"
                )
                date_df[wave]["admin4"] = date_df[wave]["admin4"].str.replace(
                    subcountry_replace_list2, " "
                )

                admin_df_map["ADM4_EN"] = admin_df_map["ADM4_EN"].str.replace(
                    subcountry_replace_list2, " "
                )

                keep_cols_dates = [
                    "hhid",
                    "date",
                    "year",
                    "day",
                    "month",
                    "region",
                    "subreg",
                    "admin5",
                    "admin4",
                    "admin3",
                    "admin2",
                ]
                keep_cols_survey = ["hhid", "sanitation", "drinking_water"]

                # Join survey data to GSEC1 which brings in survey date and region
                df_map[wave] = df_map[wave][keep_cols_survey].merge(
                    date_df[wave][keep_cols_dates], on="hhid", how="left"
                )

                admin_df_map = admin_df_map.rename(columns={"ADM0_EN": "country"})
                df_map[wave]["admin4"] = df_map[wave]["admin4"].astype(str)
                admin_df_map["ADM4_EN"] = admin_df_map["ADM4_EN"].astype(str)
                # print(len(mismatched_data[mismatched_data['admin4'].isnull()]) )

                # Create a dataframe of matched geographic data
                exact_match = df_map[wave][df_map[wave]["admin4"] != ""].merge(
                    admin_df_map, left_on="admin4", right_on="ADM4_EN", how="left"
                )
                exact_match = exact_match.dropna(subset=["ADM4_EN"])
                exact_match["key"] = df_map[wave]["admin4"]

                # Create a dataframe of unmatched geographic data
                fuzzy_match = df_map[wave][df_map[wave]["admin4"] != ""].merge(
                    admin_df_map, left_on="admin4", right_on="ADM4_EN", how="left"
                )
                fuzzy_match = fuzzy_match[(fuzzy_match["ADM4_EN"].isnull())]
                fuzzy_match = fuzzy_match[
                    [
                        "hhid",
                        "sanitation",
                        "drinking_water",
                        "date",
                        "year",
                        "day",
                        "month",
                        "region",
                        "admin4",
                    ]
                ]
                fuzzy_match["key"] = fuzzy_match["admin4"].apply(
                    lambda x: [process.extract(x, admin_df_map["ADM4_EN"], limit=1)][0][
                        0
                    ][0]
                )
                fuzzy_match = fuzzy_match.merge(
                    admin_df_map, left_on="key", right_on="ADM4_EN", how="inner"
                )

                # Note that some of the survey data contains no geographic info
                # Regions except Kampala
                survey_blanks1 = df_map[wave][
                    (df_map[wave]["admin4"] == "")
                    & (df_map[wave]["region"] != "Kampala")
                ]
                survey_blanks1["key"] = survey_blanks1["admin4"]
                survey_blanks1 = survey_blanks1.merge(
                    admin_df_map2, left_on="region", right_on="ADM1_EN", how="inner"
                )

                # Region equals Kampala
                survey_blanks2 = df_map[wave][
                    (df_map[wave]["admin4"] == "")
                    & (df_map[wave]["region"] == "Kampala")
                ]
                survey_blanks2["key"] = survey_blanks2["admin4"]
                survey_blanks2 = survey_blanks2.merge(
                    admin_df_map3, left_on="region", right_on="ADM2_EN", how="inner"
                )

                # Try the join to the date_df again using an alternative hhid
                # if alt_hh_id != "":
                #    survey_blanks = survey_blanks[keep_cols_survey].merge( date_df[wave],left_on="hhid",right_on=alt_hh_id, how="left")
                #    survey_blanks = survey_blanks.merge(admin_df_map, left_on='admin4', right_on='ADM4_EN', how='left')

                # EXPORTING A DATACUT PER YEAR
                df_map[wave] = pd.concat(
                    [exact_match, fuzzy_match, survey_blanks1, survey_blanks2]
                )

            df = pd.concat(df_map)
            df = df.reset_index()
            df = df.rename(columns={"level_0": "wave"})
            df.to_csv("Uganda LSMSWASHDATA FINAL results.csv")

        elif self.country_level == "South Sudan":
            # Add state label
            for wave in [2, 3, 4]:
                df_map[wave] = df_map[wave].replace(ss_hfs_options)

            # convert month
            for wave in [1, 2, 3, 4]:
                try:
                    df_map[wave]["month"] = df_map[wave]["month"].replace(
                        month_to_date[wave]
                    )
                    df_map[wave]["month"] = pd.to_datetime(df_map[wave]["month"])
                except TypeError:
                    df_map[wave]["month"] = df_map[wave]["month"].replace(
                        {689: "Jun", 688: "May", 690: "Jul", 691: "Aug"}
                    )
                    df_map[wave]["month"] = df_map[wave]["month"].replace(
                        month_to_date[wave]
                    )
                    df_map[wave]["month"] = pd.to_datetime(df_map[wave]["month"])

            # Fix spelling
            df_map[1] = df_map[1].replace(ss_state_spelling)
            df = pd.concat(df_map)
            df = df.reset_index()
            df = df.rename(columns={"level_0": "wave"})
            df = df.merge(admin_df_map, on=["state", "ea", "wave"], how="left")

        elif self.country_level == "Djibouti":
            df = pd.concat(df_map)
            df = df.reset_index()
            df = df.rename(columns={"level_0": "year"})
            admin_df_map = admin_df_map.drop(["ADM0_PCODE", "ADM1_PCODE"], axis=1)
            admin_df_map = admin_df_map.rename(
                columns={"ADM0_EN": "country", "ADM1_EN": "region"}
            )
            df = df.merge(admin_df_map, on="region", how="left")
            df["month"] = df["year"].apply(lambda y: datetime.date(y, 4, 1))
        else:
            raise NotImplementedError
        return df

    def run(self):
        df = self.data_prep()
        df = df.dropna(subset=["month"])
        if self.country_level != "Uganda":
            try:
                df["month"] = pd.to_datetime(df["month"])
            except pd._libs.tslibs.np_datetime.OutOfBoundsDatetime:
                df = df.loc[df["month"] != datetime.date(14, 1, 28)]
                df["month"] = pd.to_datetime(df["month"])
            df["year"] = df["month"].dt.year
            df["month_num"] = df["month"].dt.month
        else:
            df = df.rename(columns={"month": "month_num"})

        if self.country_level == "Ethiopia":
            df.loc[df["year"] == 2015, "year"] = 2014
            df = df.rename(
                columns={"lat_dd_mod": "latitude", "lon_dd_mod": "longitude"}
            )

        df["total"] = 1
        # Sanitation
        sanitation_cat = [
            "Open defecation",
            "Unimproved",
            "At least basic",
            "Limited service",
        ]
        df_san = df.loc[df["sanitation"].isin(sanitation_cat)]
        df_san["unimproved_sanitation"] = 0
        df_san.loc[
            df_san["sanitation"].isin(["Open defecation", "Unimproved"]),
            "unimproved_sanitation",
        ] = 1

        if self.country_level == "Djibouti":
            index_vars = ["year", "month_num", "region"]

        elif self.country_level == "Uganda":
            # NOTE THAT ADMIN LEVEL4 SHOULD GO HERE ONCE WE ARE ABLE TO JOIN TO X VARS
            # Note sure if it makes sense to include interview date?
            # index_vars = ['wave', 'year', 'region', "admin4", "date", "month_num"]
            index_vars = ["wave", "year", "region", "month_num"]

        else:
            index_vars = ["wave", "latitude", "longitude", "year", "month_num"]

        df_san = df_san.groupby(by=index_vars, as_index=False)[
            "unimproved_sanitation", "total"
        ].sum()
        df_san["unimproved_sanitation"] = (
            df_san["unimproved_sanitation"] / df_san["total"]
        )
        # Water source
        water_source = [
            "At least basic",
            "Limited service",
            "Surface water",
            "Unimproved",
        ]
        df_water = df.loc[df["drinking_water"].isin(water_source)]
        df_water["unimproved_drinking_water"] = 0
        df_water.loc[
            df_water["drinking_water"].isin(["Surface water", "Unimproved"]),
            "unimproved_drinking_water",
        ] = 1

        df_water = df_water.groupby(by=index_vars, as_index=False)[
            "unimproved_drinking_water", "total"
        ].sum()
        df_water["unimproved_drinking_water"] = (
            df_water["unimproved_drinking_water"] / df_water["total"]
        )

        df_san = df_san.drop("total", axis=1)
        df_water = df_water.drop("total", axis=1)

        out_df = pd.merge(df_san, df_water, on=index_vars, how="outer")

        if self.country_level == "Djibouti":
            out_df = out_df.merge(
                df[["region", "geometry"]].drop_duplicates(), on="region", how="left"
            )
        # out_df.to_csv("out_df.csv")
        with self.output().open("w") as out:
            out.write(out_df)


@inherits(GlobalParameters)
class PullJPMWASHData(ExternalTask):
    def output(self):
        if self.country_level == "Djibouti":
            return CkanTarget(
                dataset={"id": "c65119db-71e9-42de-afd4-0789130607b3"},
                resource={"id": "68bbd20a-6f30-4407-bbe9-a08b57e8bea2"},
            )
        elif self.country_level == "Ethiopia":
            return CkanTarget(
                dataset={"id": "c65119db-71e9-42de-afd4-0789130607b3"},
                resource={"id": "7d67e4c6-405b-4b10-b416-2f76ad2253e2"},
            )
        elif self.country_level == "Uganda":
            return CkanTarget(
                dataset={"id": "c65119db-71e9-42de-afd4-0789130607b3"},
                resource={"id": "2335c621-20d4-4f17-9d7a-acdabe13e95f"},
            )
        else:
            raise NotImplementedError


@inherits(GlobalParameters)
class PullJPMAdmin(ExternalTask):
    def output(self):
        if self.country_level == "Djibouti":
            return CkanTarget(
                dataset={"id": "cf5f891c-6fec-4509-9e3a-0ac69bb1b6e7"},
                resource={"id": "44348d34-5140-4792-a1a1-6d8c8a59da58"},
            )
        elif self.country_level == "Uganda":
            return CkanTarget(
                dataset={"id": "dcd8b1bd-f6bd-4422-82b4-97d53337f68e"},
                resource={"id": "053925bf-cbe5-4d9b-ac9d-001d9508ca33"},
            )
        else:
            raise NotImplementedError


@requires(PullJPMWASHData, PullJPMAdmin)
class JPMWASHData(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        df = pd.read_csv(self.input()[0].path)
        gdf = gpd.read_file(f"zip://{self.input()[1].path}")

        df_san = df.loc[df["Service Type"] == "Sanitation"]
        sani_map = {
            "At least basic": "Improved",
            "Basic service": "Improved",
            "Limited service": "Unimproved",
            "Open defecation": "Unimproved",
            "Safely managed service": "Improved",
            "Unimproved": "Unimproved",
        }
        df_san["Service level"] = df_san["Service level"].replace(sani_map)

        index_vars = ["Country", "Region", "Service Type", "Year", "Service level"]
        index_vars = [i for i in index_vars if i in df_san.columns]
        df_san = df_san.groupby(index_vars, as_index=False)["Coverage"].sum()
        df_san = df_san[df_san["Service level"] == "Unimproved"].copy()
        df_san = df_san.rename(columns={"Coverage": "unimproved_sanitation"})

        df_water = df.loc[df["Service Type"] == "Drinking water"]
        water_map = {
            "At least basic": "Improved",
            "Limited service": "Improved",
            "Surface water": "Unimproved",
            "Unimproved": "Unimproved",
        }
        df_water["Service level"] = df_water["Service level"].replace(water_map)

        df_water = df_water.groupby(index_vars, as_index=False)["Coverage"].sum()
        df_water = df_water[df_water["Service level"] == "Unimproved"].copy()
        df_water = df_water.rename(columns={"Coverage": "unimproved_drinking_water"})

        index_vars = ["Country", "Region", "Year"]
        index_vars = [i for i in index_vars if i in df_water.columns]
        df_san = df_san[index_vars + ["unimproved_sanitation"]]
        df_water = df_water[index_vars + ["unimproved_drinking_water"]]
        df = df_san.merge(df_water, on=index_vars, how="left")
        df = gdf.merge(df, left_on="ADM0_EN", right_on="Country", how="right")

        with self.output().open("w") as out:
            out.write(df)


@inherits(GlobalParameters)
class DHSClusterGPS(ExternalTask):
    def output(self):
        if self.country_level == "Ethiopia":
            return LocalTarget(path="output/DHS_GPS/ETGE71FL.ZIP")
        elif self.country_level == "Kenya":
            return {
                2014: LocalTarget("output/DHS_GPS/KEGE71FL_2014.zip"),
                2015: LocalTarget("output/DHS_GPS/KEGE7AFL_2015.zip"),
            }
        else:
            return NotImplementedError


@inherits(GlobalParameters)
class DHSWASHSurvey(ExternalTask):
    def output(self):
        if self.country_level == "Ethiopia":
            return CkanTarget(
                dataset={"id": "353a8727-c58c-4aba-93e8-eb8081f21c18"},
                resource={"id": "575ed664-c291-4780-b971-1b885cf9e568"},
            )
        elif self.country_level == "Kenya":
            return {
                2014: CkanTarget(
                    dataset={"id": "9b0c4eac-6583-434f-b820-40d7cf1b4cb6"},
                    resource={"id": "43a157c9-29e6-423a-b32d-274e892c0567"},
                ),
                2015: CkanTarget(
                    dataset={"id": "01d5dbf3-cff3-4a5f-816b-b0846ee6fd29"},
                    resource={"id": "a9bdddcd-7d10-4bcc-ad45-a840b8b1d388"},
                ),
            }
        else:
            raise NotImplementedError


@requires(DHSClusterGPS, DHSWASHSurvey)
class DHSWASHData(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        if self.country_level == "Ethiopia":
            df = read_data(self.input()[1].path)
            gdf = gpd.read_file(f"zip://{self.input()[0].path}")
            df["month"] = df.apply(
                lambda x: to_gregorian(x["hv007"], x["hv006"], 1), axis=1
            )
        elif self.country_level == "Kenya":
            gdf_map = self.input()[0]
            gdf = pd.concat(
                [gpd.read_file(f"zip://{v.path}") for _, v in gdf_map.items()]
            )
            df_map = self.input()[1]
            columns = [
                "hhid",
                "hv001",
                "hv002",
                "hv006",
                "hv007",
                "hv008",
                "hv201",
                "hv204",
                "hv205",
                "hv225",
            ]
            df = pd.concat(
                [pd.read_stata(v.path, columns=columns) for _, v in df_map.items()]
            )
            df["month"] = df.apply(
                lambda x: datetime.date(x["hv007"], x["hv006"], 1), axis=1
            )
        else:
            raise NotImplementedError

        df = df.drop_duplicates(subset=["hhid"])
        df["month"] = pd.to_datetime(df["month"])
        df["year"] = df["month"].dt.year
        df["month_num"] = df["month"].dt.month
        df = df.rename(columns={"hv001": "DHSCLUST"})
        df["sanitation"] = df["hv205"].replace(DHS_toilet_facility_map)
        df.loc[
            (df["sanitation"] == "At least basic") & (df["hv225"] == "yes"),
            "sanitation",
        ] = "Unimproved"
        df["drinking_water"] = df["hv201"].replace(DHS_water_source_map)

        df_san = df.loc[df["sanitation"] != "Other"]
        df_san = self.group_wash_var(df_san, "unimproved_sanitation", "sanitation")
        df_water = df.loc[df["drinking_water"] != "Other"]
        df_water = self.group_wash_var(
            df_water, "unimproved_drinking_water", "drinking_water"
        )
        df = pd.merge(
            df_san, df_water, on=["DHSCLUST", "year", "month_num"], how="outer"
        )
        gdf = gdf.rename(columns={"DHSYEAR": "year"})
        df = df.merge(gdf, on=["DHSCLUST", "year"], how="left")
        with self.output().open("w") as out:
            out.write(df)

    @staticmethod
    def group_wash_var(df, new_var, var):
        df = df.dropna(subset=[var])
        df[new_var] = 0
        df.loc[df[var] != "At least basic", new_var] = 1
        df["Total"] = 1
        index_vars = ["DHSCLUST", "year", "month_num"]
        df = df.groupby(index_vars, as_index=False)[new_var, "Total"].sum()
        df[new_var] = df[new_var] / df["Total"]
        df = df.drop("Total", axis=1)
        return df
