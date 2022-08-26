import pandas as pd
import numpy as np  # noqa: F401
import os
import pickle

from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget  # noqa: F401
from kiluigi.tasks import ExternalTask, Task  # noqa: F401
from luigi.configuration import get_config
from luigi.util import requires

from impyute.imputation.cs import mice

CONFIG = get_config()
MIGRATION_OUTPUT_PATH = os.path.join(
    CONFIG.get("paths", "output_path"), "migration_model"
)


# Check if output path exists, if not create it
if not os.path.exists(MIGRATION_OUTPUT_PATH):
    os.makedirs(MIGRATION_OUTPUT_PATH, exist_ok=True)


class CkanCleanedData(ExternalTask):
    """
    Inventory of the preprocessed/pre-selected dataset from CKAN (cleaned by Yared)
    """

    def output(self):

        return {
            "base_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "b0196e07-deb2-4962-adda-36fb2afef321"},
            ),
            "climate_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "ccc760ff-c61e-4490-b3c4-8b0faf2bbc83"},
            ),
            "omai_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "c3fc1987-1f47-448e-836e-713910ba2130"},
            ),
            "ohdi_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "a2b64f64-f17e-43cb-ab4b-474ff9621030"},
            ),
            "dhdi_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "d47b9ddf-f1fa-42e7-9ba7-2380f02a1974"},
            ),
            "oupop_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "d3b8374a-b59a-4125-a1f2-cd47a501b3cd"},
            ),
            "dupop_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "6029da17-3c8c-4b3c-9d10-19a8b276692d"},
            ),
            "unhcr_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "3f2ff4de-3fee-4d1c-99fd-3751eb8739fc"},
            ),
            "upsala_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "45812aec-77ed-4f5e-925b-76ec533e6a47"},
            ),
            "ogain_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "eda2ef13-0d06-411f-8c02-ffab196626d7"},
            ),
            "dgain_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "de200638-7edc-49c1-b85e-cc0f0f1e3000"},
            ),
            "language_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "12f15e09-472f-497b-820f-e72c2795798b"},
            ),
            "opolity_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "114296ae-a96c-4697-a076-6804ca7d470a"},
            ),
            "polity_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "b7befb03-136e-43bc-bb23-617d793e7958"},
            ),
            "origin_country_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "2391d68e-1760-44fd-955f-f08bea1d5a09"},
            ),
            "destination_country_df": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "7b6252f5-c7ff-4104-a95e-ae885b2ab561"},
            ),
        }


class AdditonalCleanedData(ExternalTask):
    """
    Inventory of the preprocessed/pre-selected dataset from CKAN (cleaned by Yared)
    """

    def output(self):

        return {
            "remit": CkanTarget(
                dataset={"id": "46c22af9-f493-4722-9fbc-c29e8a0d3139"},
                resource={"id": "20a18f9a-cdad-4506-ab84-9405e22ec1a6"},
            ),
            "neigbour": CkanTarget(
                dataset={"id": "c6f2021c-892b-4f8b-9e6f-d9990bee9928"},
                resource={"id": "e4b4ed14-cd67-403c-b8e8-fae0e6242d29"},
            ),
            "faid": CkanTarget(
                dataset={"id": "c6f2021c-892b-4f8b-9e6f-d9990bee9928"},
                resource={"id": "c0dcfefe-fa6f-4481-b77a-89a37b877e68"},
            ),
        }


@requires(CkanCleanedData, AdditonalCleanedData)
class CkanDataPath(Task):

    """
    retrieves data from CKAN and returns the dir path name dictionary.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        cleaned_data_dict = self.input()[0]
        addtn_data_dict = self.input()[1]

        base_df_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[0]].path
        climate_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[1]].path
        omai_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[2]].path
        ohdi_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[3]].path
        dhdi_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[4]].path
        oupop_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[5]].path
        dupop_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[6]].path
        unhcr_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[7]].path
        upsala_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[8]].path
        ogain_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[9]].path
        dgain_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[10]].path
        language_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[11]].path
        opolity_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[12]].path
        polity_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[13]].path
        o_country_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[14]].path
        d_country_dir = cleaned_data_dict[list(cleaned_data_dict.keys())[15]].path

        remit_dir = addtn_data_dict[list(addtn_data_dict.keys())[0]].path
        neighbour_dir = addtn_data_dict[list(addtn_data_dict.keys())[1]].path
        faid_dir = addtn_data_dict[list(addtn_data_dict.keys())[2]].path

        path_names = {
            "base_df_dir": base_df_dir,
            "climate_dir": climate_dir,
            "omai_dir": omai_dir,
            "ohdi_dir": ohdi_dir,
            "dhdi_dir": dhdi_dir,
            "oupop_dir": oupop_dir,
            "dupop_dir": dupop_dir,
            "unhcr_dir": unhcr_dir,
            "upsala_dir": upsala_dir,
            "ogain_dir": ogain_dir,
            "dgain_dir": dgain_dir,
            "language_dir": language_dir,
            "opolity_dir": opolity_dir,
            "polity_dir": polity_dir,
            "o_country_dir": o_country_dir,
            "d_country_dir": d_country_dir,
            "faid_dir": faid_dir,
            "remit_dir": remit_dir,
            "neighbour_dir": neighbour_dir,
        }
        with self.output().open("w") as output:
            output.write(path_names)


@requires(CkanDataPath)
class ClimateMerge(Task):
    """
    Merge the source data for migrant flow with climate data
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        path_names = self.input().open().read()
        base_df = pd.read_csv(path_names["base_df_dir"])
        climate_df = pd.read_csv(path_names["climate_dir"])

        cl = climate_df.set_index(["origin", "indicator"]).unstack()

        cl.columns = cl.columns.map(lambda x: "{}_{}".format(x[0], x[1]))
        cl = cl.reset_index()
        cl = cl.drop(["label_bws", "label_drr"], axis=1)
        with open("models/migration_model/countryname_typo.pickle", "rb") as handle:
            standard_dict = pickle.load(handle)  # noqa: S301
        cl = cl.replace({"origin": standard_dict})
        cl = cl.groupby(["origin"]).mean().reset_index()
        od = pd.merge(base_df, cl, how="left", on=["origin"]).reset_index()
        od_missing = od[od["opolity"].isnull()]
        od_missing = od_missing.groupby(by=["opolity"]).count().reset_index()
        od_dmissing = od[od["score_bws"].isnull()]
        od_dmissing = od_dmissing.groupby(by=["origin"]).count().reset_index()

        od["o_count_conflict"] = od["o_count_conflict"].fillna(0)
        od["o_fatalities"] = od["o_fatalities"].fillna(0)
        od["d_count_conflict"] = od["d_count_conflict"].fillna(0)
        od["d_fatalities"] = od["d_fatalities"].fillna(0)
        od["o_lag_count_conflict"] = od["o_lag_count_conflict"].fillna(0)
        od["o_lag_count_conflict2"] = od["o_lag_count_conflict2"].fillna(0)
        od["o_lag_fatalities"] = od["o_lag_fatalities"].fillna(0)
        od["o_lag_fatalities2"] = od["o_lag_fatalities2"].fillna(0)
        od["d_lag_count_conflict"] = od["d_lag_count_conflict"].fillna(0)
        od["d_lag_fatalities"] = od["d_lag_fatalities"].fillna(0)
        od["d_lag_count_conflict2"] = od["d_lag_count_conflict2"].fillna(0)
        od["d_lag_fatalities2"] = od["d_lag_fatalities2"].fillna(0)

        # create dummy vars for o_incomelevel and d_incomelevel
        cat_vars = pd.get_dummies(od[["o_incomelevel", "d_incomelevel"]])
        df_aggregate = pd.concat([od, cat_vars], axis=1).drop(
            columns=["o_incomelevel", "d_incomelevel"]
        )
        # create dummy vars for o_oregion and d_oregion
        cat_vars1 = pd.get_dummies(od[["o_oregion", "d_oregion"]])
        df_aggregate1 = pd.concat([df_aggregate, cat_vars1], axis=1).drop(
            columns=["o_oregion", "d_oregion"]
        )

        # add a ratio of fatalities to conflicts, number of ppl who died per event of conflict
        # _lag_ is the ratio for the previous year

        df_aggregate1["o_fatalities_conf_ratio"] = (
            df_aggregate1["o_fatalities"] / df_aggregate1["o_count_conflict"]
        )
        df_aggregate1["d_fatalities_conf_ratio"] = (
            df_aggregate1["d_fatalities"] / df_aggregate1["d_count_conflict"]
        )
        df_aggregate1["o_lag_fatalities_conf_ratio1"] = (
            df_aggregate1["o_lag_fatalities"] / df_aggregate1["o_lag_count_conflict"]
        )
        df_aggregate1["d_lag_fatalities_conf_ratio1"] = (
            df_aggregate1["d_lag_fatalities"] / df_aggregate1["d_lag_count_conflict"]
        )
        df_aggregate1["o_lag_fatalities_conf_ratio2"] = (
            df_aggregate1["o_lag_fatalities2"] / df_aggregate1["o_lag_count_conflict2"]
        )
        df_aggregate1["d_lag_fatalities_conf_ratio2"] = (
            df_aggregate1["d_lag_fatalities2"] / df_aggregate1["d_lag_count_conflict2"]
        )

        # create lag conflict ratio for the prvious 2 years
        df_aggregate1["o_lag_fatalities_conf"] = (
            df_aggregate1["o_lag_fatalities"] + df_aggregate1["o_lag_fatalities2"]
        )
        df_aggregate1["d_lag_fatalities_conf"] = (
            df_aggregate1["d_lag_fatalities"] + df_aggregate1["d_lag_fatalities2"]
        )
        df_aggregate1["o_lag_count_conf"] = (
            df_aggregate1["o_lag_count_conflict"]
            + df_aggregate1["o_lag_count_conflict2"]
        )
        df_aggregate1["d_lag_count_conf"] = (
            df_aggregate1["d_lag_count_conflict"]
            + df_aggregate1["d_lag_count_conflict2"]
        )
        df_aggregate1["o_lag_fatalities_conf_ratio"] = (
            df_aggregate1["o_lag_fatalities_conf"] / df_aggregate1["o_lag_count_conf"]
        )
        df_aggregate1["d_lag_fatalities_conf_ratio"] = (
            df_aggregate1["d_lag_fatalities_conf"] / df_aggregate1["d_lag_count_conf"]
        )

        with self.output().open("w") as output:
            output.write(df_aggregate1)


@requires(ClimateMerge, CkanDataPath)
class OmaiMerge(Task):
    """
    Merge the previous dataset for migrant flow with omai data
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        climate_merge_df = self.input()[0].open().read()  # noqa: F841
        path_names = self.input()[1].open().read()
        omai_df = pd.read_csv(path_names["omai_dir"])
        dmai = omai_df.rename(columns={"omai": "dmai"})
        with open("models/migration_model/destination_omai.pickle", "rb") as handle:
            standard_dict = pickle.load(handle)  # noqa: S301
        dmai = dmai.replace({"destination": standard_dict})
        dmai = dmai.groupby(["destination"]).mean().reset_index()

        od = pd.merge(
            climate_merge_df, dmai, how="left", on=["destination"]
        ).reset_index()
        od_dmissing = od[od["dmai"].isnull()]
        od_dmissing = od_dmissing.groupby(by=["destination"]).count().reset_index()

        od["o_count_conflict"] = od["o_count_conflict"].fillna(0)
        od["o_fatalities"] = od["o_fatalities"].fillna(0)
        od["d_count_conflict"] = od["d_count_conflict"].fillna(0)
        od["d_fatalities"] = od["d_fatalities"].fillna(0)
        od["o_lag_count_conflict"] = od["o_lag_count_conflict"].fillna(0)
        od["o_lag_count_conflict2"] = od["o_lag_count_conflict2"].fillna(0)
        od["o_lag_fatalities"] = od["o_lag_fatalities"].fillna(0)
        od["o_lag_fatalities2"] = od["o_lag_fatalities2"].fillna(0)
        od["d_lag_count_conflict"] = od["d_lag_count_conflict"].fillna(0)
        od["d_lag_fatalities"] = od["d_lag_fatalities"].fillna(0)
        od["d_lag_count_conflict2"] = od["d_lag_count_conflict2"].fillna(0)
        od["d_lag_fatalities2"] = od["d_lag_fatalities2"].fillna(0)
        od["log_refugees"] = np.log(od["refugees"] + 1)

        with self.output().open("w") as output:
            output.write(od)


@requires(OmaiMerge, CkanDataPath)
class hdiMerge(Task):
    """
    Merge the previous dataset for migrant flow with ohdi data
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        dmai_merge_df = self.input()[0].open().read()  # noqa: F841
        path_names = self.input()[1].open().read()
        ohdi_df = pd.read_csv(path_names["ohdi_dir"])
        dhdi_df = pd.read_csv(path_names["dhdi_dir"])
        dmai_merge_df = dmai_merge_df.drop(["level_0"], axis=1)
        od = pd.merge(
            dmai_merge_df, ohdi_df, how="left", on=["origin", "year"]
        ).reset_index()
        od["ohdi"] = od.groupby(["origin"])["ohdi"].apply(lambda x: x.fillna(x.max()))
        od = od.drop(
            ["level_0", "index", "Unnamed: 0_x", "Unnamed: 0_y", "Unnamed: 0_y"], axis=1
        )
        od1 = pd.merge(
            od, dhdi_df, how="left", on=["year", "destination"]
        ).reset_index()
        od1["dhdi"] = od1.groupby(["destination"])["dhdi"].apply(
            lambda x: x.fillna(x.max())
        )
        od1["ohdi"] = od1[["ohdi"]].convert_objects(convert_numeric=True)
        od1["dhdi"] = od1[["dhdi"]].convert_objects(convert_numeric=True)
        od1[
            [
                "o_incomelevel_High income",
                "o_incomelevel_Low income",
                "o_incomelevel_Lower middle income",
                "o_incomelevel_Upper middle income",
                "d_incomelevel_High income",
                "d_incomelevel_Low income",
                "d_incomelevel_Lower middle income",
                "d_incomelevel_Upper middle income",
                "o_oregion_East Asia & Pacific",
                "o_oregion_Europe & Central Asia",
                "o_oregion_Latin America & Caribbean ",
                "o_oregion_Middle East & North Africa",
                "o_oregion_North America",
                "o_oregion_South Asia",
                "o_oregion_Sub-Saharan Africa ",
                "d_oregion_East Asia & Pacific",
                "d_oregion_Europe & Central Asia",
                "d_oregion_Latin America & Caribbean ",
                "d_oregion_Middle East & North Africa",
                "d_oregion_North America",
                "d_oregion_South Asia",
                "d_oregion_Sub-Saharan Africa ",
            ]
        ] = od1[
            [
                "o_incomelevel_High income",
                "o_incomelevel_Low income",
                "o_incomelevel_Lower middle income",
                "o_incomelevel_Upper middle income",
                "d_incomelevel_High income",
                "d_incomelevel_Low income",
                "d_incomelevel_Lower middle income",
                "d_incomelevel_Upper middle income",
                "o_oregion_East Asia & Pacific",
                "o_oregion_Europe & Central Asia",
                "o_oregion_Latin America & Caribbean ",
                "o_oregion_Middle East & North Africa",
                "o_oregion_North America",
                "o_oregion_South Asia",
                "o_oregion_Sub-Saharan Africa ",
                "d_oregion_East Asia & Pacific",
                "d_oregion_Europe & Central Asia",
                "d_oregion_Latin America & Caribbean ",
                "d_oregion_Middle East & North Africa",
                "d_oregion_North America",
                "d_oregion_South Asia",
                "d_oregion_Sub-Saharan Africa ",
            ]
        ].astype(
            str
        )
        with self.output().open("w") as output:
            output.write(od1)


@requires(hdiMerge, CkanDataPath)
class upopMerge(Task):
    """
    Merge the previous dataset for migrant flow with urban population data
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        od1_df = self.input()[0].open().read()  # noqa: F841
        path_names = self.input()[1].open().read()
        oupop_df = pd.read_csv(path_names["oupop_dir"])
        dupop_df = pd.read_csv(path_names["dupop_dir"])
        upop_df = pd.merge(
            od1_df, oupop_df, how="left", on=["origin", "year"]
        ).reset_index()
        upop_df["oupop"] = upop_df.groupby(["origin"])["oupop"].apply(
            lambda x: x.fillna(x.max())
        )
        upop_df.drop(["level_0"], axis=1, inplace=True)
        upop_df = pd.merge(
            upop_df, dupop_df, how="left", on=["year", "destination"]
        ).reset_index()
        upop_df["dupop"] = upop_df.groupby(["destination"])["dupop"].apply(
            lambda x: x.fillna(x.max())
        )

        with self.output().open("w") as output:
            output.write(upop_df)


@requires(upopMerge, CkanDataPath)
class unhcrMerge(Task):
    """
    Merge the previous dataset for migrant flow with unhcr refugee acceptance  data
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        upop_df = self.input()[0].open().read()  # noqa: F841
        path_names = self.input()[1].open().read()
        unhcr_df = pd.read_csv(path_names["unhcr_dir"])
        origin_df = pd.read_csv(path_names["o_country_dir"])
        destination_df = pd.read_csv(path_names["d_country_dir"])

        unhcr_df = unhcr_df.replace("*", np.NaN)
        unhcr_df.recognised = unhcr_df.recognised.fillna(0)
        unhcr_df.recognised = unhcr_df.recognised.astype(int)
        unhcr_df.total_decision = unhcr_df.total_decision.fillna(1)
        unhcr_df.total_decision = unhcr_df.total_decision.astype(int)
        unhcr_df = unhcr_df.groupby(["origin", "destination", "year"]).sum()
        unhcr_df["dacceptance_rate"] = (
            unhcr_df.recognised / unhcr_df.total_decision
        ) * 100
        unhcr_df = unhcr_df.sort_values(by=["year"], ascending=False).reset_index()
        unhcr_df["dlag_acceptance"] = unhcr_df.groupby(["origin", "destination"])[
            "dacceptance_rate"
        ].shift(-1)

        with open("models/migration_model/destination_typo.pickle", "rb") as handle:
            standard_dict = pickle.load(handle)  # noqa: S301
        unhcr_df = unhcr_df.replace({"destination": standard_dict})
        upop_df = upop_df.replace({"destination": standard_dict})
        unhcr_df = (
            unhcr_df.groupby(["year", "origin", "destination"]).sum().reset_index()
        )
        upop_df.drop(["level_0"], axis=1, inplace=True)
        accept = pd.merge(
            upop_df, unhcr_df, how="left", on=["year", "origin", "destination"]
        ).reset_index()
        origin_df = origin_df[["origin", "o_incomelevel", "o_oregion"]]
        destination_df = destination_df[["destination", "d_incomelevel", "d_oregion"]]
        accept.drop(["level_0"], axis=1, inplace=True)
        oaccept = pd.merge(accept, origin_df, how="left", on=["origin"]).reset_index()
        oaccept.drop(["level_0"], axis=1, inplace=True)
        odaccept = pd.merge(
            oaccept, destination_df, how="left", on=["destination"]
        ).reset_index()
        odaccept[
            ["o_incomelevel", "d_incomelevel", "o_oregion", "d_oregion"]
        ] = odaccept[
            ["o_incomelevel", "d_incomelevel", "o_oregion", "d_oregion"]
        ].fillna(
            "Other"
        )
        with self.output().open("w") as output:
            output.write(odaccept)


@requires(unhcrMerge, CkanDataPath)
class upsalaMerge(Task):
    """
    Merge the previous dataset for migrant flow with  upsala conflict data
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        odaccept_df = self.input()[0].open().read()  # noqa: F841
        path_names = self.input()[1].open().read()
        upsala_df = pd.read_csv(path_names["upsala_dir"])
        ogain_df = pd.read_csv(path_names["ogain_dir"])
        dgain_df = pd.read_csv(path_names["dgain_dir"])
        with open("models/migration_model/origin_typo.pickle", "rb") as handle:
            o_standard_dict = pickle.load(handle)  # noqa: S301
        upsala_df = upsala_df.replace({"origin": o_standard_dict})
        upsala_df = upsala_df.groupby(["origin", "year"])[["obrd"]].sum().reset_index()

        odaccept_df.drop(["level_0"], axis=1, inplace=True)
        df = pd.merge(
            odaccept_df, upsala_df, how="left", on=["year", "origin"]
        ).reset_index()
        df.drop(["level_0"], axis=1, inplace=True)
        ogain_df = ogain_df.replace({"origin": o_standard_dict})
        ogain_df = ogain_df.groupby(["origin", "year"])[["ogain"]].mean().reset_index()
        df1 = pd.merge(df, ogain_df, how="left", on=["year", "origin"])
        with open("models/migration_model/destination_typo.pickle", "rb") as handle:
            d_standard_dict = pickle.load(handle)  # noqa: S301
        dgain_df = dgain_df.replace({"destination": d_standard_dict})
        dgain_df = (
            dgain_df.groupby(["destination", "year"])[["dgain"]].mean().reset_index()
        )
        df2 = pd.merge(df1, dgain_df, how="left", on=["year", "destination"])

        # add a ratio of fatalities to conflicts, number of ppl who died per event of conflict
        df2["o_fatalities_conf_ratio"] = df2["o_fatalities"] / df2["o_count_conflict"]
        df2["d_fatalities_conf_ratio"] = df2["d_fatalities"] / df2["d_count_conflict"]
        df2["o_lag_fatalities_conf_ratio1"] = (
            df2["o_lag_fatalities"] / df2["o_lag_count_conflict"]
        )
        df2["d_lag_fatalities_conf_ratio1"] = (
            df2["d_lag_fatalities"] / df2["d_lag_count_conflict"]
        )
        df2["o_lag_fatalities_conf_ratio2"] = (
            df2["o_lag_fatalities2"] / df2["o_lag_count_conflict2"]
        )
        df2["d_lag_fatalities_conf_ratio2"] = (
            df2["d_lag_fatalities2"] / df2["d_lag_count_conflict2"]
        )

        # create lag conflict ratio for the prvious 2 years
        df2["o_lag_fatalities_conf"] = (
            df2["o_lag_fatalities"] + df2["o_lag_fatalities2"]
        )
        df2["d_lag_fatalities_conf"] = (
            df2["d_lag_fatalities"] + df2["d_lag_fatalities2"]
        )
        df2["o_lag_count_conf"] = (
            df2["o_lag_count_conflict"] + df2["o_lag_count_conflict2"]
        )
        df2["d_lag_count_conf"] = (
            df2["d_lag_count_conflict"] + df2["d_lag_count_conflict2"]
        )
        df2["o_lag_fatalities_conf_ratio"] = (
            df2["o_lag_fatalities_conf"] / df2["o_lag_count_conf"]
        )
        df2["d_lag_fatalities_conf_ratio"] = (
            df2["d_lag_fatalities_conf"] / df2["d_lag_count_conf"]
        )
        df2["o_pop"] = df2.groupby(["year", "o_oregion"])["o_pop"].apply(
            lambda x: x.fillna(x.min())
        )

        df2["sfi"] = df2.groupby(["year", "o_oregion"])["sfi"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["opolity"] = df2.groupby(["year", "o_oregion"])["opolity"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["opi"] = df2.groupby(["year", "o_oregion"])["opi"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["score_bws"] = df2.groupby(["year", "o_oregion"])["score_bws"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["score_drr"] = df2.groupby(["year", "o_oregion"])["score_drr"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["d_pop"] = df2.groupby(["year", "d_oregion"])["d_pop"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["o_cpi"] = df2.groupby(["year", "o_oregion"])["o_cpi"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["d_cpi"] = df2.groupby(["year", "d_oregion"])["d_cpi"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["oydp"] = df2.groupby(["year", "o_oregion"])["oydp"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["dydp"] = df2.groupby(["year", "d_oregion"])["dydp"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["ohdi"] = df2.groupby(["year", "o_oregion"])["ohdi"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["ohdi"] = df2.groupby(["year", "o_oregion"])["ohdi"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["dhdi"] = df2.groupby(["year", "d_oregion"])["dhdi"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["dmai"] = df2.groupby(["year", "d_oregion"])["dmai"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["oupop"] = df2.groupby(["year", "o_oregion"])["oupop"].apply(
            lambda x: x.fillna(x.min())
        )
        df2["dupop"] = df2.groupby(["year", "d_oregion"])["dupop"].apply(
            lambda x: x.fillna(x.min())
        )

        df2.fillna(0, inplace=True)
        df2["olowincome"] = (
            df2["o_incomelevel_Low income"] + df2["o_incomelevel_Lower middle income"]
        )
        df2["dlowincome"] = (
            df2["d_incomelevel_Low income"] + df2["d_incomelevel_Lower middle income"]
        )
        df2["oeuropeandna"] = (
            df2["o_oregion_Europe & Central Asia"] + df2["o_oregion_North America"]
        )
        df2["deuropeandna"] = (
            df2["d_oregion_Europe & Central Asia"] + df2["d_oregion_North America"]
        )
        df2["od"] = df2["origin"] + "_" + df2["destination"]

        # Excluding climate change and upsala data
        odaccept_df["o_fatalities_conf_ratio"] = (
            odaccept_df["o_fatalities"] / odaccept_df["o_count_conflict"]
        )
        odaccept_df["d_fatalities_conf_ratio"] = (
            odaccept_df["d_fatalities"] / odaccept_df["d_count_conflict"]
        )
        odaccept_df["o_lag_fatalities_conf_ratio1"] = (
            odaccept_df["o_lag_fatalities"] / odaccept_df["o_lag_count_conflict"]
        )
        odaccept_df["d_lag_fatalities_conf_ratio1"] = (
            odaccept_df["d_lag_fatalities"] / odaccept_df["d_lag_count_conflict"]
        )
        odaccept_df["o_lag_fatalities_conf_ratio2"] = (
            odaccept_df["o_lag_fatalities2"] / odaccept_df["o_lag_count_conflict2"]
        )
        odaccept_df["d_lag_fatalities_conf_ratio2"] = (
            odaccept_df["d_lag_fatalities2"] / odaccept_df["d_lag_count_conflict2"]
        )

        # create lag conflict ratio for the prvious 2 years
        odaccept_df["o_lag_fatalities_conf"] = (
            odaccept_df["o_lag_fatalities"] + odaccept_df["o_lag_fatalities2"]
        )
        odaccept_df["d_lag_fatalities_conf"] = (
            odaccept_df["d_lag_fatalities"] + odaccept_df["d_lag_fatalities2"]
        )
        odaccept_df["o_lag_count_conf"] = (
            odaccept_df["o_lag_count_conflict"] + odaccept_df["o_lag_count_conflict2"]
        )
        odaccept_df["d_lag_count_conf"] = (
            odaccept_df["d_lag_count_conflict"] + odaccept_df["d_lag_count_conflict2"]
        )
        odaccept_df["o_lag_fatalities_conf_ratio"] = (
            odaccept_df["o_lag_fatalities_conf"] / odaccept_df["o_lag_count_conf"]
        )
        odaccept_df["d_lag_fatalities_conf_ratio"] = (
            odaccept_df["d_lag_fatalities_conf"] / odaccept_df["d_lag_count_conf"]
        )

        # recurent_conflict during the past 3 years
        odaccept_df["occf"] = (
            odaccept_df["o_fatalities"]
            + odaccept_df["o_lag_fatalities"]
            + odaccept_df["o_lag_fatalities2"]
        )
        odaccept_df["dccf"] = (
            odaccept_df["d_fatalities"]
            + odaccept_df["d_lag_fatalities"]
            + odaccept_df["d_lag_fatalities2"]
        )

        odaccept_df["occc"] = (
            odaccept_df["o_count_conflict"]
            + odaccept_df["o_lag_count_conflict"]
            + odaccept_df["o_lag_count_conflict2"]
        )
        odaccept_df["dccc"] = (
            odaccept_df["d_count_conflict"]
            + odaccept_df["d_lag_count_conflict"]
            + odaccept_df["d_lag_count_conflict2"]
        )

        odaccept_df["occ_ratio"] = odaccept_df["occf"] / odaccept_df["occc"]
        odaccept_df["dcc_ratio"] = odaccept_df["dccf"] / odaccept_df["dccc"]

        odaccept_df["oyouth_percent"] = (odaccept_df.oydp / odaccept_df.o_pop) * 100

        odaccept_df["oincomelevel_new"] = 5
        odaccept_df.loc[df.o_incomelevel == "Low income", "oincomelevel_new"] = 1
        odaccept_df.loc[
            df.o_incomelevel == "Lower middle income", "oincomelevel_new"
        ] = 2
        odaccept_df.loc[
            df.o_incomelevel == "Upper middle income", "oincomelevel_new"
        ] = 3
        odaccept_df.loc[df.o_incomelevel == "High income", "oincomelevel_new"] = 4

        odaccept_df["dincomelevel_new"] = 5
        odaccept_df.loc[df.d_incomelevel == "Low income", "dincomelevel_new"] = 1
        odaccept_df.loc[
            df.d_incomelevel == "Lower middle income", "dincomelevel_new"
        ] = 2
        odaccept_df.loc[
            df.d_incomelevel == "Upper middle income", "dincomelevel_new"
        ] = 3
        odaccept_df.loc[df.d_incomelevel == "High income", "dincomelevel_new"] = 4

        odaccept_df["olowincome"] = (
            df2["o_incomelevel_Low income"] + df2["o_incomelevel_Lower middle income"]
        )
        odaccept_df["dlowincome"] = (
            df2["d_incomelevel_Low income"] + df2["d_incomelevel_Lower middle income"]
        )
        odaccept_df["oeuropeandna"] = (
            odaccept_df["o_oregion_Europe & Central Asia"]
            + df2["o_oregion_North America"]
        )
        odaccept_df["deuropeandna"] = (
            odaccept_df["d_oregion_Europe & Central Asia"]
            + df2["d_oregion_North America"]
        )

        odaccept_df["od"] = odaccept_df["origin"] + "_" + odaccept_df["destination"]

        with self.output().open("w") as output:
            output.write(odaccept_df)


@requires(upsalaMerge, CkanDataPath)
class languageMerge(Task):
    """
    Merge the previous dataset for migrant flow with  language data
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=3600)

    def run(self):
        upsala_df = self.input()[0].open().read()
        path_names = self.input()[1].open().read()
        language_df = pd.read_stata(path_names["language_dir"])
        dpolity_df = pd.read_csv(path_names["opolity_dir"])

        language_df = language_df[
            ["country_o", "country_d", "col", "csl", "cnl", "prox1", "prox2"]
        ]
        language_df.rename(
            columns={
                "country_o": "origin",
                "country_d": "destination",
                "col": "common_official_language",
                "csl": "common_spoken_language",
                "cnl": "common_native_language",
                "prox1": "lingustic_proximity1",
                "prox2": "lingustic_proximity2",
            },
            inplace=True,
        )
        with open("models/migration_model/origin_typo.pickle", "rb") as handle:
            o_standard_dict = pickle.load(handle)  # noqa: S301
        language_df = language_df.replace({"origin": o_standard_dict})
        with open("models/migration_model/destination_typo.pickle", "rb") as handle:
            d_standard_dict = pickle.load(handle)  # noqa: S301
        language_df = language_df.replace({"destination": d_standard_dict})
        language_df = language_df.groupby(["origin", "destination"]).mean()
        df_merged = pd.merge(
            upsala_df, language_df, how="left", on=["origin", "destination"]
        )

        dpolity_df = dpolity_df.rename(
            columns={"origin": "destination", "opolity": "dpolity"}
        )
        dpolity_df = dpolity_df.replace({"destination": d_standard_dict})
        dpolity_df = (
            dpolity_df.groupby(["year", "destination"])[["dpolity"]]
            .mean()
            .reset_index()
        )

        final_df = pd.merge(
            df_merged, dpolity_df, how="left", on=["year", "destination"]
        )
        polity_df = pd.read_csv(path_names["polity_dir"])
        polity_df = polity_df[["year", "country", "democ", "autoc", "polity2"]]
        polity_df["year"] = polity_df.year.astype(int)
        polity_df = polity_df.loc[polity_df["year"] > 2011]
        polity_df = polity_df.replace({"country": o_standard_dict})
        polity_df = polity_df.groupby(["year", "country"]).mean().reset_index()

        opolity_df = polity_df.rename(
            columns={
                "country": "origin",
                "democ": "odemoc",
                "autoc": "oautoc",
                "polity2": "opolity2",
            }
        )

        final_opolity = pd.merge(
            final_df, opolity_df, how="left", on=["year", "origin"]
        )
        dpolity = polity_df.rename(
            columns={
                "country": "destination",
                "democ": "ddemoc",
                "autoc": "dautoc",
                "polity2": "dpolity2",
            }
        )
        final_polity = pd.merge(
            final_opolity, dpolity, how="left", on=["year", "destination"]
        )
        # final_polity.drop(columns=['index'], inplace=True)
        # print(final_polity.head())
        # impute columns with missing values

        select_cols = [
            "o_lag_idps",
            "o_pop",
            "d_pop",
            "oupop",
            "dupop",
            "oyouth_percent",
            "o_cpi",
            "d_cpi",
            "opcgdp",
            "opolity2",
            "dpolity2",
            "opi",
            "score_drr",
            "dmai",
            "dlag_acceptance",
            "olowincome",
            "oeuropeandna",
            "dlowincome",
            "deuropeandna",
            "distance",
            "neighbor",
        ]

        mice_impute = mice(final_polity[select_cols].astype(float).values)
        df_mice = pd.DataFrame(
            mice_impute,
            columns=final_polity[select_cols].columns,
            index=final_polity.index,
        )
        final_df[select_cols] = df_mice
        final_df.fillna(0, inplace=True)

        with self.output().open("w") as output:
            output.write(final_df)
