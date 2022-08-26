import datetime

import geopandas as gpd
import luigi
import pandas as pd
from luigi import ExternalTask, Task
from luigi.util import requires
from rasterstats import zonal_stats

from kiluigi.targets import CkanTarget, IntermediateTarget


class PullDisplacementData(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "e8a18cb9-27d8-47b2-9e33-d3b2a0abc1ae"},
            resource={"id": "a2c1f3a5-ae7b-4b21-8459-aa79d3b9e6c3"},
        )


@requires(PullDisplacementData)
class DisplacementGPS(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        df = pd.read_csv(self.input().path)
        df = df.rename(
            columns={
                "1.1.f.1: GPS: Longitude": "longitude",
                "1.1.f.2: GPS: Latitude": "latitude",
            }
        )
        df = df.dropna(subset=["longitude", "latitude"])
        df = df[["longitude", "latitude"]]
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df["longitude"], df["latitude"])
        )
        gdf["geometry"] = gdf["geometry"].buffer(0.1)
        with self.output().open("w") as out:
            out.write(gdf)


class PullNightTimeLight(ExternalTask):
    month = luigi.DateParameter(default=datetime.date(2012, 4, 1))

    def output(self):
        return CkanTarget(
            dataset={"id": "19aa8884-ed48-4c25-88e9-c1f96d5877bc"},
            resource={"name": f"night_time_light_{self.month.strftime('%Y_%m')}"},
        )


@requires(DisplacementGPS, PullNightTimeLight)
class NightTimeLightStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        with self.input()[0].open() as src:
            df = src.read()
        src_file = self.input()[1].path
        temp = zonal_stats(
            df, src_file, stats="mean", geojson_out=True, all_touched=True
        )
        gdf = gpd.GeoDataFrame.from_features(temp)
        gdf["month"] = self.month
        gdf = gdf.rename(columns={"mean": "ntl_mean"})
        with self.output().open("w") as out:
            out.write(gdf)


class MergeNTLWithDisplacemnet(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def requires(self):
        data_task = self.clone(PullDisplacementData)
        ntl_task = self.clone(NightTimeLightStats)
        month_list = pd.date_range(
            datetime.date(2012, 4, 1), datetime.date(2019, 1, 1), freq="M"
        )
        return {"data": data_task, "ntl": [ntl_task.clone(month=i) for i in month_list]}

    def run(self):
        df = pd.read_csv(self.input()["data"].path)
        df["1.1.a.1: Survey Date"] = pd.to_datetime(df["1.1.a.1: Survey Date"])
        df["1.4.a.1: Site Open Date"] = pd.to_datetime(df["1.4.a.1: Site Open Date"])
        df = df.rename(
            columns={
                "1.1.f.1: GPS: Longitude": "longitude",
                "1.1.f.2: GPS: Latitude": "latitude",
                "1.4.a.1: Site Open Date": "site_open_date",
                "1.1.a.1: Survey Date": "survey_date",
            }
        )
        df = df.reset_index(drop=True)
        data_list = []
        for index, row in df.iterrows():
            try:
                temp = row.to_frame().T
                temp["index"] = index
                temp_date = pd.DataFrame(
                    {"date": pd.date_range(row["site_open_date"], row["survey_date"])}
                )
                temp_date["month_num"] = temp_date["date"].dt.month
                temp_date["year"] = temp_date["date"].dt.year
                temp_date = temp_date.drop_duplicates(subset=["month_num", "year"])
                temp_date["index"] = index
                temp = temp.merge(temp_date, on="index", how="outer")
                temp = temp.drop("index", 1)
                data_list.append(temp)
            except ValueError:
                pass
        df = pd.concat(data_list)
        ntl_df = pd.concat([i.open().read() for i in self.input()["ntl"]])
        ntl_df["month"] = pd.to_datetime(ntl_df["month"])
        ntl_df["month_num"] = ntl_df["month"].dt.month
        ntl_df["year"] = ntl_df["month"].dt.year

        df = df.merge(ntl_df, on=["year", "month_num", "longitude", "latitude"])
        with self.output().open("w") as out:
            out.write(df)
