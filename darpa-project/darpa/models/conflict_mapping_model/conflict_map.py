import json
import zipfile
import os
import luigi
from luigi import ExternalTask, Task
from luigi.configuration import get_config
from luigi.util import requires
from shapely.geometry import Point
from kiluigi.targets import IntermediateTarget, SerializerTarget, ExpiringLocalTarget
from kiluigi.tasks import ExternalTask, Task, ReadDataFrameTask  # noqa: F811


from kiluigi.targets.ckan import CkanTarget


import geopandas as gpd
from utils.geospatial_tasks.functions.geospatial import (
    get_raster_proj_configfile,
    rasterize_vectorfile,
)


config = get_config()


class DataFilesCkan(ExternalTask):
    def output(self):
        return {
            "acled": CkanTarget(
                dataset={"id": "fe67ac03-e905-4553-98bd-b368a1273184"},
                resource={"id": "0c7626eb-3d3d-4330-969c-a2a7be43233a"},
            )
        }


class GeoFilesCkan(ExternalTask):
    def output(self):
        return {
            "ethnicity": CkanTarget(
                dataset={"id": "2af662c6-f5a9-400b-a7b2-5f4d32a573dd"},
                resource={"id": "ed440f1e-bda5-44a7-843b-c44ed9a33d65"},
            ),
            "admin0": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "4ca5e4e8-2f80-44c4-aac4-379116ffd1d9"},
            ),
        }


@requires(DataFilesCkan)
class ReadData(ReadDataFrameTask):

    read_method = "read_excel"
    timeout = 10
    read_args = {"infer_datetime_format": True}


@requires(ReadData, GeoFilesCkan)
class PrepData(Task):
    def output(self):
        return IntermediateTarget("ethnic_map_conflict", timeout=100)

    def run(self):
        data_dict = self.input()[0].open().read()

        zip_ref = zipfile.ZipFile(self.input()[1]["ethnicity"].path, "r")
        zip_ref.extractall()
        fp = zip_ref.extract("SSD_ethnic_2.shp")
        zip_ref.close()
        ethnic_map = gpd.read_file(fp)

        # Extract shapefile of south sudan
        zip_ref = zipfile.ZipFile(self.input()[1]["admin0"].path, "r")
        zip_ref.extractall()
        fp = zip_ref.extract("ss_admin0.shp")
        zip_ref.close()
        admin0 = gpd.read_file(fp)
        poly = gpd.GeoSeries(admin0.loc[0, "geometry"])

        df = data_dict["acled"]
        df["geometry"] = df.apply(
            lambda x: Point((float(x.LONGITUDE), float(x.LATITUDE))), axis=1
        )
        df = gpd.GeoDataFrame(df, geometry="geometry")
        df["YEAR_STR"] = df["YEAR"].astype(str)
        ss = df.loc[(df["COUNTRY"] == "South Sudan") | (df["COUNTRY"] == "Sudan")]

        # Extract all data within the limits of south sudan
        ss_df = ss.loc[ss["geometry"].intersects(poly[0])].copy()

        output_data = {"acled": ss_df, "ethnicity": ethnic_map}

        with self.output().open("w") as output:
            output.write(output_data)


@requires(PrepData)
class DefineConlifctMarker(Task):

    # fname = luigi.Parameter(default="data/conflict_shapefile.shp")
    output_path = "data/"
    if not os.path.exists(output_path):
        os.makedirs(output_path, exist_ok=True)

    def output(self):
        return ExpiringLocalTarget(
            os.path.join(self.output_path, "conflict_shapefile.shp"), timeout=100
        )

    def run(self):
        data_dict = self.input().open().read()
        df = data_dict["acled"]
        schema = gpd.io.file.infer_schema(df)

        schema["properties"]["TIMESTAMP"] = "datetime"
        schema["properties"]["EVENT_DATE"] = "datetime"
        df.to_file(self.output().path, schema=schema)


@requires(DefineConlifctMarker)
class RasterizeConflictMap(Task):

    attr_field = "conflict_flg"
    proj_fname = "ss_raster_proj.json"

    def output(self):
        return SerializerTarget("something", timeout=100)

    def run(self):
        get_raster_proj_configfile(
            luigi.get("paths", "ss_proj"), fname_out=self.proj_fname
        )
        with self.proj_fname.open("r") as fid:
            ref_proj = json.load(fid)
        vector_file = self.input().path
        rasterize_vectorfile(
            ref_proj,
            vector_file,
            self.attr_field,
            no_data_val=-9999,
            fname_out="out.tif",
        )
        pass


# roads = gpd.read_file('/Users/alicampion/Downloads/ssd_roads_v2/ssd_roads_v2.shp')
##
# fig, ax = plt.subplots(figsize=[12, 12])
# output_data['ethnicity'].plot(column='G1Name', ax=ax, alpha=0.3, cmap='YlGnBu', legend=True)
# roads.plot(ax=ax, color='black', alpha=0.5)
# output_data['acled'].plot(column='YEAR_STR', ax=ax, cmap='copper')
#
# ax.set_aspect('equal')
# ax.set_xlabel('Longitude', fontsize=12)
# ax.set_ylabel('Latitude', fontsize=12)
# ax.set_title('Conflict in South Sudan 2011-2018', fontsize=14)
# leg = ax.get_legend()
# leg.set_bbox_to_anchor((1.2, 1))
# ax.grid(True)
# plt.savefig('initial_map_3.svg', bbox_inches='tight')
#
# fig, ax = plt.subplots(figsize=[12, 6])
# ss.plot(x='EVENT_DATE', y='FATALITIES', ax=ax)

# g = sns.FacetGrid(ss_df, hue='EVENT_TYPE', col="EVENT_TYPE", size=3, aspect = 2,
#                  margin_titles=False, sharex=True, sharey=False, col_wrap=3)
# g.map(plt.scatter, 'EVENT_DATE', "FATALITIES", marker='o')
# g.set_titles('{col_name}', size=14, weight='bold')
# g.set_ylabels('Fatalities')
# g.set_xlabels('Time')
# plt.savefig('conflict_overview_1.png')
