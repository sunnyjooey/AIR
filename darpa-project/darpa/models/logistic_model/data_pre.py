import datetime
import logging
import tempfile
from pathlib import Path

import fiona
import geopandas as gpd
import luigi
import networkx as nx
import numpy as np
import osmnx as ox
import pandas as pd
import rasterio
from luigi import ExternalTask, Task
from luigi.util import inherits, requires
from rasterio.mask import mask
from shapely.geometry import MultiLineString, MultiPoint, mapping, shape
from shapely.ops import nearest_points

from kiluigi.targets import CkanTarget, IntermediateTarget
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from ..agromet.utils import mask_rasterfile
from .mapping import road_surface_map
from .utils import (
    country_f_coordinates,
    extract_stats_buffer,
    raster_t_df,
    reverse_geocode,
    stats_buffer,
)

logger = logging.getLogger("luigi-interface")

ox.config(use_cache=True, log_console=True)

RELATIVEPATH = "models/logistic_model/data_pre/"


class PullTarrifDataFromCkan(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"id": "d32aa514-e2e6-4c7d-9fb7-c3a09808e4c3"},
        )


class PullCrudeOilPrice(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"id": "9e98e5c0-2d81-4643-bc2a-0c6a5a06aaeb"},
        )


class PullPopulationData(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"id": "9a2fd1ce-8258-46f1-a797-3de5d754c10b"},
        )


@requires(PullTarrifDataFromCkan, PullPopulationData, PullCrudeOilPrice)
class ODPopulation(Task):
    """
    Extract population statistics within a given radius from ODs
    """

    radius = luigi.FloatParameter(default=0.1)
    stats = luigi.ListParameter(default=["min", "max", "mean", "sum", "median", "std"])

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    @staticmethod
    def get_date(row):
        date = datetime.date(int(row["year"]), int(row["month"]), 1)
        return date

    def get_input(self):
        df = pd.read_csv(self.input()[0].path)
        df = df.dropna(
            subset=[
                "Olongitude",
                "Olatitude",
                "Dlongitude",
                "Dlatitude",
                "year",
                "month",
            ]
        )
        df["date"] = df.apply(lambda row: self.get_date(row), axis=1)
        df["date"] = pd.to_datetime(df["date"])
        df_crude = pd.read_excel(self.input()[2].path)
        df_crude = df_crude.rename(
            columns={"Month": "date", "Price": "crude_oil_price"}
        )
        df_crude["date"] = pd.to_datetime(df_crude["date"])
        df = df.merge(df_crude, on="date", how="left")
        df = df.drop(["date", "Change"], axis=1)
        pop_src = self.input()[1].path
        return df, pop_src

    def run(self):
        df, pop_src = self.get_input()

        # Extract popluation at Origin
        gdf = extract_stats_buffer(
            df,
            pop_src,
            "origin_pop",
            "Olongitude",
            "Olatitude",
            self.radius,
            self.stats,
        )
        # Extract popluation at destination
        gdf = extract_stats_buffer(
            gdf, pop_src, "dest_pop", "Dlongitude", "Dlatitude", self.radius, self.stats
        )
        with self.output().open("w") as out:
            out.write(gdf)


@requires(ODPopulation)
class ReverseGeocoding(Task):
    """
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    @staticmethod
    def border_crossing_func(orig, dest):
        if any(pd.isnull(i) for i in [orig, dest]):
            return np.nan
        elif orig == dest:
            return 0
        else:
            return 1

    def run(self):
        with self.input().open() as src:
            df = src.read()

        df = df.reset_index(drop=True)
        new_vars = ["country_dest", "country_origin", "travel_time", "google_dist"]
        df[new_vars] = df.apply(reverse_geocode, axis=1, result_type="expand")

        df["border_crossing"] = df.apply(
            lambda x: self.border_crossing_func(x["country_dest"], x["country_origin"]),
            axis=1,
        )
        with self.output().open("w") as out:
            out.write(df)


@inherits(GlobalParameters)
class PullRoadDataFromOSM(ExternalTask):
    """
    Pull road network data from OSM
    """

    road_class = luigi.ListParameter(
        default=[
            "motorway",
            "trunk",
            "primary",
            "secondary",
            "tertiary",
            "unclassified",
        ]
    )
    countries = luigi.ListParameter(
        default=[
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
    )

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        try:
            geo_area = self.geography["features"][0]["geometry"]
        except KeyError:
            geo_area = self.geography

        west, south, east, north = shape(geo_area).bounds

        rd_class = None
        for i in self.road_class:
            if rd_class is None:
                rd_class = i
            else:
                rd_class = rd_class + "|" + i

        if len(self.countries) > 0:
            for index, country in enumerate(self.countries):
                temp = ox.graph_from_place(
                    country,
                    network_type="none",
                    infrastructure=f'way["highway"~"{rd_class}"]',
                )
                if index == 0:
                    G = temp
                else:
                    G = nx.compose(G, temp)
        else:
            G = ox.graph_from_bbox(
                north,
                south,
                east,
                west,
                network_type="none",
                infrastructure=f'way["highway"~"{rd_class}"]',
            )
        with self.output().open("w") as out:
            out.write(G)


@requires(PullRoadDataFromOSM, ReverseGeocoding)
class CalcuateODLength(Task):
    """
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        try:
            with self.input()[0].open() as src:
                G = src.read()
        except AttributeError:
            G = ox.load_graphml(self.input()[0].path)

        with self.input()[1].open() as src:
            df = src.read()

        df = df.dropna(subset=["Olatitude", "Dlatitude", "Olongitude", "Dlongitude"])

        df = df.reset_index(drop=True)
        df["distance_calc"] = np.nan
        df["O_dist_t_node"] = np.nan
        df["D_dist_t_node"] = np.nan
        edges = ox.graph_to_gdfs(G, nodes=False).set_index(["u", "v"]).sort_index()
        geometry = []
        drop_index = []
        for index, row in df.iterrows():
            try:
                orig_coords = (row["Olatitude"], row["Olongitude"])
                dest_coords = (row["Dlatitude"], row["Dlongitude"])

                orig_node, dist_o_node = ox.get_nearest_node(
                    G, orig_coords, return_dist=True
                )
                dest_node, dist_d_node = ox.get_nearest_node(
                    G, dest_coords, return_dist=True
                )

                lenth, route = nx.single_source_dijkstra(
                    G, orig_node, dest_node, weight="length"
                )

                route_pairwise = zip(route[:-1], route[1:])
                lines = [edges.loc[uv, "geometry"].iloc[0] for uv in route_pairwise]
                geometry.append(MultiLineString(lines))
                col_names = ["distance_calc", "O_dist_t_node", "D_dist_t_node"]
                col_values = (lenth, dist_o_node, dist_d_node)
                df.loc[index, col_names] = col_values
            except nx.NetworkXNoPath:
                drop_index.append(index)

        df = df.drop(drop_index)
        df["geometry"] = geometry
        df = df.dropna(subset=["geometry"])
        with self.output().open("w") as out:
            out.write(df)


class PullRasterFromCkan(ExternalTask):
    """
    """

    def output(self):
        return {
            "dem": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "8cdbc26c-cf9f-4107-a405-d4e4e1777631"},
            ),
            "night_lights": CkanTarget(
                dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
                resource={"id": "e73d41e4-2ca4-4ab6-91d2-be2785b8f06c"},
            ),
            "acled": CkanTarget(
                dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
                resource={"id": "8609ad1c-03ef-4da6-81ee-aef3452aa64f"},
            ),
            "road_quality": CkanTarget(
                dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
                resource={"id": "19dfaeda-e215-43c0-8002-3b67546d64d4"},
            ),
        }


@requires(CalcuateODLength, PullRasterFromCkan)
class ExtractStatsFromRasters(Task):
    """
    """

    stats = luigi.ListParameter(default=["min", "max", "mean", "std", "median"])

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def extract_summary_stats(self, df, raster_src, var_name):
        for i in self.stats:
            df[i] = np.nan
        with rasterio.open(raster_src) as src:
            for index, row in df.iterrows():
                try:
                    masked, _ = mask(
                        src, [mapping(row["geometry"])], crop=True, all_touched=True
                    )
                    masked = masked.astype("float32")
                    masked[masked == src.nodata] = np.nan
                    df.loc[index, "min"] = np.nanmin(masked)
                    df.loc[index, "max"] = np.nanmax(masked)
                    df.loc[index, "mean"] = np.nanmean(masked)
                    df.loc[index, "median"] = np.nanmedian(masked)
                    df.loc[index, "std"] = np.nanstd(masked)
                except IndexError:
                    pass

        df = df.rename(columns={i: f"{var_name}_{i}" for i in self.stats})
        return df

    def cat_zonal_stats(self, df, raster_src):
        for k in road_surface_map:
            df[k] = np.nan
        with rasterio.open(raster_src) as src:
            for index, row in df.iterrows():
                try:
                    arr, _ = mask(
                        src, [mapping(row["geometry"])], crop=True, all_touched=True
                    )
                    for k, v in road_surface_map.items():
                        df.loc[index, k] = (arr == v).sum()
                except (IndexError, ValueError):
                    pass
        return df

    @staticmethod
    def road_surface_condition(df):
        surface_list = ["good", "fair", "bad", "excellent", "poor", "very_poor"]
        df["total"] = df[surface_list].sum(axis=1)
        for i in surface_list:
            df[f"surface_condition_{i}"] = df[i] / df["total"]
        df = df.drop(surface_list + ["total"], axis=1)
        return df

    def run(self):
        raster_map = {k: v.path for k, v in self.input()[1].items()}
        with self.input()[0].open() as src:
            df = src.read()

        df = df.dropna(subset=["geometry"])
        df = gpd.GeoDataFrame(df, geometry="geometry")

        for k, v in raster_map.items():
            if k != "road_quality":
                df = self.extract_summary_stats(df, v, k)
            else:
                df = self.cat_zonal_stats(df, v)
            logger.info(f"{k} extraction complete")
        df = self.road_surface_condition(df)
        with self.output().open("w") as out:
            out.write(df)


@requires(ExtractStatsFromRasters)
class UploadLogisticTrainingData(Task):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"name": "Logistic Model Training Data V2 with Geom"},
        )

    def run(self):
        with self.input().open() as src:
            df = src.read()
        with tempfile.TemporaryDirectory() as tmpdirname:
            dst_file = str(
                Path(tmpdirname) / "logistic_model_training_data_w_geom.geojson"
            )
            df.to_file(dst_file, driver="GeoJSON")
            self.output().put(dst_file)


class PullHospitalSchoolData(ExternalTask):
    def output(self):
        return {
            "schools": CkanTarget(
                dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
                resource={"id": "420a4b5f-dee4-4198-a853-634cf576a5d4"},
            ),
            "hospitals": CkanTarget(
                dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
                resource={"id": "045aa08d-f29e-4b2c-a48a-8987d4cd854f"},
            ),
        }


class PullWarehouseData(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"id": "5c65d7de-1bf0-45b3-9582-5863a597baac"},
        )


class PullCountryShapefile(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"id": "5a86e1cd-9caa-4882-8844-cc84251b3930"},
        )


@requires(PullWarehouseData, PullPopulationData, PullCountryShapefile)
@inherits(ODPopulation)
class WarehousePopulationStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        df = pd.read_csv(self.input()[0].path)
        pop_src = self.input()[1].path
        df = df.rename(columns={"Long": "Olongitude", "Lat": "Olatitude"})
        df = df.reset_index(drop=True)
        df = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["Olongitude"], df["Olatitude"]),
            crs="EPSG:4326",
        )
        df_count = gpd.read_file(f"zip://{self.input()[2].path}")
        df["Ocountry"] = df.apply(
            lambda x: country_f_coordinates(df_count, x["Olatitude"], x["Olongitude"]),
            axis=1,
        )
        df = extract_stats_buffer(
            df,
            pop_src,
            "origin_pop",
            "Olongitude",
            "Olatitude",
            self.radius,
            self.stats,
        )
        df = df[[i for i in df.columns if i != "geometry"]]
        with self.output().open("w") as out:
            out.write(df)


class PullProjectedRoadData(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"id": "6273464b-fe23-487e-9092-b1512a767ff0"},
        )


@requires(PullProjectedRoadData, PullPopulationData)
@inherits(GlobalParameters)
class PossiblePointsofService(Task):

    distance_f_road = luigi.FloatParameter(default=200)
    population_threshold = luigi.IntParameter(default=20)

    def output(self):
        dst = Path(RELATIVEPATH) / "possible_pos.tif"
        return IntermediateTarget(path=str(dst), task=self, timeout=31536000)

    def run(self):

        try:
            mask_geom = self.geography["features"][0]["geometry"]
        except KeyError:
            mask_geom = self.geography

        out_image, out_meta = self.raster_mask(self.input()[1].path, mask_geom)

        # Population above threshold
        # Define bins >20pop, and then classify the data
        class_bins = [self.population_threshold, np.iinfo(np.int32).max]
        # Classify the original image array, then unravel it again for plotting
        lspop2015_Eth_class = np.digitize(out_image, class_bins)
        lspop2015_Eth_class_ma = np.ma.masked_where(
            lspop2015_Eth_class == 0, lspop2015_Eth_class, copy=True
        )

        temp = tempfile.NamedTemporaryFile(suffix=".tif")
        with rasterio.open(temp.name, "w", **out_meta) as dst:
            dst.write(lspop2015_Eth_class_ma.astype(out_meta["dtype"]))

        road_geom = self.distance_f_road_buffer()
        masked, meta = self.raster_mask(temp.name, road_geom)
        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write(masked.astype(meta["dtype"]))

    @staticmethod
    def raster_mask(src_file, geom):
        if not isinstance(geom, list):
            geom = [geom]
        with rasterio.open(src_file) as src:
            out_image, out_transform = mask(src, geom, crop=True, all_touched=True)
            meta = src.meta.copy()
        _, h, w = out_image.shape
        meta.update(height=h, width=w, transform=out_transform)
        return out_image, meta

    def distance_f_road_buffer(self):
        df = gpd.read_file(f"zip://{self.input()[0].path}")
        df["geometry"] = df["geometry"].buffer(self.distance_f_road)
        df = df.to_crs(epsg=4326)
        return [i["geometry"] for i in mapping(df["geometry"])["features"]]


@requires(PossiblePointsofService, PullPopulationData, PullCountryShapefile)
@inherits(ODPopulation)
class PossiblePOSPopulationStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def run(self):
        pop_src = self.input()[1].path
        df = raster_t_df(
            self.input()[0].path, "possible_pos", "Dlongitude", "Dlatitude"
        )
        df = df.reset_index(drop=True)
        df = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["Dlongitude"], df["Dlatitude"]),
            crs="EPSG:4326",
        )
        df_count = gpd.read_file(f"zip://{self.input()[2].path}")
        df["Dcountry"] = df.apply(
            lambda x: country_f_coordinates(df_count, x["Dlatitude"], x["Dlongitude"]),
            axis=1,
        )
        df = extract_stats_buffer(
            df, pop_src, "dest_pop", "Dlongitude", "Dlatitude", self.radius, self.stats
        )
        df = df[[i for i in df.columns if i != "geometry"]]
        with self.output().open("w") as out:
            out.write(df)


@requires(PullHospitalSchoolData, PullPopulationData, PullCountryShapefile)
@inherits(ODPopulation)
class SchoolHospitalPopulationStats(Task):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    @staticmethod
    def read_data(filepath):
        try:
            df = pd.read_csv(filepath)
        except ValueError:
            df = gpd.read_file(f"zip://{filepath}")
        return df

    def run(self):
        pop_src = self.input()[1].path
        data_map = {k: self.read_data(v.path) for k, v in self.input()[0].items()}
        data_map["schools"] = data_map["schools"].rename(
            columns={"Long": "Dlongitude", "Lat": "Dlatitude"}
        )

        data_map["hospitals"]["Dlongitude"] = data_map["hospitals"]["geometry"].x
        data_map["hospitals"]["Dlatitude"] = data_map["hospitals"]["geometry"].y

        df = pd.concat([data_map["schools"], data_map["hospitals"]], sort=True)
        df = df.reset_index(drop=True)
        df = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df["Dlongitude"], df["Dlatitude"]),
            crs="EPSG:4326",
        )
        df_count = gpd.read_file(f"zip://{self.input()[2].path}")
        df["country_dest"] = df.apply(
            lambda x: country_f_coordinates(df_count, x["Dlatitude"], x["Dlongitude"]),
            axis=1,
        )
        df["geometry"] = df["geometry"].buffer(self.radius)
        df = stats_buffer(df, pop_src, "dest_pop", self.stats)
        df = df[[i for i in df.columns if i != "geometry"]]
        with self.output().open("w") as out:
            out.write(df)


@inherits(GlobalParameters)
class MergeOriginAndDestination(Task):

    nearest_warehouse = luigi.BoolParameter(default=False)
    use_point_interest = luigi.BoolParameter(default=False)

    def requires(self):
        origin_task = self.clone(WarehousePopulationStats)
        if self.use_point_interest:
            dest_task = self.clone(SchoolHospitalPopulationStats)
        else:
            dest_task = self.clone(PossiblePOSPopulationStats)
        return origin_task, dest_task

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    @staticmethod
    def get_origin_long_lat(point, origin_list):
        # find the nearest point
        nearest_point = nearest_points(point, origin_list)[1]
        long, lat = nearest_point.x, nearest_point.y
        return long, lat

    def run(self):
        with self.input()[0].open() as src:
            df_origin = src.read()
        with self.input()[1].open() as src:
            df_dest = src.read()

        if self.nearest_warehouse:
            df_dest = gpd.GeoDataFrame(
                df_dest,
                geometry=gpd.points_from_xy(
                    df_dest["Dlongitude"], df_dest["Dlatitude"]
                ),
            )

            df_origin = gpd.GeoDataFrame(
                df_origin,
                geometry=gpd.points_from_xy(
                    df_origin["Olongitude"], df_origin["Olatitude"],
                ),
            )

            origin_points = MultiPoint(df_origin["geometry"].tolist())

            df_dest[["Olongitude", "Olatitude"]] = df_dest.apply(
                lambda x: self.get_origin_long_lat(x.geometry, origin_points),
                axis=1,
                result_type="expand",
            )
            var_list = [
                "Ocountry",
                "Olatitude",
                "Olongitude",
                "Warehouse",
                "origin_pop_max",
                "origin_pop_mean",
                "origin_pop_median",
                "origin_pop_min",
                "origin_pop_std",
                "origin_pop_sum",
            ]
            gdf = df_dest.merge(
                df_origin[var_list], on=["Olongitude", "Olatitude"], how="left",
            )
        else:
            for _df in (df_dest, df_origin):
                _df["merge_on"] = 1
            gdf = pd.merge(df_dest, df_origin, on="merge_on", how="outer")
            gdf = gdf.drop("merge_on", axis=1)
        gdf = gdf.rename(columns={"Warehouse": "origin"})
        with self.output().open("w") as out:
            out.write(gdf)


class PullEthiopiaRoadData(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"id": "ef187831-23b3-4663-8ae2-114352cec850"},
        )


@requires(PullEthiopiaRoadData, MergeOriginAndDestination)
class GetRouteGeometry(CalcuateODLength):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)


class PullInferenceRasterFromCkan(ExternalTask):
    """
    """

    def output(self):
        return {
            "dem": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "8cdbc26c-cf9f-4107-a405-d4e4e1777631"},
            ),
            "night_lights": CkanTarget(
                dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
                resource={"id": "e73d41e4-2ca4-4ab6-91d2-be2785b8f06c"},
            ),
            "acled": CkanTarget(
                dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
                resource={"id": "8609ad1c-03ef-4da6-81ee-aef3452aa64f"},
            ),
            "road_quality": CkanTarget(
                dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
                resource={"id": "dcb7ef07-7972-4f08-a3f8-3cbbbae824f7"},
            ),
        }


@requires(GetRouteGeometry, PullInferenceRasterFromCkan)
class ExtractRouteStats(ExtractStatsFromRasters):
    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)


@requires(ExtractRouteStats)
@inherits(PossiblePointsofService)
class CacheInferenceData(Task):
    admin1_name = luigi.ChoiceParameter(choices=["oromia", "gambela"], default="oromia")

    def output(self):
        return CkanTarget(
            dataset={"id": "3a9a3e86-f967-4a55-bfa1-84a729e378f6"},
            resource={"name": self.get_name()},
        )

    def get_name(self):
        base_name = f"logistic_inference_{self.admin1_name}_v3{self.nearest_warehouse}_{self.use_point_interest}_"
        sign_params = (
            f"{self.country_level}_{self.population_threshold}_{self.distance_f_road}"
        )
        return base_name + sign_params

    def run(self):
        with self.input().open() as src:
            df = src.read()
        with tempfile.TemporaryDirectory() as tmpdirname:
            dst_file = Path(tmpdirname) / f"{self.get_name()}.geojson"
            with fiona.Env():
                df.to_file(str(dst_file), driver="GeoJSON")
            self.output().put(str(dst_file))


@requires(PullPopulationData)
@inherits(GlobalParameters)
class MaskPopulationData(Task):
    def output(self):
        dst = Path(RELATIVEPATH) / "population_masked.tif"
        return IntermediateTarget(path=str(dst), task=self, timeout=5184000)

    def run(self):
        try:
            mask_geom = self.geography["features"][0]["geometry"]
        except KeyError:
            mask_geom = self.geography

        masked, meta = mask_rasterfile(self.input().path, mask_geom)

        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write(masked.astype(meta["dtype"]), 1)
