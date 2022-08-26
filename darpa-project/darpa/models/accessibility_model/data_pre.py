import os
from pathlib import Path

import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import rasterio
from fiona.crs import from_epsg
from kiluigi.targets import CkanTarget, IntermediateTarget
from luigi import ExternalTask, Task
from luigi.util import inherits, requires
from osgeo import gdal
from rasterio.features import rasterize
from rasterio.mask import mask
from rasterio.warp import Resampling, reproject
from shapely.geometry import Point, box, mapping, shape

from models.hydrology_model.river_discharge.tasks import MonthlyRiverFloodingTiff
from utils.geospatial_tasks.functions.geospatial import geography_f_country
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

RELATIVEPATH = "models/accessibility_model/data_pre/"


class PullRastersFromCkan(ExternalTask):

    """
    Pull accessibility model raster files from CKAN
    """

    def output(self):
        return {
            "landcover": CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "9800c99c-57d0-48d6-81aa-5b9136a0dd4f"},
            ),
            "towns_rast": CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "22d3c679-5c7a-437e-b3a7-f7b45280b671"},
            ),
            "dem": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "8cdbc26c-cf9f-4107-a405-d4e4e1777631"},
            ),
        }


@requires(PullRastersFromCkan)
class CalculateSlope(Task):
    """
    Calculate slope from DEM
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVEPATH}{self.task_id}.tif", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        src_filename = self.input()["dem"].path
        with self.output().open("w") as out:
            gdal.DEMProcessing(out.name, src_filename, "slope", scale=111120)


class PullShapefilesFromCkan(ExternalTask):
    """
    Pull accessibility model shapefiles from CKAN
    """

    def output(self):
        return {
            "roads": CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "50b8f113-1297-4b48-a50c-5abedef19efa"},
            ),
            "rivers": CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "5793f367-f5c0-49bd-859f-8be4145a0ff0"},
            ),
            "line_boundary": CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "4ba8e9e5-58e2-4b11-8bf0-8a2db8fad889"},
            ),
            # There is no railway in Somalia
            "railway": CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "595b6f79-2d6f-4d1d-8948-5ff9ef6490cc"},
            ),
        }


@requires(PullRastersFromCkan, PullShapefilesFromCkan, GlobalParameters)
class GetGeographyofInfluence(Task):
    """
    Calculate area of influence from geography
    """

    increase_geography_pct = luigi.FloatParameter(default=1.05)

    def output(self):
        path = os.path.join(RELATIVEPATH, self.task_id)
        return IntermediateTarget(path=path, timeout=60 * 60 * 24 * 365)

    def run(self):
        dem_file = self.input()[0]["dem"].path
        landcover_file = self.input()[0]["landcover"].path

        dataset_bounds = self.get_bounds([dem_file, landcover_file])
        dst_minx = max(dataset_bounds[i]["minx"] for i in dataset_bounds)
        dst_miny = max(dataset_bounds[i]["miny"] for i in dataset_bounds)
        dst_maxx = min(dataset_bounds[i]["maxx"] for i in dataset_bounds)
        dst_maxy = min(dataset_bounds[i]["maxy"] for i in dataset_bounds)

        df = gpd.GeoDataFrame(index=[0])
        geography = geography_f_country(self.country_level)
        try:
            geo_mask = geography["features"][0]["geometry"]
        except KeyError:
            geo_mask = geography

        df["geometry"] = shape(geo_mask)

        geo_minx, geo_miny, geo_maxx, geo_maxy = df.total_bounds

        scale_factor = self.increase_geography_pct - 1
        geo_minx = geo_minx - (geo_maxx - geo_minx) * scale_factor
        geo_maxx = geo_maxx + (geo_maxx - geo_minx) * scale_factor
        geo_miny = geo_miny - (geo_maxy - geo_miny) * scale_factor
        geo_maxy = geo_maxy + (geo_maxy - geo_miny) * scale_factor

        minx = geo_minx if geo_minx > dst_minx else dst_minx
        miny = geo_miny if geo_miny > dst_miny else dst_miny
        maxx = geo_maxx if geo_maxx < dst_maxx else dst_maxx
        maxy = geo_maxy if geo_maxy < dst_maxy else dst_maxy

        bbox = box(minx, miny, maxx, maxy)

        geo = gpd.GeoDataFrame({"geometry": bbox}, index=[0], crs=from_epsg(4326))

        mask_geo = mapping(geo.geometry)["features"][0]["geometry"]

        with self.output().open("w") as out:
            out.write(mask_geo)

    def get_bounds(self, filepath):
        bounds = {}
        if isinstance(filepath, str):
            filepath = [filepath]
        for i in filepath:
            key, _ = os.path.splitext(os.path.basename(i))
            if i.endswith("shp"):
                src = gpd.read_file(i)
                minx, miny, maxx, maxy = src.total_bounds
            else:
                with rasterio.open(i) as src:
                    minx, miny, maxx, maxy = src.bounds
            bounds[key] = {}
            bounds[key].update(minx=minx, miny=miny, maxx=maxx, maxy=maxy)
        return bounds


@requires(PullRastersFromCkan, GetGeographyofInfluence, CalculateSlope)
class MaskAndReprojectRasterFiles(Task):
    """
    Mask and reproject raster files to match DEM
    """

    def output(self):
        rasts = ["landcover", "towns_rast", "dem", "slope"]
        return {
            i: IntermediateTarget(
                path=os.path.join(RELATIVEPATH, f"{i}.tif"), task=self, timeout=31536000
            )
            for i in rasts
        }

    def run(self):
        data_map = self.input()[0]
        mask_geo = self.input()[1].open().read()
        data_map = {k: v.path for k, v in data_map.items()}
        data_map["slope"] = self.input()[2].path

        # Mask DEM
        dem_file = data_map["dem"]
        with rasterio.open(dem_file) as src:
            meta = src.meta.copy()
            dem_mask, transform = mask(src, [mask_geo], crop=True)

        h, w = dem_mask.shape[1], dem_mask.shape[2]
        meta.update(
            transform=transform, height=h, width=w, dtype="float32", nodata=-9999.0
        )

        for key in data_map:
            rasterfile = data_map[key]
            with rasterio.open(rasterfile) as src:
                masked, msk_transform = mask(src, [mask_geo], crop=True)
                destination = np.ones((h, w), "float32") * meta.get("nodata")
                reproject(
                    source=masked,
                    destination=destination,
                    src_transform=msk_transform,
                    src_crs=src.crs,
                    src_nodata=src.nodata,
                    dst_transform=meta["transform"],
                    dst_crs=meta["crs"],
                    dst_nodata=meta["nodata"],
                    resampling=Resampling.nearest,
                )
            with self.output()[key].open("w") as out:
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write(destination, 1)


@requires(PullShapefilesFromCkan, GetGeographyofInfluence, MaskAndReprojectRasterFiles)
class MaskAndRasterizeShapefiles(Task):
    """
    Mask and rasterize shapefiles
    """

    def output(self):
        out_list = ["line_boundary", "railway", "rivers", "roads"]
        return {
            i: IntermediateTarget(
                path=os.path.join(RELATIVEPATH, f"{i}.tif"), timeout=31536000, task=self
            )
            for i in out_list
        }

    def run(self):
        line_boundary_zip = self.input()[0]["line_boundary"].path
        railway_zip = self.input()[0]["railway"].path
        rivers_zip = self.input()[0]["rivers"].path
        roads_zip = self.input()[0]["roads"].path

        mask_geo = self.input()[1].open().read()
        raster_map = self.input()[2]

        with rasterio.open(raster_map["dem"].path) as src:
            meta = src.meta.copy()

        road_df = gpd.read_file(f"zip://{roads_zip}")
        road_df.loc[road_df["fclass"] == "primary", "speed"] = (
            road_df["speed"] + self.major_road_speed_offset
        )

        road_df.loc[road_df["fclass"] == "secondary", "speed"] = (
            road_df["speed"] + self.minor_road_speed_offset
        )

        road_df.loc[road_df["fclass"] == "trunk", "speed"] = (
            road_df["speed"] + self.major_road_speed_offset
        )
        road_df.loc[road_df["fclass"] == "tertiary", "speed"] = (
            road_df["speed"] + self.minor_road_speed_offset
        )
        road_df.loc[road_df["fclass"] == "unclassified", "speed"] = (
            road_df["speed"] + self.minor_road_speed_offset
        )
        road_df.loc[road_df["speed"] < 0, "speed"] = 0

        shp_map = {
            "line_boundary": line_boundary_zip,
            "railway": railway_zip,
            "rivers": rivers_zip,
            "roads": road_df,
        }
        for k, v in shp_map.items():
            df = self.mask_vector(v, mask_geo)
            with self.output()[k].open("w") as out:
                self.rasterize_shapefile(df, out.name, meta)

    def mask_vector(self, df, mask_geo):
        if not isinstance(df, gpd.GeoDataFrame):
            df = gpd.read_file(f"zip://{df}")
        df["geometry"] = df.intersection(shape(mask_geo))
        df = df[~(df["geometry"].is_empty & df["geometry"].notna())]
        return df

    def rasterize_shapefile(self, df, rasterfile, meta):
        with rasterio.open(rasterfile, "w", **meta) as out:
            out_shape = out.shape
            shapes = ((geom, value) for geom, value in zip(df.geometry, df.speed))
            try:
                burned = rasterize(
                    shapes=shapes,
                    out_shape=out_shape,
                    fill=out.nodata,
                    transform=out.transform,
                )
                out.write_band(1, burned)
            except ValueError:
                pass


@requires(
    MonthlyRiverFloodingTiff, GetGeographyofInfluence, MaskAndReprojectRasterFiles
)
class MaskAndReprojectFlooding(Task):
    """
    Mask and reproject flood index to match DEM
    """

    def output(self):
        month_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
        return {date.strftime("%Y_%m"): self.dst_file(date) for date in month_list}

    def dst_file(self, date):
        path = Path(RELATIVEPATH) / f"flood_ext_{date.strftime('%Y_%m')}.tif"
        return IntermediateTarget(path=str(path), task=self, timeout=60 * 60 * 24 * 365)

    def run(self):
        flood_ext_src = self.input()[0]
        mask_geo = self.input()[1].open().read()
        data_map = self.input()[2]
        for key, src_target in flood_ext_src.items():
            with rasterio.open(data_map["dem"].path) as src:
                meta = src.meta.copy()
            with src_target.open() as f:
                with rasterio.open(f) as src:
                    masked, msk_transform = mask(src, [mask_geo], crop=True)
                    masked = np.squeeze(masked)
                    if len(masked.shape) == 3:
                        shape = masked.shape[0], meta["height"], meta["width"]
                        meta.update(count=shape[0])
                    else:
                        shape = meta["height"], meta["width"]
                    destination = np.ones(shape, dtype="float32") * meta["nodata"]
                    reproject(
                        source=masked,
                        destination=destination,
                        src_transform=msk_transform,
                        src_crs=src.crs,
                        src_nodata=src.nodata,
                        dst_transform=meta["transform"],
                        dst_crs=meta["crs"],
                        dst_nodata=meta["nodata"],
                        resampling=Resampling.nearest,
                    )
            with self.output()[key].open("w") as out:
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write(destination, 1)


@inherits(GlobalParameters)
class PullMarketLocationFromCkan(ExternalTask):
    """
    Pull Market location from CKAN
    """

    def output(self):
        if self.country_level == "Ethiopia":
            return CkanTarget(
                dataset={"id": "0ba1f319-1132-405a-826c-6ce70906f1f3"},
                resource={"id": "21a2b36c-c701-45ff-89af-7e60afc76241"},
            )
        elif self.country_level == "South Sudan":
            return CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "cd93e3c3-dfeb-4158-bcfc-3fd35dd874e8"},
            )
        elif self.country_level == "Djibouti":
            return CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "9f4922a1-531a-4824-92c5-4f7b165439ba"},
            )
        elif self.country_level == "Sudan":
            return CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "db869f02-42eb-4be0-b780-64135fa167e0"},
            )
        elif self.country_level == "Kenya":
            return CkanTarget(
                dataset={"id": "303dcd30-bef7-4928-bd1a-77c3c50d0e44"},
                resource={"id": "febe07dc-dc3f-4d6f-97cc-498ae3d39037"},
            )
        elif self.country_level == "Uganda":
            return CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "417ff6a0-c247-421c-80cd-66680320cf0b"},
            )
        elif self.country_level == "Somalia":
            return CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "4b43a6a3-797e-4bf3-8fe9-e39230561d9e"},
            )
        elif self.country_level == "Eritrea":
            return CkanTarget(
                dataset={"id": "081a3cca-c6a7-4453-b93c-30ec1c2aec37"},
                resource={"id": "d84f1997-7546-4a46-b5d9-b7edbe47c123"},
            )
        else:
            raise NotImplementedError


@requires(PullMarketLocationFromCkan)
class PrepareMarketLocationData(Task):
    """
    Convert the data to geodataframe
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 365)

    def run(self):
        df = pd.read_csv(self.input().path)
        point_geom = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
        gdf = gpd.GeoDataFrame(df, geometry=point_geom)

        with self.output().open("w") as out:
            out.write(gdf)
