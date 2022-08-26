import calendar
import datetime
import json
import logging
import os

import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import rasterio
from luigi import Task
from luigi.util import requires
from rasterio.features import shapes
from rasterio.io import MemoryFile
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject
from shapely.geometry import mapping, shape

from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from models.hydrology_model.flood_index.data_cleaning import (
    MaskDEMTOBasin,
    MaskRainfallDataToMatchDEM,
    MaskSoilMositureToMatchDEM,
    MaskSubSurfaceRunoffToMatchDEM,
    MaskSurfaceRunoffToMatchDEM,
    SelectRiverBasinFromGeography,
)
from models.hydrology_model.flood_index.upstream_runoff_climatology import (
    UpstreamRunoffMixin,
)
from models.hydrology_model.utils import resample_raster_to_array
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters
from utils.visualization_tasks.push_csv_to_database import PushCSVToDatabase

logger = logging.getLogger("luigi-interface")

RELATIVE_PATH = "models/hydrology_model/flood_index/tasks"


@requires(GlobalParameters)
class PullUpstreamRunoffClimatologyFromCkan(Task):
    """
    Pull upsream runoff climatology from ckan
    """

    def output(self):
        return CkanTarget(
            resource={"id": "526bbeb6-e0b1-4418-aa86-10fc82ea0d78"},
            dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
        )


@requires(
    MaskSurfaceRunoffToMatchDEM,
    MaskDEMTOBasin,
    MaskRainfallDataToMatchDEM,
    MaskSubSurfaceRunoffToMatchDEM,
)
class UpstreamRunoff(UpstreamRunoffMixin, Task):
    """
    Get upstream runoff
    """

    def output(self):
        out_dir = f"{RELATIVE_PATH}/{self.task_id}/"
        return IntermediateTarget(path=out_dir, timeout=60 * 60 * 24 * 365)

    def run(self):
        sur_runoff_dir = self.input()[0].path
        dem_dir = self.input()[1].path
        rain_dir = self.input()[2].path
        sub_sur_runoff_dir = self.input()[3].path

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            for dem in os.listdir(dem_dir):
                dem_file = os.path.join(dem_dir, dem)
                basin_id = dem.replace(".tif", "")
                with rasterio.open(dem_file) as src:
                    meta = src.meta.copy()

                # Flow direction
                flow_dir_path = os.path.join(tmpdir, f"flow_dir_{dem}")
                self.flow_direction(dem_file, flow_dir_path)

                # Number of upstream cells
                num_cells_path = os.path.join(tmpdir, f"num_cell_{dem}")
                self.num_of_upstream_cells(flow_dir_path, num_cells_path)

                rainfall_files = [
                    i for i in os.listdir(rain_dir) if f"{basin_id}_rainfall_flux" in i
                ]
                for rain_file in rainfall_files:
                    i = rain_file.split("_")[-1]
                    month = (i.replace(".tif", ""),)
                    logger.info(
                        f"Calculating upstream runoff for the month {month} basin {basin_id}"
                    )
                    rain_arr = self.resample_raster(
                        os.path.join(rain_dir, rain_file), dem_file
                    )
                    surface_arr = self.resample_raster(
                        os.path.join(
                            sur_runoff_dir, f"{basin_id}_surface_runoff_amount_{i}"
                        ),
                        dem_file,
                    )
                    subsurface_arr = self.resample_raster(
                        os.path.join(
                            sub_sur_runoff_dir,
                            f"{basin_id}_subsurface_runoff_amount_{i}",
                        ),
                        dem_file,
                    )

                    coef_dir = os.path.join(tmpdir, f"{basin_id}_coefficient_{i}")
                    self.runoffcoefficient(
                        surface_arr, subsurface_arr, rain_arr, coef_dir, meta
                    )

                    flow_accum_path = os.path.join(
                        tmpdir, f"{basin_id}_flow_accum_dir_{i}"
                    )
                    self.flow_accumulation(flow_dir_path, flow_accum_path, coef_dir)

                    upstream_runoff_dir = os.path.join(tmpdir, f"{basin_id}_{i}")
                    self.upstream_runoff(
                        flow_accum_path, num_cells_path, upstream_runoff_dir
                    )

                    for file in [coef_dir, flow_accum_path]:
                        os.remove(file)
                for file in [flow_dir_path, num_cells_path]:
                    os.remove(file)


@requires(
    UpstreamRunoff, PullUpstreamRunoffClimatologyFromCkan, SelectRiverBasinFromGeography
)
class EuropeanRunoffIndex(Task):
    """
    Calculate European Runoff index based on cimatology
    """

    def output(self):
        out_dir = f"{RELATIVE_PATH}/{self.task_id}/"
        return IntermediateTarget(path=out_dir, timeout=60 * 60 * 24 * 365)

    def run(self):
        runoff_dir = self.input()[0].path
        climatology_dir = self.input()[1].path
        basin_df = self.input()[2].open().read()
        basin_df = basin_df.set_index("SUB_BAS")
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            for i in os.listdir(runoff_dir):
                runoff_path = os.path.join(runoff_dir, i)
                with rasterio.open(runoff_path) as src:
                    runoff_arr = src.read(1)
                    runoff_nodata = src.nodata
                    meta = src.meta.copy()
                clim_arr, clim_nodata = self.mask_rastefile(
                    climatology_dir, basin_df, i
                )
                if runoff_arr.shape == clim_arr.shape:
                    runoff_arr = np.where(
                        (runoff_arr == runoff_nodata) | (clim_arr == clim_nodata),
                        runoff_nodata,
                        runoff_arr / clim_arr,
                    )
                    dst_file = os.path.join(tmpdir, i)
                    with rasterio.open(dst_file, "w", **meta) as dst:
                        dst.write(runoff_arr.astype(meta["dtype"]), 1)

    def mask_rastefile(self, src_raster, gdf, src_runoff):
        index = int(src_runoff.split("_")[0])
        geo_mask = mapping(gdf.loc[index, "geometry"])
        with rasterio.open(src_raster) as src:
            masked, _ = mask(src, [geo_mask], crop=True)
            nodata = src.nodata
        return np.squeeze(masked), nodata


@requires(
    EuropeanRunoffIndex, MaskSoilMositureToMatchDEM, MaskDEMTOBasin, GlobalParameters
)
class FloodedAreaGeotiff(Task):
    """
    Identify pixel whose value (European runoff index based on climatology) is greater than the threshold
    """

    flood_index_threshold = luigi.FloatParameter(default=1.0)
    soil_moisture_threshold = luigi.FloatParameter(default=0.40)

    def output(self):
        out_dir = f"{RELATIVE_PATH}/{self.task_id}/"
        return IntermediateTarget(path=out_dir, timeout=60 * 60 * 24 * 365)

    def run(self):
        try:
            mask_geom = self.geography["features"][0]["geometry"]
        except KeyError:
            mask_geom = self.geography

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            index_base_dir = self.input()[0].path
            sm_dir = self.input()[1].path
            dem_dir = self.input()[2].path
            file_list = [i for i in os.listdir(index_base_dir) if i.endswith(".tif")]
            basin_file_list = []
            for i in file_list:
                with rasterio.open(os.path.join(index_base_dir, i)) as src:
                    array_base = src.read(1)
                    meta = src.profile.copy()
                    nodata_base = src.nodata

                dem_file = os.path.join(dem_dir, f"{i.split('_')[0]}.tif")
                basin_id, month = i.split("_")
                soil_file = os.path.join(
                    sm_dir, f"{basin_id}_soil_moisture_content_{month}"
                )
                sm_array = resample_raster_to_array(soil_file, dem_file)
                flood_base = self.get_flooded_pixels(array_base, sm_array, nodata_base)

                meta.update(count=1, dtype="float32")
                dst_file = os.path.join(tmpdir, i)
                basin_file_list.append(dst_file)
                with rasterio.open(dst_file, "w", **meta) as dst:
                    dst.write_band(1, flood_base.astype(meta["dtype"]))

            # Merge river basin
            dst_file_set = {i.split("_")[-1] for i in basin_file_list}
            for dst_file in dst_file_set:
                logger.info(f"Merging files for {dst_file.replace('.tif', '')}")
                river_basin_list = [i for i in basin_file_list if i.endswith(dst_file)]
                self.merge_river_basin(
                    river_basin_list, os.path.join(tmpdir, dst_file), mask_geom
                )

    def get_flooded_pixels(self, index_arr, soil_arr, nodata):
        dst_arr = np.where(
            (index_arr == nodata) | (soil_arr == nodata),
            nodata,
            np.where(
                (index_arr > self.flood_index_threshold)
                & (soil_arr > self.soil_moisture_threshold),
                1,
                0,
            ),
        )
        return dst_arr

    def merge_river_basin(self, river_basin_list, dst_file, mask_geom):
        src_list = [rasterio.open(i) for i in river_basin_list]
        merged_arry, transform = merge(src_list)
        meta = src_list[0].meta.copy()
        h, w = merged_arry.shape[1], merged_arry.shape[2]
        meta.update(transform=transform, height=h, width=w)
        with MemoryFile() as memfile:
            with memfile.open(**meta) as dst:
                dst.write(merged_arry.astype(meta["dtype"]))
            with memfile.open() as src:
                masked, transform = mask(src, [mask_geom], crop=True)
                h, w = masked.shape[1], masked.shape[2]
        meta.update(transform=transform, height=h, width=w)
        with rasterio.open(dst_file, "w", **meta) as dst:
            dst.write(masked.astype(meta["dtype"]))
        for i in river_basin_list:
            os.remove(i)


@requires(FloodedAreaGeotiff)
class FloodedAreaGeoJson(Task):
    """
    """

    def output(self):
        return FinalTarget(
            path=f"flooding_in_{self.country_level}_{self.rainfall_scenario}_rainfall.geojson",
            task=self,
            format=luigi.format.Nop,
        )

    def run(self):
        data_dir = self.input().path
        json_data = []
        for i in os.listdir(data_dir):
            logger.info(f"Converting {i} to geojson")
            array, transform = self.resampleRasterfile(os.path.join(data_dir, i))
            masked = array == 1
            start, end = self.get_month_day_range(i)
            temp = [
                {
                    "type": "Feature",
                    "properties": {"flooded": v, "start": start, "end": end},
                    "geometry": s,
                }
                for i, (s, v) in enumerate(shapes(array, masked, 4, transform))
            ]
            json_data.extend(temp)
        out_json = {"type": "FeatureCollection", "features": json_data}
        gdf = gpd.GeoDataFrame.from_features(out_json)

        if gdf.empty:
            try:
                boundary_geo = shape(self.geography["features"][0]["geometry"])
            except KeyError:
                boundary_geo = shape(self.geography)
            gdf = gpd.GeoDataFrame(index=[0], geometry=[boundary_geo])

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        gdf.to_file(self.output().path, driver="GeoJSON")

    def get_month_day_range(self, date_str):
        date = datetime.datetime.strptime(date_str.replace(".tif", ""), "%Y%m")
        first_day = date.replace(day=1).strftime("%Y-%m-%d")
        last_day = date.replace(
            day=calendar.monthrange(date.year, date.month)[1]
        ).strftime("%Y-%m-%d")
        return first_day, last_day

    def resampleRasterfile(self, raster):
        with rasterio.open(raster) as src:
            transform, width, height = calculate_default_transform(
                src_crs=src.crs,
                dst_crs=src.crs,
                width=src.width,
                height=src.height,
                left=src.bounds.left,
                bottom=src.bounds.bottom,
                right=src.bounds.right,
                top=src.bounds.top,
                resolution=(0.008332056698165648, 0.008334204793028323),
            )
            source = src.read(1)
            destination = np.ones(shape=(height, width), dtype=np.float32) * src.nodata
            reproject(
                source=source,
                destination=destination,
                src_transform=src.transform,
                src_crs=src.crs,
                src_nodata=src.nodata,
                dst_transform=transform,
                dst_crs=src.crs,
                dst_nodata=src.nodata,
            )
        return destination, transform


@requires(FloodedAreaGeoJson)
class ConvertFloodedareaGeoJsonToCSV(luigi.Task):
    def output(self):
        return FinalTarget(path="flooded_area.csv", task=self)

    def run(self):
        with open(self.input().path) as data_file:
            data = json.load(data_file)
            list_of_feature_properties = []
            for feature in data["features"]:
                dict_properties = feature["properties"].copy()
                list_of_feature_properties.append(dict_properties)
            df = pd.DataFrame(list_of_feature_properties)
            df.to_csv(self.output().path, index=False)


@requires(ConvertFloodedareaGeoJsonToCSV)
class PushCSVToDatabase_EuropeanRunoffIndex(PushCSVToDatabase):
    pass


@requires(FloodedAreaGeoJson)
class OutputFloodIndexResult(Task):
    """
    Upload flood index result to CKAN
    """

    def output(self):
        name = f"Flooding in {self.country_level} {self.rainfall_scenario} Rainfall.geojson"
        return CkanTarget(
            dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
            resource={"name": name},
        )

    def run(self):
        target = self.output()
        target.put(file_path=self.input().path)
