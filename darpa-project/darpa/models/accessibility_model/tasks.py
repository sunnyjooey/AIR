import calendar
import datetime
import io
import logging
import os
from pathlib import Path

import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import rasterio
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from kiluigi.tasks import ExternalTask, Task
from luigi.util import inherits, requires
from rasterio.io import MemoryFile
from rasterio.mask import mask
from rasterstats import zonal_stats

from models.accessibility_model.data_pre import (
    MaskAndRasterizeShapefiles,
    MaskAndReprojectFlooding,
    MaskAndReprojectRasterFiles,
    PrepareMarketLocationData,
)
from models.accessibility_model.mapping import land_cover_speed_map
from models.accessibility_model.utils import calculate_accesibility_surface
from utils.geospatial_tasks.functions.geospatial import geography_f_country
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters
from utils.visualization_tasks.push_csv_to_database import PushCSVToDatabase

logger = logging.getLogger("luigi-interface")

RELATIVEPATH = "models/accessibility_model/tasks"


@inherits(GlobalParameters)
class FrictionSurface(Task):
    """
    The output is a surface with pixel value representing speed of movement.

    The input surfaces are combined such that the fastest mode of transport took
    precedence.
    """

    date = luigi.DateParameter(default=datetime.date(2017, 1, 1))
    use_impassable = luigi.BoolParameter(default=False)
    input_vars = luigi.ListParameter(
        default=[
            "line_boundary",
            "railway",
            "rivers",
            "roads",
            "landcover",
            "dem",
            "slope",
        ]
    )

    def requires(self):
        Shape = self.clone(MaskAndRasterizeShapefiles)
        Rast = self.clone(MaskAndReprojectRasterFiles)
        Flood = self.clone(MaskAndReprojectFlooding)
        if self.use_impassable:
            input_tasks = [Shape, Rast, Flood]
        else:
            input_tasks = [Shape, Rast]
        return input_tasks

    def output(self):
        path = (
            Path(RELATIVEPATH) / f"friction_surface_{self.date.strftime('%Y_%m')}.tif"
        )
        return IntermediateTarget(path=str(path), task=self, timeout=60 * 60 * 24 * 365)

    def run(self):
        file_dict = self.input()[0]
        file_dict.update(self.input()[1])
        file_dict = {k: v for k, v in file_dict.items() if k in self.input_vars}
        if self.use_impassable:
            file_dict.update(impassable=self.input()[2][self.date.strftime("%Y_%m")])

        # Read the data
        ds, nodata = {}, {}

        for key in file_dict:
            ds[key], nodata[key] = self.read_rasterfile(file_dict[key])

        # Replace land cover classes with speed in km/hr
        for class_id, speed in land_cover_speed_map.items():
            ds["landcover"] = np.where(
                ds["landcover"] == class_id, speed, ds["landcover"]
            )

        # Calculate Elevation adjustmet factor
        ds["landcover"] = self.adjust_landspeed_for_elevation(
            ds["landcover"], nodata["landcover"], ds["dem"], nodata["dem"]
        )

        # Convert slope from degrees to radian
        ds["slope"] = np.where(
            ds["slope"] == nodata["slope"], nodata["slope"], np.radians(ds["slope"]),
        )

        ds["landcover"] = self.adjust_land_speed_for_slope(
            ds["landcover"], nodata["landcover"], ds["slope"], nodata["slope"]
        )

        friction_base = self.select_transport_mode(ds)

        with file_dict["landcover"].open() as src:
            file_byte = src.read()
        with rasterio.open(io.BytesIO(file_byte)) as src:
            meta = src.meta.copy()
        meta.update(count=1)

        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write_band(1, friction_base.astype(meta["dtype"]))

    @staticmethod
    def select_transport_mode(ds):
        if "impassable" in ds:
            roads_adj = np.where(ds["impassable"] == 1, -9999.0, ds["roads"])
        else:
            try:
                roads_adj = ds["roads"].copy()
            except KeyError:
                roads_adj = np.ones(ds["landcover"].shape) * -9999.0
        try:
            friction = np.where(ds["rivers"] == 10, 10, ds["landcover"])
        except KeyError:
            friction = ds["landcover"]
        try:
            friction = np.where(ds["railway"] != -9999.0, ds["railway"], friction)
        except KeyError:
            pass
        friction = np.where(roads_adj != -9999.0, roads_adj, friction)
        # Border crossing- cost
        try:
            friction = np.where(ds["line_boundary"] == 1, 1, friction)
        except KeyError:
            pass
        # Convert km-hr to minute/meter
        friction = np.where(friction == 0, -9999.0, friction)
        friction = np.where(friction == -9999.0, -9999.0, 60 / (friction * 1000))
        assert np.isfinite(friction).all(), "Friction have invalid values"
        return friction

    @staticmethod
    def read_rasterfile(raster_dir):
        with raster_dir.open() as byte_src:
            byte_file = byte_src.read()
        with rasterio.open(io.BytesIO(byte_file)) as src:
            array = np.squeeze(src.read())
            nodata = src.nodata
        return array, nodata

    def adjust_landspeed_for_elevation(
        self, landspeed, landnodata, elevation, eleva_nodata
    ):
        """Adjust land cover speed for elevation

        Parameter:
            landspeed (array): Land cover speed
            landnodata (float): Land cover no data value
            elevation (array): Elevation
            eleva_nodata (float): Elevation no data value

        Returns:
            array: Land cover speed adjusted for elevation

        Notes:
            elevation adjustment factor =  $1.016e^{-0.0001072* elevation}$
        """
        adj = np.where(
            elevation == eleva_nodata,
            eleva_nodata,
            np.multiply(1.016, np.exp(np.multiply(elevation, -0.000_107_2))),
        )

        speed_adj = np.where(
            ((landspeed == landnodata) | (elevation == eleva_nodata)),
            landnodata,
            np.multiply(landspeed, adj),
        )
        return speed_adj

    def adjust_land_speed_for_slope(self, landspeed, landnodata, slope, slopenodata):
        """Adjust land cover speed for slope

        slope ajustment factor = Tobler's walking speed/5'
        Tobler's walking speed = $6e^{-3.5|tan(0.01745*slope angle) + 0.05|}&

        Parameters:
            landspeed (array): Land cover speed
            landnodata (float): Land cover no data value
            slope (array): Slope values
            slopenodata (float): Slope no data value

        Returns:
            array: land cover speed adjusted for slope
        """
        if (slope > 1.6).any():
            raise ValueError(
                "The slope cannot be more than 1.6 radians (91.67 degrees)"
            )

        walking_speed = np.where(
            slope == slopenodata,
            np.nan,
            np.multiply(
                6,
                np.exp(
                    np.multiply(-3.5, abs(np.tan(np.multiply(0.01745, slope)) + 0.05))
                ),
            ),
        )

        slope_adj = np.divide(walking_speed, 5)

        out_array = np.where(
            (landspeed == landnodata) | (slope == slopenodata),
            landnodata,
            np.multiply(landspeed, slope_adj),
        )
        return out_array


@inherits(GlobalParameters)
class TravelTimeToNearestDestination(Task):
    """
    Task for generating  accessibility to towns surface
    """

    destination = luigi.ChoiceParameter(
        choices=["markets", "urban_centers"], default="urban_centers"
    )
    date = luigi.DateParameter(default=datetime.date(2017, 1, 1))
    use_impassable = luigi.BoolParameter(default=False)

    def requires(self):
        if self.destination == "urban_centers":
            Dest = self.clone(MaskAndReprojectRasterFiles)
        else:
            Dest = self.clone(PrepareMarketLocationData)
        Friction = self.clone(FrictionSurface)
        return [Dest, Friction.clone(date=self.date)]

    def output(self):
        country = self.country_level.replace(" ", "_")
        dst = f"accessibility_model/{self.date.strftime('%Y%m')}_travel_time_{country}.tif"
        try:
            out = {"travel_time": FinalTarget(path=dst, task=self, ACL="public-read")}
        except TypeError:
            out = {"travel_time": FinalTarget(path=dst, task=self)}
        if self.destination == "markets":
            gdf = f"accessibility_model/market_ids_{country}.geojson"
            market_grid = f"accessibility_model/market_grid_{country}.tif"
            try:
                out.update(
                    market_id=FinalTarget(path=gdf, task=self, ACL="public-read"),
                    market_grid=FinalTarget(
                        path=market_grid, task=self, ACL="public-read"
                    ),
                )
            except TypeError:
                out.update(
                    market_id=FinalTarget(path=gdf, task=self),
                    market_grid=FinalTarget(path=market_grid, task=self),
                )
        return out

    def mask_array_geography(self, src_arr, src_meta):
        geography = geography_f_country(self.country_level)
        try:
            mask_geom = geography["features"][0]["geometry"]
        except KeyError:
            mask_geom = geography

        with MemoryFile() as memfile:
            with memfile.open(**src_meta) as dst:
                if src_arr.ndim == 2:
                    dst.write(src_arr.astype(src_meta["dtype"]), 1)
                else:
                    dst.write_band(1, src_arr[0].astype(src_meta["dtype"]))
                    dst.write_band(2, src_arr[1].astype(src_meta["dtype"]))

            with memfile.open() as src:
                masked, transform = mask(src, [mask_geom], crop=True)
        h, w = masked.shape[1], masked.shape[2]
        meta = src_meta.copy()
        meta.update(transform=transform, height=h, width=w)
        return np.squeeze(masked), meta

    def run(self):
        time_str = self.date.strftime("%Y-%m-%d")
        try:
            raster_map = self.input()[0]
            with raster_map["towns_rast"].open() as src:
                byte_src = src.read()
            with rasterio.open(io.BytesIO(byte_src)) as src:
                destination = src.read(1)
        except (TypeError, KeyError):
            with self.input()[0].open() as src:
                destination = src.read()

        with self.input()[1].open() as src:
            src_byte = src.read()
        with rasterio.open(io.BytesIO(src_byte)) as src:
            meta = src.meta.copy()
            if self.destination == "urban_centers":
                travel_time = calculate_accesibility_surface(src, destination)
            elif self.destination == "markets":
                travel_time, gdf = calculate_accesibility_surface(
                    src, destination, nearest_dest=True
                )
                meta.update(count=2)
            else:
                raise NotImplementedError

        travel_time, meta = self.mask_array_geography(travel_time, meta)
        meta.update(count=1)
        with self.output()["travel_time"].open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                if self.destination == "urban_centers":
                    dst.write(travel_time.astype(meta["dtype"]), 1)
                    dst.update_tags(
                        Time=time_str, rainfall_scenario=self.rainfall_scenario
                    )
                else:
                    dst.write_band(1, travel_time[0].astype(meta["dtype"]))
                    dst.update_tags(
                        Time=time_str, rainfall_scenario=self.rainfall_scenario
                    )
        if self.destination == "markets":
            gdf = gdf.drop("indicies", axis=1)
            with self.output()["market_id"].open("w") as f:
                gdf.to_file(f.name, driver="GeoJSON")
            with self.output()["market_grid"].open("w") as out:
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write_band(1, travel_time[1].astype(meta["dtype"]))


@inherits(TravelTimeToNearestDestination)
class TravelTimeGeotiff(Task):
    """Place holder Task to ensure economic model do not break

    This task just pull data from S3 and save it in local directory
    """

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=31536000)

    def requires(self):
        InputTask = self.clone(TravelTimeToNearestDestination)
        if (self.destination == "markets") | (self.use_impassable is False):
            return InputTask
        else:
            month_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
            return [InputTask.clone(date=i) for i in month_list]

    def overwrite_input(self):
        inputs = self.input()
        if isinstance(inputs, list):
            out = [i["travel_time"] for i in inputs]
        else:
            out = [inputs["travel_time"]]
        return out

    @staticmethod
    def date_f_filename(filename):
        name = os.path.basename(filename)
        month = name.split("_")[0]
        date = datetime.datetime.strptime(month, "%Y%m")
        return date.strftime("%Y-%m-%d")

    def run(self):
        data_list = self.overwrite_input()

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            for i in data_list:
                time_str = self.date_f_filename(i.path)
                with i.open() as src:
                    src_byte = src.read()
                with rasterio.open(io.BytesIO(src_byte)) as src:
                    arr = src.read(1)
                    meta = src.meta.copy()
                    dst = os.path.join(tmpdir, os.path.basename(i.path))
                    with rasterio.open(dst, "w", **meta) as dst:
                        dst.write(arr.astype(meta["dtype"]), 1)
                        dst.update_tags(
                            Time=time_str, rainfall_scenario=self.rainfall_scenario
                        )


class AdminUnitsFromCkan(ExternalTask):

    """
    Task for pulling state shapefile from CKAN
    """

    def output(self):
        return {
            "South Sudan": CkanTarget(
                dataset={"id": "b8b945d6-3529-4b61-a06f-0d2d5a1f29d9"},
                resource={"id": "e2392374-d60b-432d-bd9a-dda4acfc009d"},
            ),
            "Ethiopia": CkanTarget(
                dataset={"id": "b8b945d6-3529-4b61-a06f-0d2d5a1f29d9"},
                resource={"id": "cb0bf62a-d0e9-48cb-8e28-8116f69dc076"},
            ),
            "Djibouti": CkanTarget(
                dataset={"id": "b8b945d6-3529-4b61-a06f-0d2d5a1f29d9"},
                resource={"id": "5990c406-6d44-4458-b64f-cff21c82c0e6"},
            ),
            "Kenya": CkanTarget(
                dataset={"id": "b8b945d6-3529-4b61-a06f-0d2d5a1f29d9"},
                resource={"id": "b368d3a0-ad63-4494-8d1c-1598b73ebd71"},
            ),
            "Sudan": CkanTarget(
                dataset={"id": "b8b945d6-3529-4b61-a06f-0d2d5a1f29d9"},
                resource={"id": "eb47f65c-b044-4091-8c49-6370e75c812c"},
            ),
            "Uganda": CkanTarget(
                dataset={"id": "b8b945d6-3529-4b61-a06f-0d2d5a1f29d9"},
                resource={"id": "16b233b6-b9ad-4d85-b958-abaa1bf26062"},
            ),
            "Somalia": CkanTarget(
                dataset={"id": "b8b945d6-3529-4b61-a06f-0d2d5a1f29d9"},
                resource={"id": "28fd1c6b-680a-44b6-8ea2-665f010375cc"},
            ),
            "Eritrea": CkanTarget(
                dataset={"id": "b8b945d6-3529-4b61-a06f-0d2d5a1f29d9"},
                resource={"id": "f23f54ad-30d4-4cfa-9186-8c98d312f795"},
            ),
        }


@inherits(TravelTimeToNearestDestination, GlobalParameters)
class TravelTimeGeoJSON(Task):
    """
    Average travel time at admin level 2
    """

    def requires(self):
        InputTask = self.clone(TravelTimeToNearestDestination)
        if self.use_impassable is False:
            return {"travel": InputTask, "admin": AdminUnitsFromCkan()}
        else:
            month_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
            return {
                "travel": [InputTask.clone(date=i) for i in month_list],
                "admin": AdminUnitsFromCkan(),
            }

    def output(self):
        country = self.country_level.replace(" ", "_")
        file_path = f"accessibility_model/average_travel_time_{country}_{self.destination}_{self.rainfall_scenario}_rainfall.geojson"
        try:
            return FinalTarget(file_path, task=self, ACL="public-read")
        except TypeError:
            return FinalTarget(file_path, task=self)

    def run(self):
        src_rasters = self.input()["travel"]
        if isinstance(src_rasters, dict):
            src_rasters = [src_rasters]
        admin_zip = self.input()["admin"][self.country_level].path
        admin_shp = gpd.read_file(f"zip://{admin_zip}")
        rename_map = {
            "NAME_0": "admin0",
            "NAME_1": "admin1",
            "NAME_2": "admin2",
            "NAME_3": "admin3",
        }
        admin_shp = admin_shp.rename(columns=rename_map)
        try:
            admin_shp = admin_shp[["admin0", "admin1", "admin2", "admin3", "geometry"]]
        except KeyError:
            admin_shp = admin_shp[["admin0", "admin1", "admin2", "geometry"]]

        src_rasters = [i["travel_time"] for i in src_rasters]
        for index, raster_file in enumerate(src_rasters):
            temp = admin_shp.copy()
            with raster_file.open() as src:
                byte_src = src.read()
            with rasterio.open(io.BytesIO(byte_src)) as src:
                arr = src.read(1)
                transform = src.transform
                nodata = src.nodata
                stats = zonal_stats(
                    temp,
                    arr,
                    affine=transform,
                    nodata=nodata,
                    geojson_out=True,
                    stats="mean",
                    all_touched=True,
                )
                temp = gpd.GeoDataFrame.from_features(stats)
                temp = temp.rename(columns={"mean": "travel_time"})

            first_day, last_day = self.get_month_day_range(raster_file.path)
            temp["start"] = first_day
            temp["end"] = last_day
            if index == 0:
                out_df = temp.copy()
            else:
                out_df = pd.concat([out_df, temp]).reset_index(drop=True)

        # exploded = out_df.explode().reset_index().rename(columns={0: "geometry"})
        # exploded = exploded[["level_0", "level_1", "geometry"]].copy()
        # merged = exploded.merge(
        #     out_df.drop("geometry", axis=1), left_on="level_0", right_index=True
        # )
        # merged = merged.set_index(["level_0", "level_1"]).set_geometry("geometry")
        # merged["destination_scenario"] = self.destination
        # merged["lat"] = merged.centroid.y
        # merged["lon"] = merged.centroid.x
        # try:
        #     merged["flood_threshold_scenario"] = self.return_period_threshold
        # except AttributeError:
        #     pass
        # merged["rainfall_scenario"] = self.rainfall_scenario
        with self.output().open("w") as out:
            out_df.to_file(out.name, driver="GeoJSON", index=False)

    def calculate_mask_statistic(self, rast, area):
        # Mask raster based on buffered shape
        out_img, out_transform = mask(rast, [area], crop=True)
        no_data_val = rast.nodata

        out_data = out_img[0]

        # Remove grid with no data values
        clipd_img = out_data[out_data != no_data_val]
        clipd_img = clipd_img[~np.isnan(clipd_img)]

        # Calculate stats on masked array
        return np.ma.mean(clipd_img)

    def get_month_day_range(self, date_str):
        date_str = os.path.basename(date_str)
        date_str = date_str.split("_")[0]
        date = datetime.datetime.strptime(date_str.replace(".tif", ""), "%Y%m")
        first_day = date.replace(day=1).strftime("%Y-%m-%d")
        last_day = date.replace(
            day=calendar.monthrange(date.year, date.month)[1]
        ).strftime("%Y-%m-%d")
        return first_day, last_day


@requires(TravelTimeGeoJSON)
class OutputTravelTime(Task):
    """
    Upload travel time to geojson to ckan
    """

    def run(self):
        output_fname = f"Average Travel Time in {self.country_level} {self.rainfall_scenario} Rainfall"
        output_target = CkanTarget(
            dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
            resource={"name": output_fname},
        )
        if output_target.exists():
            output_target.remove()
        output_target.put(self.input().path)


@requires(TravelTimeGeoJSON)
class TravelTimeToTownsCSV(luigi.Task):
    def output(self):
        dst = f"accessibility_model/avg_travel_time_{self.country_level.replace(' ','_')}.csv"
        return FinalTarget(path=dst, task=self)

    def run(self):
        df = gpd.read_file(self.input().path)
        df = df.drop("geometry", axis=1)
        df = pd.DataFrame(df)
        with self.output().open("w") as out:
            df.to_csv(out.name, index=False)


@requires(TravelTimeToTownsCSV)
class PushCSVToDatabase_TravelTimeToTowns(PushCSVToDatabase):
    pass
