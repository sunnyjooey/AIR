import datetime
import glob
import json
import logging
import os

import geopandas as gpd
import numpy as np
import pygeoprocessing.routing as routing
import rasterio
from fiona.crs import from_epsg
from luigi import ExternalTask, Task
from luigi.date_interval import Custom as CustomDateInterval
from luigi.parameter import DateIntervalParameter
from luigi.util import requires
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.warp import reproject
from shapely.geometry import box, mapping

from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from models.hydrology_model.flood_index.data_cleaning import ScrapeFLDASData
from models.hydrology_model.utils import map_f_path

logger = logging.getLogger("luigi-interface")


RELATIVEPATH = "models/hydrology_model/flood_index/upstream_runoff_climatology"

DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS = (
    "GTiff",
    (
        "TILED=YES",
        "BIGTIFF=YES",
        "COMPRESS=DEFLATE",
        "BLOCKXSIZE=%d" % (256),
        "BLOCKYSIZE=%d" % (256),
    ),
)


class PullFilesFromCkan(ExternalTask):
    """
    """

    def output(self):
        return {
            "dem": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "befe08dc-7e84-4132-ba56-1be39d25c0d7"},
            ),
            "basin": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "1c3b4411-5190-4091-a5fa-73931ea77412"},
            ),
        }


@requires(PullFilesFromCkan)
class MaskDEMTOBasin(Task):
    """Clip DEMs for various basin
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVEPATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            dem_file = self.input()["dem"].path
            basin_file = self.input()["basin"].path
            df = gpd.read_file(f"zip://{basin_file}")
            df = df.set_index("SUB_BAS")
            with rasterio.open(dem_file) as src:
                meta = src.meta.copy()
                for index in df.index:
                    geo_mask = mapping(df.loc[index, "geometry"])
                    masked, out_transform = mask(src, [geo_mask], crop=True)
                    meta.update(
                        transform=out_transform,
                        height=masked.shape[1],
                        width=masked.shape[2],
                    )
                    dst_file = os.path.join(tmpdir, f"{index}.tif")
                    with rasterio.open(dst_file, "w", **meta) as dst:
                        dst.write(masked.astype(meta["dtype"]))


class ScrapeFLDASDataFrom2001To2019(ScrapeFLDASData):
    """
    Scrape FLDAS data from 2001 to 2019
    """

    flood_index_time = DateIntervalParameter(
        default=CustomDateInterval(
            datetime.date.fromisoformat("2001-01-01"),
            datetime.date.fromisoformat("2019-01-01"),
        )
    )

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVEPATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )


@requires(ScrapeFLDASDataFrom2001To2019)
class ExtractMonthlyRunnoffData(Task):
    """
    Extract surface and subsurface runoff data from NETCDF files
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVEPATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        data_map = map_f_path(self.input().path)
        data_map = {name.split(".")[1][1:]: path for name, path in data_map.items()}
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            for key, path in data_map.items():
                data, transform, xsize, ysize, nodata = self.read_band(path, 8)
                meta = self.get_metadata(transform, xsize, ysize, nodata)
                dst_path = os.path.join(tmpdir, f"surface_runoff_amount_{key}.tif")
                self.write_raster(data, dst_path, meta)
                data, transform, xsize, ysize, nodata = self.read_band(path, 9)
                meta = self.get_metadata(transform, xsize, ysize, nodata)
                dst_path = os.path.join(tmpdir, f"subsurface_runoff_amount_{key}.tif")
                self.write_raster(data, dst_path, meta)
                data, transform, xsize, ysize, nodata = self.read_band(path, 11)
                meta = self.get_metadata(transform, xsize, ysize, nodata)
                dst_path = os.path.join(tmpdir, f"rainfall_flux_{key}.tif")
                self.write_raster(data, dst_path, meta)


@requires(ExtractMonthlyRunnoffData, MaskDEMTOBasin)
class MaskRunoffDataToMatchDEM(Task):
    """
    Clip runoff data for various basins
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVEPATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            data_dir = self.input()[0].path
            dem_dir = self.input()[1].path

            for dem in os.listdir(dem_dir):
                dem_file = os.path.join(dem_dir, dem)
                basin_id = dem.replace(".tif", "")
                with rasterio.open(dem_file) as dem:
                    bounds = dem.bounds
                    meta = dem.meta.copy()

                bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
                gdf = gpd.GeoDataFrame(
                    {"geometry": bbox}, index=[0], crs=from_epsg(4326)
                )
                msk_shape = [json.loads(gdf.to_json())["features"][0]["geometry"]]

                for i in os.listdir(data_dir):
                    if i.endswith(".tif"):
                        src = rasterio.open(os.path.join(data_dir, i))
                        img, transform = mask(src, msk_shape, crop=True)
                        meta.update(
                            transform=transform,
                            height=img.shape[1],
                            width=img.shape[2],
                            dtype="float32",
                        )
                        dst_filename = os.path.join(tmpdir, f"{basin_id}_{i}")
                        with rasterio.open(dst_filename, "w", **meta) as dst:
                            dst.write(np.float32(img))


class UpstreamRunoffMixin:
    """
    Method for calculating upstream runoff
    """

    def resample_raster(self, src_file, dst_file):
        """
        Resample raster files to match DEM
        """
        with rasterio.open(dst_file) as dem_src:
            dst_crs = dem_src.crs
            dst_transform = dem_src.transform
            destination = np.ones(dem_src.shape) * -9999.0

        with rasterio.open(src_file) as rain_src:
            rain_arr = rain_src.read(1)
            src_transform = rain_src.transform
            src_crs = rain_src.crs
            src_nodata = rain_src.nodata

        reproject(
            source=rain_arr,
            destination=destination,
            src_transform=src_transform,
            src_crs=src_crs,
            src_nodata=src_nodata,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            dst_nodata=-9999.0,
        )
        return destination

    def runoffcoefficient(self, surface, subsurface, rain, coeff_dir, meta):
        """
        Get runoff coefficient
        """
        runoff = np.where(
            ((surface == -9999.0) | (subsurface == -9999.0)),
            -9999.0,
            (surface + subsurface),  # * 86400,
        )

        runoff_coef = np.where(
            ((rain == -9999.0) | (runoff == -9999.0)),
            -9999.0,
            np.where(rain == 0.0, 0.0, runoff / rain),
        )
        meta.update(
            dtype="float32", tiled=True, blockxsize=256, blockysize=256, nodata=-9999.0
        )
        with rasterio.open(coeff_dir, "w", **meta) as dst:
            dst.write(runoff_coef.astype(meta["dtype"]), 1)

    def flow_direction(self, dem_file_path, flow_dir_path, flow_method="mfd"):
        """
        Calculate flow direction
        """
        if flow_method == "D8":
            routing.flow_dir_d8(
                (dem_file_path, 1),
                flow_dir_path,
                None,
                DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS,
            )
        elif flow_method == "mfd":
            routing.flow_dir_mfd(
                (dem_file_path, 1),
                flow_dir_path,
                None,
                DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS,
            )
        else:
            raise NotImplementedError

    def num_of_upstream_cells(self, flow_dir_path, num_cells_path, flow_method="mfd"):
        """
        Calculate the number of upstream cells
        """
        if flow_method == "D8":
            routing.flow_accumulation_d8(
                (flow_dir_path, 1),
                num_cells_path,
                None,
                DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS,
            )
        elif flow_method == "mfd":
            routing.flow_accumulation_mfd(
                (flow_dir_path, 1),
                num_cells_path,
                None,
                DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS,
            )
        else:
            raise NotImplementedError

    def flow_accumulation(
        self, flow_dir_path, flow_accum_path, coef_path, flow_method="mfd"
    ):
        """
        Calculate flow accumulation
        """
        if flow_method == "D8":
            routing.flow_accumulation_d8(
                flow_dir_raster_path_band=(flow_dir_path, 1),
                target_flow_accum_raster_path=flow_accum_path,
                weight_raster_path_band=(coef_path, 1),
                raster_driver_creation_tuple=DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS,
            )
        elif flow_method == "mfd":
            routing.flow_accumulation_mfd(
                flow_dir_mfd_raster_path_band=(flow_dir_path, 1),
                target_flow_accum_raster_path=flow_accum_path,
                weight_raster_path_band=(coef_path, 1),
                raster_driver_creation_tuple=DEFAULT_GTIFF_CREATION_TUPLE_OPTIONS,
            )
        else:
            raise NotImplementedError

    def upstream_runoff(self, flow_accum_path, num_cells_path, upstream_runoff_dir):
        """
        """
        with rasterio.open(flow_accum_path) as src:
            accum_arr = src.read(1)
            meta = src.meta.copy()
            accm_nodata = src.nodata
        with rasterio.open(num_cells_path) as src:
            num_cell_arr = src.read(1)
            num_cell_nodata = src.nodata

        arr = np.where(
            (accum_arr == accm_nodata) | (num_cell_arr == num_cell_nodata),
            -9999.0,
            accum_arr / num_cell_arr,
        )
        meta.update(nodata=-9999.0)
        with rasterio.open(upstream_runoff_dir, "w", **meta) as dst:
            dst.write(arr, 1)


@requires(MaskRunoffDataToMatchDEM, MaskDEMTOBasin)
class UpstreamRunoff(Task, UpstreamRunoffMixin):
    """
    Calculate upstream runofff
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVEPATH}/{self.task_id}/", timeout=60 * 60 * 24 * 365
        )

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            runoff_dir = self.input()[0].path
            dem_dir = self.input()[1].path

            for dem in os.listdir(dem_dir):
                dem_file = os.path.join(dem_dir, dem)
                with rasterio.open(dem_file) as src:
                    meta = src.meta.copy()

                basin_id = dem.replace(".tif", "")
                flow_dir_path = os.path.join(tmpdir, f"flow_dir_{basin_id}.tif")
                self.flow_direction(dem_file, flow_dir_path)

                num_cells_path = os.path.join(tmpdir, f"num_cell_{basin_id}.tif")
                self.num_of_upstream_cells(flow_dir_path, num_cells_path)

                rainfall_files = [
                    i
                    for i in os.listdir(runoff_dir)
                    if f"{basin_id}_rainfall_flux" in i
                ]
                for rain_file in rainfall_files:
                    i = rain_file.split("_")[-1]
                    logger.info(
                        f"Calculating upstream runoff for basin {basin_id} month {i} and rainfall {rain_file}"
                    )
                    rain_arr = self.resample_raster(
                        os.path.join(runoff_dir, rain_file), dem_file
                    )
                    surface_arr = self.resample_raster(
                        os.path.join(
                            runoff_dir, f"{basin_id}_surface_runoff_amount_{i}"
                        ),
                        dem_file,
                    )
                    subsurface_arr = self.resample_raster(
                        os.path.join(
                            runoff_dir, f"{basin_id}_subsurface_runoff_amount_{i}"
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


@requires(UpstreamRunoff, MaskDEMTOBasin)
class RunoffClimatology(Task):
    """
    """

    def output(self):
        return IntermediateTarget(
            path=f"{RELATIVEPATH}/{self.task_id}/", timeout=31536000
        )

    def run(self):
        data_dir = self.input()[0].path
        data_dem = self.input()[1].path
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            for dem in os.listdir(data_dem):
                annual_max = []
                for year in range(2001, 2018):
                    start_with = f"{dem.replace('.tif', '')}_{str(year)}"
                    file_list = [
                        i for i in os.listdir(data_dir) if i.startswith(start_with)
                    ]
                    assert len(file_list) == 12, f"The data for {year} is incomplete"
                    for index, i in enumerate(file_list):
                        with rasterio.open(os.path.join(data_dir, i)) as src:
                            arr = src.read(1)
                            nodata = src.nodata
                        arr = np.where(arr == nodata, np.nan, arr)
                        if index == 0:
                            arr_max = arr
                        else:
                            arr_max = np.maximum(arr_max, arr)
                    annual_max.append(arr_max)
                logger.info("Annual max calculated")
                # annual_max = [np.where(i == nodata, np.nan, i) for i in annual_max]
                logger.info("Annual max nodata")
                annual_max_sum = np.ones_like(annual_max[0]) * np.nan
                for ar in annual_max:
                    annual_max_sum = np.nansum([annual_max_sum, ar], axis=0)

                # annual_max_sum = np.nansum(annual_max, axis=0)
                logger.info("Annual sum")

                finite_vals = (np.where(np.isfinite(arr), 1, 0) for arr in annual_max)
                finite_val_count = np.zeros(annual_max[0].shape)
                for arr in finite_vals:
                    finite_val_count += arr
                logger.info("Getting count")
                climatology = annual_max_sum / finite_val_count
                climatology = np.where(np.isfinite(climatology), climatology, nodata)
                # Get meta data
                with rasterio.open(os.path.join(data_dir, i)) as src:
                    meta = src.meta.copy()

                with rasterio.open(os.path.join(tmpdir, dem), "w", **meta) as dst:
                    dst.write(climatology.astype(meta["dtype"]), 1)


@requires(RunoffClimatology)
class MosaicUpstreamRunoffClimatology(Task):
    """
    Merge upstream runoff climatology for various basin
    """

    def output(self):
        return FinalTarget("upstream_runoff_climatology.tif", task=self)

    def run(self):
        data_dir = self.input().path
        file_list = glob.glob(f"{data_dir}*.tif")

        src_list = []

        for i in file_list:
            src_list.append(rasterio.open(i))

        dest, out_transform = merge(src_list)

        meta = src_list[0].meta.copy()

        meta.update(transform=out_transform, height=dest.shape[1], width=dest.shape[2])

        dst_path = self.output().path
        with rasterio.open(dst_path, "w", **meta) as dst:
            dst.write(dest.astype(meta["dtype"]))


@requires(MosaicUpstreamRunoffClimatology)
class UploadUpstreamRunoffClimatologyToCKAN(Task):
    """Upload upstream runoff climatology to ckan
    """

    def output(self):
        target = CkanTarget(
            resource={"id": "526bbeb6-e0b1-4418-aa86-10fc82ea0d78"},
            dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
        )
        if target.exists():
            target.remove()
        return target

    def run(self):
        src_file = self.input().path
        self.output().put(file_path=src_file)
