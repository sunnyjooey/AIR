import datetime
import logging
import os

import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import rasterio
from climate_indices import compute, indices
from dateutil.relativedelta import relativedelta
from luigi import ExternalTask, Task
from luigi.parameter import DateIntervalParameter
from luigi.util import inherits, requires
from rasterio.warp import Resampling, reproject

from kiluigi.targets import CkanTarget, FinalTarget
from models.population_model.tasks import HiResPopRasterMasked
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from .data_pre import (
    MaskAndResamplePotentailEvapotranspiration,
    MaskAndResampleWaterCapacity,
    MaskPrecipitationDataToGeography,
)
from .mappings import age_group_band_map
from .utils import convert_rasters_to_geojson, read_list_of_targets

logger = logging.getLogger("luigi-interface")

RELATIVEPATH = "models/agromet/tasks/"


@inherits(GlobalParameters)
class ShapefileForAggregatingIndices(ExternalTask):
    """
    Pull shapefile for aggregating climate indices
    """

    def output(self):
        if (self.country_level == "Ethiopia") & (self.admin_level == "admin2"):
            return CkanTarget(
                dataset={"id": "d07b30c6-4909-43fa-914b-b3b435bef314"},
                resource={"id": "d4804e8a-5146-48cd-8a36-557f981b073c"},
            )
            # woreda level
        elif (self.country_level == "Ethiopia") & (self.admin_level == "admin3"):
            return CkanTarget(
                dataset={"id": "d07b30c6-4909-43fa-914b-b3b435bef314"},
                resource={"id": "0ac45eb6-eeb8-40ce-b4f0-b21524b19891"},
            )

        elif self.country_level == "South Sudan":
            return CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "0cf2b13b-3c9f-4f4e-8074-93edc01ab1bd"},
            )
        else:
            raise NotImplementedError


@requires(ShapefileForAggregatingIndices)
class AggegatingGeomToS3(Task):
    def output(self):
        return FinalTarget(f"{RELATIVEPATH}agg_geom.geojson", task=self)

    def run(self):
        df = gpd.read_file(f"zip://{self.input().path}")
        with self.output().open("w") as out:
            df.to_file(out.name, driver="GeoJSON")


@inherits(GlobalParameters)
class StandardizedPrecipitationIndex(Task):
    """
    """

    scale = luigi.IntParameter(default=1)
    distribution = luigi.ChoiceParameter(choices=["gamma", "pearson"], default="gamma")
    data_start_date = luigi.DateParameter(default=datetime.date(1982, 1, 1))
    calibration_initial_date = luigi.DateParameter(default=datetime.date(1982, 1, 1))
    calibration_final_date = luigi.DateParameter(default=datetime.date(2016, 12, 31))
    periodicity = luigi.ChoiceParameter(choices=["monthly", "daily"], default="monthly")

    def requires(self):
        date_list = [
            self.data_start_date,
            self.calibration_initial_date,
            self.calibration_final_date,
            self.time.date_a,
            self.time.date_b,
        ]
        self.data_period = pd.date_range(min(date_list), max(date_list), freq="M")
        PrcpClone = self.clone(MaskPrecipitationDataToGeography)
        return {i: PrcpClone.clone(date=i) for i in self.data_period}

    def output(self):
        return FinalTarget(f"{RELATIVEPATH}spi.tif", task=self)

    def run(self):
        if self.distribution == "gamma":
            dist = indices.Distribution.gamma
        elif self.distribution == "pearson":
            dist = indices.Distribution.pearson
        else:
            raise f"Invalid distribution {self.distribution}"

        if self.periodicity == "monthly":
            temporal_res = compute.Periodicity.monthly
        elif self.param_kwargs == "daily":
            temporal_res = compute.Periodicity.daily
        else:
            raise f"Unsupported periodicity argument: {self.periodicity}"

        prcp_array, meta = read_list_of_targets(self.input(), self.data_period)

        spi_array = np.ones(prcp_array.shape) * meta["nodata"]

        row_indices, col_indices = np.where(spi_array[0])
        logger.info("Calculating SPI")
        for row, col in zip(row_indices, col_indices):
            try:
                spi_array[:, row, col] = indices.spi(
                    values=prcp_array[:, row, col],
                    scale=self.scale,
                    distribution=dist,
                    data_start_year=self.data_start_date.year,
                    calibration_year_initial=self.calibration_initial_date.year,
                    calibration_year_final=self.calibration_final_date.year,
                    periodicity=temporal_res,
                )
            except ValueError:
                logger.info(f"SPI for the indices {row, col} could not be calculated")

        time_len = spi_array.shape[0]
        band_map = {
            f"band_{i+1}": (
                self.calibration_initial_date + relativedelta(months=i)
            ).strftime("%Y-%m-%d")
            for i in range(time_len)
        }
        meta.update(count=time_len, compress="ZSTD")
        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write(spi_array.astype(meta["dtype"]))
                dst.update_tags(**band_map)

    @staticmethod
    def name_f_date(date):
        return f"spi/spi_{date.strftime('%Y_%m')}.tif"


@requires(StandardizedPrecipitationIndex, ShapefileForAggregatingIndices)
class ConvertStandardizedPrecipitationIndexToGeojson(Task):
    date_filter = luigi.BoolParameter(default=False)

    def output(self):
        return FinalTarget("spi_v1.csv", task=self)

    def run(self):
        spi_data = self.input()[0].path
        shape_file_src = f"zip://{self.input()[1].path}"
        logger.info("Converting raster to geojson")
        gdf = convert_rasters_to_geojson(
            spi_data,
            shape_file_src,
            self.time.date_a,
            self.time.date_b,
            "mean",
            self.date_filter,
        )
        gdf = gdf.rename(columns={"mean": "spi"})
        df = gdf[["admin0", "admin1", "admin2", "start", "end", "spi"]]
        with self.output().open("w") as out:
            df.to_csv(out.name, index=False)


@inherits(GlobalParameters, StandardizedPrecipitationIndex)
class StandardizedPrecipitationEvapotranspirationIndex(Task):
    """
    """

    def output(self):
        return FinalTarget(f"{RELATIVEPATH}spei.tif", task=self)

    def requires(self):
        date_list = [
            self.data_start_date,
            self.calibration_initial_date,
            self.calibration_final_date,
            self.time.date_a,
            self.time.date_b,
        ]
        self.data_period = pd.date_range(min(date_list), max(date_list), freq="M")
        PrcpClone = self.clone(MaskPrecipitationDataToGeography)
        PetClone = self.clone(MaskAndResamplePotentailEvapotranspiration)
        return {
            "prcp": {i: PrcpClone.clone(date=i) for i in self.data_period},
            "pet": {i: PetClone.clone(date=i) for i in self.data_period},
        }

    def run(self):
        if self.distribution == "gamma":
            dist = indices.Distribution.gamma
        elif self.distribution == "pearson":
            dist = indices.Distribution.pearson
        else:
            raise f"Invalid distribution {self.distribution}"

        if self.periodicity == "monthly":
            temporal_res = compute.Periodicity.monthly
        elif self.param_kwargs == "daily":
            temporal_res = compute.Periodicity.daily
        else:
            raise f"Unsupported periodicity argument: {self.periodicity}"

        data_dict = self.input()

        prcp_array, meta = read_list_of_targets(data_dict["prcp"], self.data_period)
        pet_array, _ = read_list_of_targets(data_dict["pet"], self.data_period)

        spei_array = np.ones(prcp_array.shape) * meta["nodata"]

        row_indices, col_indices = np.where(spei_array[0])

        logger.info("Calculating SPEI")

        for row, col in zip(row_indices, col_indices):
            try:
                spei_array[:, row, col] = indices.spei(
                    precips_mm=prcp_array[:, row, col],
                    pet_mm=pet_array[:, row, col],
                    scale=self.scale,
                    distribution=dist,
                    periodicity=temporal_res,
                    data_start_year=self.data_start_date.year,
                    calibration_year_initial=self.calibration_initial_date.year,
                    calibration_year_final=self.calibration_final_date.year,
                )
            except ValueError:
                logger.info(f"SPEI for the indices {row, col} could not be calculated")

        time_len = spei_array.shape[0]
        band_map = {
            f"band_{i+1}": (
                self.calibration_initial_date + relativedelta(months=i)
            ).strftime("%Y-%m-%d")
            for i in range(time_len)
        }
        meta.update(count=time_len, compress="ZSTD")
        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write(spei_array.astype(meta["dtype"]))
                dst.update_tags(**band_map)


@requires(
    StandardizedPrecipitationEvapotranspirationIndex, ShapefileForAggregatingIndices
)
class StandardizedPrecipitationEvapotranspirationIndexGeojson(Task):
    date_filter = luigi.BoolParameter(default=False)

    def output(self):
        return FinalTarget("spei_v1.csv", task=self)

    def run(self):
        spei_data = self.input()[0].path
        shape_file_src = f"zip://{self.input()[1].path}"
        gdf = convert_rasters_to_geojson(
            spei_data,
            shape_file_src,
            self.time.date_a,
            self.time.date_b,
            "mean",
            self.date_filter,
        )
        gdf = gdf.rename(columns={"mean": "spei"})
        df = gdf[["admin0", "admin1", "admin2", "start", "end", "spei"]]
        with self.output().open("w") as out:
            df.to_csv(out.name, index=False)


@inherits(GlobalParameters, StandardizedPrecipitationIndex)
class PalmerDroughtIndices(Task):
    """
    Compute palmer drought indices
    """

    indices_list = luigi.ListParameter(
        default=["scpdsi", "pdsi", "phdi", "pmdi", "zindex"]
    )
    scale = luigi.IntParameter(default=1)

    def requires(self):
        date_list = [
            self.data_start_date,
            self.calibration_initial_date,
            self.calibration_final_date,
            self.time.date_a,
            self.time.date_b,
        ]
        self.data_period = pd.date_range(min(date_list), max(date_list), freq="M")
        PrcpClone = self.clone(MaskPrecipitationDataToGeography)
        PetClone = self.clone(MaskAndResamplePotentailEvapotranspiration)
        input_map = {
            "prcp": {i: PrcpClone.clone(date=i) for i in self.data_period},
            "pet": {i: PetClone.clone(date=i) for i in self.data_period},
            "awc": self.clone(MaskAndResampleWaterCapacity),
        }
        return input_map

    def output(self):
        out = {
            indice: FinalTarget(f"{RELATIVEPATH}{indice}.tif", task=self)
            for indice in self.indices_list
        }
        return out

    def run(self):
        with self.input()["awc"].open() as src:
            temp = src.read()
            awc_arr, meta = temp["array"], temp["meta"]
        awc_arr[awc_arr == meta["nodata"]] = np.nan

        # Read precipitation data
        precp_arr, meta = read_list_of_targets(self.input()["prcp"], self.data_period)

        # Read evaporation data
        pet_arr, _ = read_list_of_targets(self.input()["pet"], self.data_period)

        if self.scale != 1:
            precp_arr = np.cumsum(precp_arr, axis=0)
            precp_arr[self.scale :, :, :] = (  # noqa: E203
                precp_arr[self.scale :, :, :]  # noqa: E203
                - precp_arr[: -self.scale, :, :]  # noqa: E203
            )
            precp_arr = precp_arr[12:, :, :]  # noqa: E203
            pet_arr = np.cumsum(pet_arr, axis=0)
            pet_arr[self.scale :, :, :] = (  # noqa: E203
                pet_arr[self.scale :, :, :] - pet_arr[: -self.scale, :, :]  # noqa: E203
            )
            pet_arr = pet_arr[12:, :, :]  # noqa: E203
            self.calibration_initial_date = (
                self.calibration_initial_date + relativedelta(years=1)
            )
            self.data_start_date = self.data_start_date + relativedelta(years=1)

        # Convert data from mm to inches
        awc_arr = awc_arr * 0.0393701
        precp_arr = precp_arr * 0.0393701
        pet_arr = pet_arr * 0.0393701

        data = {}
        for i in self.indices_list:
            data[i] = np.ones(precp_arr.shape) * meta["nodata"]

        row_indices, col_indices = np.where(~np.isnan(awc_arr))

        logger.info("Calculating Palmer Indices")

        for row, col in zip(row_indices, col_indices):
            try:
                a, b, c, d, e = indices.scpdsi(
                    precp_arr[:, row, col],
                    pet_arr[:, row, col],
                    awc_arr[row, col],
                    self.data_start_date.year,
                    self.calibration_initial_date.year,
                    self.calibration_final_date.year,
                )
                try:
                    data["scpdsi"][:, row, col] = a
                except KeyError:
                    pass
                try:
                    data["pdsi"][:, row, col] = b
                except KeyError:
                    pass
                try:
                    data["phdi"][:, row, col] = c
                except KeyError:
                    pass
                try:
                    data["pmdi"][:, row, col] = d
                except KeyError:
                    pass
                try:
                    data["zindex"][:, row, col] = e
                except KeyError:
                    pass

            except ValueError:
                logger.info(f"scpdsi for indexes {row}, {col} could not be computed")

        # Clip the data
        for var in data:
            if var == "zindex":
                lower_lim, upper_lim = -2.75, 3.5
            else:
                lower_lim, upper_lim = -4, 4
            data[var] = self.clip_numpy_array(
                data[var], meta["nodata"], lower_lim, upper_lim
            )

        time_len = data[self.indices_list[0]].shape[0]
        band_map = {
            f"band_{i+1}": (
                self.calibration_initial_date + relativedelta(months=i)
            ).strftime("%Y-%m-%d")
            for i in range(time_len)
        }
        meta.update(count=time_len, compress="ZSTD")
        for var in data:
            with self.output()[var].open("w") as out:
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write(data[var].astype(meta["dtype"]))
                    dst.update_tags(**band_map)

    @staticmethod
    def clip_numpy_array(arr, nodata, lower_lim, upper_lim):
        arr[arr != nodata] = np.clip(arr[arr != nodata], lower_lim, upper_lim)
        return arr


@requires(PalmerDroughtIndices, ShapefileForAggregatingIndices)
class ConvertPalmerDroughtIndicesToGeoJSON(Task):
    """
    Convert Palmer drought indices to Geojson
    """

    indices_list = luigi.ListParameter(
        default=["scpdsi", "pdsi", "phdi", "pmdi", "zindex"]
    )
    date_filter = luigi.BoolParameter(default=False)

    def output(self):
        out_target = {
            i: FinalTarget(f"{i}_scale_{self.scale}.csv", task=self)
            for i in self.indices_list
        }
        return out_target

    def run(self):
        data_map = self.input()[0]
        shape_file_src = f"zip://{self.input()[1].path}"
        for var in self.indices_list:
            gdf = convert_rasters_to_geojson(
                data_map[var].path,
                shape_file_src,
                self.time.date_a,
                self.time.date_b,
                ["mean", "median"],
                date_filter=self.date_filter,
            )
            # gdf = gdf.rename(columns={"mean": var})
            # df = gdf[["admin0", "admin1", "admin2", "start", "end", var]]
            cols = [i for i in gdf.columns if i != "geometry"]
            df = gdf[cols]
            with self.output()[var].open("w") as out:
                df.to_csv(out.name, index=False)


@requires(PalmerDroughtIndices)
class IdentifyDrought(Task):
    """
    """

    indices_list = luigi.ListParameter(default=["scpdsi", "pdsi", "phdi", "pmdi"])
    drought_threshold = luigi.FloatParameter(default=-2)

    def output(self):
        targets = {
            i: FinalTarget(f"{i}_drought_{self.scale}.tif", task=self)
            for i in self.indices_list
        }
        return targets

    def run(self):
        data = self.input()
        for i in self.indices_list:
            with rasterio.open(data[i].path) as src:
                arr = src.read()
                meta = src.meta.copy()
                tags = src.tags()

            out_arr = np.where(
                arr == meta["nodata"],
                meta["nodata"],
                np.where(arr <= self.drought_threshold, 1, 0),
            )

            with self.output()[i].open("w") as out:
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write(out_arr.astype(meta["dtype"]))
                    dst.update_tags(**tags)


@requires(IdentifyDrought, ShapefileForAggregatingIndices)
class IdentifyDroughtGeojson(Task):
    date_filter = luigi.BoolParameter(default=False)

    def output(self):
        out = {
            i: FinalTarget(f"{i}_drought_scale_{self.scale}.csv", task=self)
            for i in self.indices_list
        }
        return out

    def run(self):
        data_map = self.input()[0]
        shape_file_src = f"zip://{self.input()[1].path}"
        for var in self.indices_list:
            gdf = convert_rasters_to_geojson(
                data_map[var].path,
                shape_file_src,
                self.time.date_a,
                self.time.date_b,
                ["sum", "count"],
                date_filter=self.date_filter,
            )
            cols = [i for i in gdf.columns if i != "geometry"]
            df = gdf[cols]
            with self.output()[var].open("w") as out:
                df.to_csv(out.name, index=False)


@requires(IdentifyDroughtGeojson, ConvertPalmerDroughtIndicesToGeoJSON)
class MergeIndexAverageAndDrought(Task):
    """
    """

    def output(self):
        out = {
            i: FinalTarget(f"{i}_avg_drought_scale_{self.scale}.csv", task=self)
            for i in self.indices_list
        }
        return out

    def run(self):
        area_src = self.input()[0]
        avg_src = self.input()[1]
        for i in self.indices_list:
            area_df = pd.read_csv(area_src[i].path)
            var = f"{i}_{self.scale}_percent_drought"
            area_df[var] = area_df["sum"] / area_df["count"]
            index_cols = ["OBJECTID_1", "R_CODE", "W_CODE", "Z_CODE", "month"]
            area_df = area_df[index_cols + [var]]
            avg_df = pd.read_csv(avg_src[i].path)
            avg_df = avg_df.merge(area_df, on=index_cols, how="left")
            rename_map = {var: f"{var}_{i}_{self.scale}" for var in ["mean", "median"]}
            avg_df = avg_df.rename(columns=rename_map)
            with self.output()[i].open("w") as out:
                avg_df.to_csv(out.name, index=False)


@inherits(StandardizedPrecipitationIndex)
class NumPeopleAffectedByDrought(Task):
    """
    Estimate number of people affected by drought using SPI
    """

    agromet_index = luigi.ChoiceParameter(
        default="spi",
        choices=["spi", "spei", "scpdsi", "pdsi", "phdi", "pmdi", "zindex"],
    )
    drought_thres = luigi.FloatParameter(default=-2)
    age_group = luigi.ChoiceParameter(
        default="total", choices=list(age_group_band_map.keys())
    )

    def output(self):
        return FinalTarget(f"{RELATIVEPATH}drought_impact.tif", task=self)

    def requires(self):
        if self.agromet_index == "spi":
            index_task = self.clone(StandardizedPrecipitationIndex)
        pop_task = self.clone(HiResPopRasterMasked)
        date_list = [
            self.data_start_date,
            self.calibration_initial_date,
            self.calibration_final_date,
            self.time.date_a,
            self.time.date_b,
        ]
        # TODO: How to get population below 2010
        timepoint = f"2010-01-01-{max(date_list).strftime('%Y-%m-%d')}"
        return [
            index_task,
            pop_task.clone(time=DateIntervalParameter().parse(timepoint)),
        ]

    def run(self):
        spi_src = self.input()[0].path
        pop_dir = self.input()[1].path

        # Read SPI data
        with rasterio.open(spi_src) as src:
            spi_arr = src.read()
            spi_meta = src.meta.copy()
            spi_tags = src.tags()

        spi_arr[np.isnan(spi_arr)] = spi_meta["nodata"]
        pop_src_ls = os.listdir(pop_dir)
        pop_years = [int(i.split("_")[0]) for i in pop_src_ls]
        pop_years = np.array(pop_years)
        # Read one population dataset

        with rasterio.open(os.path.join(pop_dir, pop_src_ls[0])) as src:
            pop_meta = src.meta.copy()

        # Resample spi_arr to match pop data
        spi_dest_arr = np.ones(
            shape=(spi_arr.shape[0], pop_meta["height"], pop_meta["width"])
        )
        spi_dest_arr *= pop_meta["nodata"]
        reproject(
            source=spi_arr,
            destination=spi_dest_arr,
            src_transform=spi_meta["transform"],
            src_crs=spi_meta["crs"],
            src_nodata=spi_meta["nodata"],
            dst_transform=pop_meta["transform"],
            dst_crs=pop_meta["crs"],
            dst_nodata=pop_meta["nodata"],
            resampling=Resampling.nearest,
        )

        out_arr = np.ones(shape=spi_dest_arr.shape) * pop_meta["nodata"]
        for i in range(out_arr.shape[0]):
            spi_year = int(spi_tags[f"band_{i+1}"].split("-")[0])
            close_pop_year = pop_years[np.abs(pop_years - spi_year).argmin()]
            pop_src = [i for i in pop_src_ls if i.startswith(f"{close_pop_year}_")]
            with rasterio.open(os.path.join(pop_dir, pop_src[0])) as src:
                pop_arr = src.read(age_group_band_map[self.age_group])

            out_arr[i] = np.where(
                (
                    (spi_dest_arr[i] == pop_meta["nodata"])
                    | (pop_arr == pop_meta["nodata"])
                ),
                pop_meta["nodata"],
                np.where(spi_dest_arr[i] <= self.drought_thres, pop_arr, 0),
            )

        meta = pop_meta.copy()
        meta.update(count=spi_meta["count"])
        out_arr[out_arr < 0] = meta["nodata"]
        with self.output().open("w") as out:
            with rasterio.open(out, "w", **meta) as dst:
                dst.write(out_arr.astype(meta["dtype"]))
                dst.update_tags(**spi_tags)


@requires(NumPeopleAffectedByDrought, ShapefileForAggregatingIndices)
class NumPeopleAffectedByDroughtGeojson(Task):

    date_filter = luigi.BoolParameter(default=True)

    def output(self):
        return FinalTarget(f"drought_impact_{self.agromet_index}.csv", task=self)

    def run(self):
        src_file = self.input()[0].path
        shape_file_src = f"zip://{self.input()[1].path}"
        gdf = convert_rasters_to_geojson(
            src_file,
            shape_file_src,
            self.time.date_a,
            self.time.date_b,
            "sum",
            self.date_filter,
        )
        gdf = gdf.rename(columns={"sum": "population"})
        df = gdf[["admin0", "admin1", "admin2", "start", "end", "population"]]
        with self.output().open("w") as out:
            df.to_csv(out.name, index=False)
