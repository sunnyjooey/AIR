import copy
import datetime
import logging
import os
import shlex
import shutil
import subprocess  # noqa: S404
import tarfile

import fiona
import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import rasterio
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from kiluigi.tasks import ExternalTask, Task
from luigi.configuration import get_config
from luigi.util import requires
from rasterio.mask import mask
from shapely.geometry import mapping

from models.crop_model.config import load_config
from models.crop_model.mapping import crop_config
from models.crop_model.peerless import execute
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

logger = logging.getLogger("luigi-interface")

CONFIG = get_config()


class CropModelParameters(GlobalParameters):
    crop = luigi.ChoiceParameter(choices=["maize", "sorghum"], default="maize")
    sample = luigi.IntParameter(default=-1)
    erain = luigi.ChoiceParameter(
        var_type=float,
        choices=[0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0],
        default=1.0,
    )
    management = luigi.ChoiceParameter(
        default="rf_lowN", choices=["rf_lowN", "rf_highN", "rf_0N", "irrig"]
    )
    start_year = luigi.IntParameter(default=2010)
    num_years = luigi.IntParameter(11)

    def complete(self):
        return True


class PullDataForRunningDSSAT(ExternalTask):
    """
    Pull data for running DSSAT from CKAN
    """

    def output(self):

        return {
            "South Sudan": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "97c3d7f2-dc4d-4471-b3f2-ccfa3e2d9697"},
            ),
            "Ethiopia": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "0b08d803-38c0-4f76-84f3-01ed793f2236"},
            ),
            "Kenya": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "8308b9b6-8561-49fc-a4b8-64b320b354e3"},
            ),
            "Uganda": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "593e9326-fa58-4289-9204-aed82e2ef4eb"},
            ),
            "Somalia": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "d0a351d0-b30d-4604-9862-899c62e287f7"},
            ),
            "Djibouti": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "ec17569c-3396-4fd5-a61f-f03f427db285"},
            ),
            "Eritrea": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "f2c00ab6-607f-4ddf-908d-180d47dd41b4"},
            ),
            "Sudan": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "54c4003f-715d-4806-b9db-b733b26972a9"},
            ),
        }


class PullSorghumDataFromCkanForETH(ExternalTask):
    """
    Pull crop specific harvested area data-set from Ckan
    """

    def output(self):
        return {
            "harvested_area": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "fde1737c-e86f-443f-959a-bfb44b28e3ec"},
            ),
            "templates": CkanTarget(
                dataset={"id": "797a5578-79dd-4174-a2ff-f0886bbb6cae"},
                resource={"id": "ff28fc7a-5530-4fb3-95db-b45e07e70eee"},
            ),
        }


@requires(PullDataForRunningDSSAT, CropModelParameters)
class UnZipDSSATData(Task):
    """Unzip data pulled from S3."""

    def output(self):
        # we don't pass a `task` instance to this IntermediateTarget, because it shouldn't use
        # different outputs for a different set of taks parameters. The input files will not
        # change frequently, if at all
        return IntermediateTarget(
            path=f"crop_model/crop_model_data/{self.country_level}_{self.task_id}/",
            timeout=60 * 60 * 24 * 7,
        )

    def run(self):
        with self.output().temporary_path() as tmpdir:
            zipfile_path = self.input()[0][self.country_level].path
            # Unzip the files to the Target directory, via a temporary_path()
            with tarfile.open(zipfile_path) as f:
                f.extractall(tmpdir)


@requires(CropModelParameters)
class GenerateDSSATConfigFile(Task):
    """
    Create config file for running DSSAT
    """

    def output(self):
        # no timeout here, config will not change for different Crop Model Data, only for
        # different parameters which `task=self` takes care of
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 365)

    def run(self):
        # General config
        dssat_config = copy.deepcopy(crop_config[self.crop][self.country_level])
        dssat_config.update(sample=self.sample, erain=self.erain)
        dssat_config["default_setup"]["erain"] = self.erain
        runs = dssat_config["runs"]
        runs_m = []
        for i in runs:
            if self.management in i["name"]:
                runs_m.append(i)
        dssat_config["runs"] = runs_m
        dssat_config["default_setup"]["nyers"] = self.num_years
        dssat_config["default_setup"]["sdate"] = datetime.date(
            self.start_year, 1, 1
        ).strftime("%Y-%m-%d")
        dssat_config["default_setup"]["pfrst"] = datetime.date(
            self.start_year, 3, 1
        ).strftime("%Y-%m-%d")
        dssat_config["default_setup"]["plast"] = datetime.date(
            self.start_year, 5, 23
        ).strftime("%Y-%m-%d")
        with self.output().open("w") as out:
            out.write(dssat_config)


# we require UnZipDSSATData here, so we can use it in self.input()
# otherwise we wouldn't have to as it's a dependency of GenerateDSSATConfigFile
@requires(GenerateDSSATConfigFile, UnZipDSSATData)
class GenerateDSSATInputFiles(Task):
    """Run `execute` function from `peerless.py` script to generate DSSAT inputs files.

    The `execute` function creates two level of subdirectories where the name
    of the first level subdirectory is latitude and the second level is longitude.
    should have three files, weather file, soil file and template file
    """

    def output(self):
        # timeout: for the remote chance that the Crop Model Data changes on CKAN;
        # task: because the config is parameter specific so we need to regenerate
        # on parameter change
        return IntermediateTarget(
            path="crop_model/dssat_input_files/", task=self, timeout=60 * 60 * 24 * 7
        )

    def run(self):
        with self.input()[0].open() as src:
            config = src.read()

        with self.output().temporary_path() as tmpdir:
            # we need a absolute path to write it to config
            tmpdir = os.path.abspath(tmpdir)
            os.makedirs(tmpdir, exist_ok=True)

            # we have to remember the path where we were, otherwise the IntermediateTarget
            # might write to a wrong location when we exit the with block
            old_pwd = os.getcwd()
            # we run the execute peerless from the path that contains all the input
            # files (downloaded from world modeler s3 and unzipped).
            os.chdir(os.path.join(self.input()[1].path))

            # WorkDir with tmpdir, so our output ends up
            # in the right location
            config["workDir"] = f"{tmpdir}"

            # Merge run list with default parameters
            config = load_config(config, validate=False, merge=True)
            logger.info("Preparing files for running DSSAT")
            execute(config)
            os.chdir(old_pwd)


@requires(GenerateDSSATInputFiles)
class GetDSSATRunList(Task):
    """
    Get the directory to run DSSAT and the template to be used
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=60 * 60 * 24 * 7)

    def run(self):
        data_dir = self.input().path
        run_list = []
        for root, _, files in os.walk(data_dir, topdown=False):
            for name in files:
                if name.upper().endswith("X"):
                    run_list.append({"dir": root, "file": name})

        with self.output().open("w") as out:
            out.write(run_list)


@requires(GetDSSATRunList)
class RunDSSAT(Task):
    """Run dscsm047 binary on input files generated by peerless.py

    We are only interested in `summary.csv` files that the dscsm047 binary produces for each
    directory, so we will only copy this into the output directory for this Task.
    We will change the output of GenerateDSSATInputFiles by runnnig the dscsm047 binary
    inside that folder, but for the sake of clarity and the output based Luigi model we will
    store the resulting csv files in a separate IntermediateTarget
    """

    def output(self):
        return IntermediateTarget(
            path="crop_model/dssat_output_files/", task=self, timeout=60 * 60 * 24 * 7
        )

    def run(self):
        with self.input().open() as src:
            run_list = src.read()
        with self.output().temporary_path() as tmpdir:
            tmpdir = os.path.abspath(tmpdir)
            os.makedirs(tmpdir, exist_ok=True)

            # we have to remember the path where we were, otherwise the IntermediateTarget
            # might write to a wrong location when we exit the with block
            old_pwd = os.getcwd()

            for details in run_list:
                try:
                    logger.info(
                        "Running dscsm047 binary with %s in %s",
                        details["file"],
                        details["dir"],
                    )
                    # we change to the path that contains all the input files, to run the binary from there
                    os.chdir(details["dir"])
                    subprocess.check_call(  # noqa: S603
                        shlex.split(f"/app/dssat47/dscsm047 A {details['file']}")
                    )
                    cell = "/".join(details["dir"].split("/")[-3:])
                    output_dir = os.path.join(tmpdir, cell)
                    os.makedirs(output_dir)
                    # this is the newly created file that we are interested in for aggregation
                    # we have to use a absolute path here for destination or it will end up
                    # in a subfolder of the current directory
                    shutil.move("summary.csv", output_dir)
                    os.chdir(old_pwd)
                except NotADirectoryError:
                    pass
            # change back so the output doesn't end up a subfolder of something else
            os.chdir(old_pwd)


@requires(
    RunDSSAT,
    GenerateDSSATConfigFile,
    UnZipDSSATData,
    CropModelParameters,
    GenerateDSSATConfigFile,
)
class CollateDSSATRunOutput(Task):
    """
    Calculate total production by multiplying yield per ha with harvested area
    """

    def output(self):
        # Get the different crop management practices
        runs = crop_config[self.crop][self.country_level]["runs"]
        runs = [i for i in runs if self.management in i["name"]]
        return {
            i["name"]: FinalTarget(
                path=f"{i['name']}_{self.country_level}.csv", task=self
            )
            for i in runs
        }

    def run(self):
        dssat_output_dir = self.input()[0].path
        config = self.input()[1].open().read()
        raster_dir = self.input()[2].path

        for run in config.get("runs"):
            name = run["name"]
            df = self.collate_outputs(
                base_dir=os.path.join(dssat_output_dir, name),
                harv_raster=os.path.join(raster_dir, run["harvestArea"].split("::")[1]),
                name=name,
            )
            with self.output()[name].open("w") as out:
                df.to_csv(out.name)

    def collate_outputs(self, base_dir, harv_raster, name):
        df = pd.DataFrame()
        for pixel_dir in self.get_summary_dir(base_dir, "summary.csv"):
            pixel_df = pd.read_csv(os.path.join(pixel_dir, "summary.csv"))
            pixel_df["id"] = 1
            lat, lng = self.extract_coordinates(pixel_dir)
            with rasterio.open(harv_raster) as ds:
                band = ds.read(1)
                harea = self.get_raster_value(ds, band, lng, lat)
            temp = pd.DataFrame(
                [{"LATITUDE": lat, "LONGITUDE": lng, "HARVEST_AREA": harea, "id": 1}]
            )
            temp["RUN_NAME"] = name
            pixel_df = pd.merge(temp, pixel_df, on="id", how="right")
            pixel_df = pixel_df.drop("id", axis=1)
            if df.shape[0] == 0:
                df = pixel_df.copy()
            else:
                df = pd.concat([df, pixel_df])
                df = df.reset_index(drop=True)

        df["HWAH"] = df["HARVEST_AREA"] * df["HWAM"]
        return df

    def get_raster_value(self, ds, band, lng, lat):
        row, col = ds.index(lng, lat)
        value = band[row, col]
        return value

    def get_summary_dir(self, path, target_file):
        for root, _subdir, files in os.walk(path, topdown=False):
            if target_file in files:
                yield root

    def extract_coordinates(self, path):
        str_coordinates = path.split(os.path.sep)[-2:]
        lat, lng = [np.float(i.replace("_", ".")[:-1]) for i in str_coordinates]
        if str_coordinates[0][-1] == "S":
            lat = -lat
        if str_coordinates[0][-1] == "W":
            lng = -lng

        return lat, lng


@requires(CollateDSSATRunOutput)
class DSSATOutput(Task):
    def output(self):
        dst = f"crop_model/{self.crop}_{self.country_level}_production.csv"
        return FinalTarget(path=dst, task=self)

    def run(self):
        data = {k: pd.read_csv(v.path) for k, v in self.input().items()}
        df = pd.concat([v for _, v in data.items()])
        df["year"] = df["SDAT"].apply(lambda x: int(str(x)[:-3]))
        with self.output().open("w") as out:
            df.to_csv(out.name, index=False)


@requires(
    CollateDSSATRunOutput, UnZipDSSATData, CropModelParameters, GenerateDSSATConfigFile
)
class HarvestedYieldGeotiffs(Task):
    """
    Generate geotiff for the crop yield from csv files
    """

    def output(self):
        # Get the crop management practices
        # Get the different crop management practices
        runs = crop_config[self.crop][self.country_level]["runs"]
        runs = [i for i in runs if self.management in i["name"]]
        return {
            i["name"]: FinalTarget(f"{i['name']}_{self.country_level}.tif", task=self)
            for i in runs
        }

    def run(self):
        data_dir = self.input()[0]
        raster_dir = self.input()[1].path

        with self.input()[3].open() as src:
            dssat_config = src.read()
        runs = dssat_config["runs"]
        for run in runs:
            run_name = run["name"]
            harv_raster = os.path.join(raster_dir, run["harvestArea"].split("::")[1])
            with rasterio.open(harv_raster) as src:
                df = pd.read_csv(data_dir[run_name].path)
                df["year"] = df["SDAT"].apply(lambda x: int(str(x)[:-3]))
                years_list = list(set(df["year"]))
                meta = src.meta.copy()
                meta.update(nodata=-9999.0, count=len(years_list))
                with self.output()[run_name].open("w") as out:
                    with rasterio.open(out, "w", **meta) as dst:
                        for index, year in enumerate(years_list, 1):
                            if year != 9999:
                                logger.info(f"Year {index} of {len(years_list)}")
                                out_array = np.ones(src.shape) * -9999.0
                                temp = df.loc[df["year"] == year].copy()
                                for lat, lng in zip(df["LATITUDE"], df["LONGITUDE"]):
                                    row, col = src.index(lng, lat)
                                    try:
                                        out_array[row, col] = temp.loc[
                                            (temp["LATITUDE"] == lat)
                                            & (temp["LONGITUDE"] == lng),
                                            "HWAH",
                                        ]
                                    except ValueError:
                                        pass
                                dst.write(np.float32(out_array), index)
                                dst.update_tags(**{f"band_{index}": year})


class PullAdminShapefile(ExternalTask):
    """
    Pull admin shapefile from Ckan
    """

    def output(self):
        return {
            "South Sudan": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "edf6d3a8-e237-4473-8c3a-f11e20957532"},
            ),
            "Ethiopia": CkanTarget(
                dataset={"id": "d07b30c6-4909-43fa-914b-b3b435bef314"},
                resource={"id": "d4804e8a-5146-48cd-8a36-557f981b073c"},
            ),
            "Kenya": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "ea6d5137-ec71-4b7c-b72c-4aba04f39ece"},
            ),
            "Uganda": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "dad789ef-5231-492d-85b2-0a2244ba1cfb"},
            ),
            "Somalia": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "e999a8da-4352-4784-a8dd-e5dfcf31a9c9"},
            ),
            "Djibouti": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "1cea7203-f6fd-4912-9520-b14ad917638b"},
            ),
            "Eritrea": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "e755ae40-6702-4c11-b12a-bb1ae72bcb43"},
            ),
            "Sudan": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "1fc3f4eb-013e-4b5f-8a2c-e7614ffaa6b2"},
            ),
        }


@requires(PullAdminShapefile, HarvestedYieldGeotiffs, CropModelParameters)
class CropProductionAtAdmin2GeoJson(Task):
    """
    Convert crop production raster to geojson
    """

    def output(self):
        return FinalTarget(
            f"{self.crop}_poduction_at_admin2_{self.country_level}.geojson", task=self
        )

    def run(self):
        admin_zip = self.input()[0][self.country_level].path
        gdf = gpd.read_file(f"zip://{admin_zip}")

        rename_map = {"NAME_0": "admin0", "NAME_1": "admin1", "NAME_2": "admin2"}

        gdf = gdf.rename(columns=rename_map)
        if "admin0" not in gdf.columns:
            gdf["admin0"] = self.country_level
        index_admins = ["admin0", "admin1", "admin2"]
        gdf["index"] = gdf[index_admins].apply(
            lambda x: "::".join(x.dropna().astype(str)), axis=1
        )
        gdf = gdf.set_index("index")
        data = gdf[index_admins].copy()
        data["id"] = 1
        years_df = pd.DataFrame({"date": [1984 + i for i in range(0, 35)]})
        years_df["id"] = 1
        years_df["date"] = years_df["date"].astype(int)
        years_df["start"] = years_df["date"].apply(lambda year: f"{year}-01-01")
        years_df["end"] = years_df["date"].apply(lambda year: f"{year}-12-31")
        data = pd.merge(data, years_df, on="id", how="outer")
        data["index"] = data[index_admins].apply(
            lambda x: "::".join(x.dropna().astype(str)), axis=1
        )
        data = data.drop(index_admins + ["id", "date"], axis=1)
        raster_map = self.input()[1]
        for key, input_target in raster_map.items():
            logger.info(f"Converting {key} to geojson")
            with rasterio.open(input_target.path) as src:
                meta = src.meta.copy()

                data[key] = np.nan
                for index in gdf.index:
                    geo_mask = mapping(gdf.loc[index, "geometry"])

                    masked, transform = mask(src, [geo_mask], crop=True)
                    masked[masked == meta["nodata"]] = np.nan
                    for band, year in src.tags().items():
                        if band.startswith("band_"):
                            i = np.int(band.split("_")[-1]) - 1
                            start_date = f"{year}-01-01"
                            end_date = f"{year}-12-31"
                            crop_production = np.nansum(masked[i])
                            data.loc[
                                (data["index"] == index)
                                & (data["start"] == start_date)
                                & (data["end"] == end_date),
                                key,
                            ] = crop_production
        gdf = gdf.reset_index()
        out_df = pd.merge(gdf, data, on="index", how="outer")
        out_df = out_df.drop("index", axis=1)
        os.makedirs(os.path.basename(self.output().path), exist_ok=True)
        with self.output().open("w") as out:
            try:
                out_df.to_file(out.name, driver="GeoJSON")
            except fiona.errors.GeometryTypeValidationError:
                exploded = (
                    out_df.explode().reset_index().rename(columns={0: "geometry"})
                )
                merged = exploded.merge(
                    out_df.drop("geometry", axis=1), left_on="level_0", right_index=True
                )
                merged = merged.set_index(["level_0", "level_1"]).set_geometry(
                    "geometry"
                )

                merged.to_file(out.name, driver="GeoJSON")


@requires(CollateDSSATRunOutput, PullAdminShapefile)
class CropProductionCSV(Task):
    def output(self):
        return FinalTarget(
            f"{self.crop}_poduction_at_admin2_{self.country_level}.csv", task=self
        )

    def run(self):
        df = pd.read_csv(list(self.input()[0].values())[0].path)
        df["year"] = df["SDAT"].apply(lambda x: int(str(x)[:-3]))
        df = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df["LONGITUDE"], df["LATITUDE"])
        )
        df = df.set_crs("epsg:4326")
        df_admin = gpd.read_file(f"zip://{self.input()[1][self.country_level].path}")
        df = gpd.sjoin(df, df_admin, how="left")
        rename_map = {"NAME_0": "admin0", "NAME_1": "admin1", "NAME_2": "admin2"}

        df = df.rename(columns=rename_map)

        df = df.groupby(["admin0", "admin1", "admin2", "year"], as_index=False)[
            "HWAH"
        ].sum()
        df["year"] = df["year"].astype(int)
        df["start"] = df["year"].apply(lambda year: f"{year}-01-01")
        df["end"] = df["year"].apply(lambda year: f"{year}-12-31")
        df = df.rename(columns={"HWAH": "crop_production"})
        with self.output().open("w") as out:
            df.to_csv(out.name)


@requires(CropProductionAtAdmin2GeoJson, CropModelParameters)
class UploadCropProductionAtAdmin2ToCKAN(Task):
    """
    Upload crop production data at admin 2 to CKAN
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "3cd58ca0-3c06-4059-915a-b0321b593eba"},
            resource={
                "name": f"{self.crop} production at admin2 in {self.country_level}"
            },
        )

    def run(self):
        file_path = self.input()[0].path
        self.output().put(file_path)
