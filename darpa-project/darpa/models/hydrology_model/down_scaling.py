import os
import shutil
import zipfile

import fiona
import luigi
import numpy as np
import pandas as pd
import pyspatialml
import rasterio
import rasterio.mask
import requests
from dateutil import parser
from keras.callbacks import ModelCheckpoint
from keras.models import load_model
from luigi import ExternalTask, Task
from luigi.configuration import get_config
from luigi.util import requires
from sklearn.model_selection import train_test_split

from kiluigi.targets import CkanTarget, IntermediateTarget, RESTTarget
from models.hydrology_model.utils import (
    extract_zipfile,
    generate_earthdata_token,
    get_url_paths,
    map_f_path,
    mask_raster,
    mlp_model,
    reproject_like,
    resample_raster,
    weighted_average,
)

CONFIG = get_config()

CACHE_DIR = CONFIG.get("core", "cache_dir")


class CkanFile(ExternalTask):

    """
    Get shapefile from ckan
    """

    def output(self):
        return {
            "boundary": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "c8bf5ca8-af58-44d7-a25a-66ad7786b516"},
            ),
            "ss_boundary": CkanTarget(
                dataset={"id": "2c6b5b5a-97ea-4bd2-9f4b-25919463f62a"},
                resource={"id": "4ca5e4e8-2f80-44c4-aac4-379116ffd1d9"},
            ),
        }


class NSIDCTarget(RESTTarget):
    """
    Task for downloading SMAP soil moisture data
    """

    url = "https://n5eil01u.ecs.nsidc.org/egi/request?"
    bbox = "23.42, 3.45, 36.1, 12.41"
    username = CONFIG.get("earthdata", "username")
    password = CONFIG.get("earthdata", "password")

    def __init__(self, short_name, version, time, coverage, filename, revisit):
        self.short_name = short_name
        self.version = version
        self.time = time
        self.coverage = coverage
        self.filename = filename
        self.revisit = revisit
        self.token = generate_earthdata_token(self.username, self.password)

    def iter_num(self):
        start, end = [parser.parse(i) for i in self.time.split(",")]
        days = end - start
        num_files = days.days * self.revisit
        num_pages = np.ceil(num_files / 10).astype(int)
        return num_pages

    def get(self):
        path, file = os.path.split(self.filename.path)
        os.makedirs(path, exist_ok=True)

        file, ext = os.path.splitext(file)
        num_pages = self.iter_num() + 1
        for page_num in range(1, num_pages):
            url = (
                f"{self.url}short_name={self.short_name}&version={self.version}&"
                f"format=GeoTIFF&time={self.time}&coverage={self.coverage}"
                f"&bbox={self.bbox}&projection=Geographic"
                f"&token={self.token}&page_num={page_num}"
            )
            response = requests.get(url, stream=True)
            temp = os.path.join(path, f"{file}_{page_num}{ext}")
            with open(temp, "wb") as handle:
                for block in response.iter_content(1000):
                    handle.write(block)
        return path


class SoilMoisture36km(ExternalTask):

    """
    Download soil moisture with 36km resolution from SMAP
    """

    def output(self):
        filename = IntermediateTarget(
            path="hydrology_model/SPL3SMP_ZIP/surface.zip", timeout=36000
        )
        short_name = "SPL3SMP"
        version = "005"
        time = "2015-03-31,2015-04-20"
        revisit = 1
        return NSIDCTarget(
            short_name,
            version,
            time,
            "/Soil_Moisture_Retrieval_Data_AM/soil_moisture",
            filename,
            revisit,
        )


class SoilMoisture9km(ExternalTask):

    """
    Download soil moisture with 9km resolution from SMAP
    """

    def output(self):
        filename = IntermediateTarget(
            path="hydrology_model/SPL3SMP_E_ZIP/surface.zip", timeout=3600
        )
        short_name = "SPL3SMP_E"
        version = "002"
        time = "2015-03-31,2018-04-20"
        revisit = 1
        return NSIDCTarget(
            short_name,
            version,
            time,
            "/Soil_Moisture_Retrieval_Data_AM/soil_moisture",
            filename,
            revisit,
        )


# TODO: Find another source of NDVI. EU has phased out spirits
class ScrapeNDVIData(ExternalTask):

    """
    Pull NDVI data and unzip the files
    """

    years = luigi.ListParameter(default=[2015], significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI/", timeout=36000)

    def substring(self, path):
        name = os.path.basename(path)
        idxs = [index for index, _ in enumerate(name) if name[index].isdigit()]
        start, end = idxs[0], (idxs[-1] + 1)
        return int(name[start:end])

    def run(self):

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir)
            print(tmpdir)

        year_list = [f"ea{str(year)[-2:]}" for year in self.years]
        # Download files
        files = get_url_paths("http://spirits.jrc.ec.europa.eu/files/emodis/east/")
        for i, f in enumerate(files):
            if i < 10:
                name = os.path.basename(f)
                if name[:4] in year_list:
                    r = requests.get(f)
                    zip_file = os.path.join(tmpdir, name)
                    with open(zip_file, "wb") as f:
                        f.write(r.content)

        # Unzip files
        for item in os.listdir(tmpdir):
            file_name = os.path.join(tmpdir, item)
            zip_ref = zipfile.ZipFile(file_name)
            zip_ref.extractall(tmpdir)
            zip_ref.close()
            os.remove(file_name)


@requires(SoilMoisture36km, CkanFile)
class ExtractZippedFiles36km(Task):
    def output(self):
        return IntermediateTarget(path="hydrology_model/SPL3SMP/", timeout=36000)

    def run(self):
        folder = self.input()[0].get()
        zip_file = self.input()[1]["boundary"].path

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            for file in os.listdir(folder):
                file_path = os.path.join(folder, file)
                with zipfile.ZipFile(file_path, "r") as archive:
                    archive.extractall(path=tmpdir)

            # Mask the files
            shp_file = extract_zipfile(
                zipath=zip_file, filename="box.shp", path=CACHE_DIR
            )
            with fiona.open(shp_file, "r") as shp:
                features = [feature["geometry"] for feature in shp]

            for subdirect in os.listdir(tmpdir):
                if os.path.isdir(f"{tmpdir}/{subdirect}/"):
                    name = os.listdir(f"{tmpdir}/{subdirect}/")[0]
                    src_path = os.path.join(tmpdir, subdirect, name)
                    dst_path = os.path.join(tmpdir, name)
                    mask_raster(src_path, dst_path, features)
                    shutil.rmtree(os.path.join(tmpdir, subdirect))


@requires(SoilMoisture9km, CkanFile)
class ExtractZippedFiles9km(ExtractZippedFiles36km):
    def output(self):
        IntermediateTarget(path="hydrology_model/SPL3SMP_E/", timeout=36000)


@requires(ExtractZippedFiles36km)
class ThreeDayAverage36km(Task):

    """
    Calculate the three day average
    """

    fewsnet_ndvi = luigi.BoolParameter(default=False, significant=False)

    def output(self):
        return IntermediateTarget(
            path="hydrology_model/SPL3SMP_3_day_avg/", timeout=36000
        )

    def run(self):
        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)
            key_filename_map = self.input()
            # Create a map of date and filename
            key_filename_map = {
                int(name): key_filename_map[name].path for name in key_filename_map
            }

            key_filename_map = dict(sorted(key_filename_map.items()))

            # Read the data
            for key, filename in key_filename_map.items():
                with rasterio.open(filename) as src:
                    key_filename_map[key] = np.squeeze(src.read())
                    no_data_val = src.nodata
                    profile = src.meta.copy()
                    profile["dtype"] = "float32"
                    key_filename_map[key] = np.where(
                        key_filename_map[key] == no_data_val,
                        np.nan,
                        key_filename_map[key],
                    )

            if self.fewsnet_ndvi:
                for key in key_filename_map:
                    key_filename_map[key] = self.actual_ndvi(key_filename_map[key])
            # Get three day mean
            key_list = list(key_filename_map)
            index = 0
            while (index + 2) <= (len(key_list) - 1):
                key1, key2, key3 = (
                    key_list[index],
                    key_list[index + 1],
                    key_list[index + 2],
                )
                output_array = np.nanmean(
                    [
                        key_filename_map[key1],
                        key_filename_map[key2],
                        key_filename_map[key3],
                    ],
                    axis=0,
                )
                index = index + 3

                # fillna with nodata
                output_array[np.isnan(output_array)] = src.nodata

                if not os.path.exists(tmpdir):
                    os.makedirs(tmpdir, exist_ok=True)
                dst_path = os.path.join(tmpdir, str(key3) + ".tif")
                with rasterio.open(dst_path, "w", **profile) as dst:
                    dst.write(np.float32(output_array), 1)

    def actual_ndvi(self, array):
        """
        https://earlywarning.usgs.gov/fews/product/448
        """
        array = np.where(array > 200, np.nan, array)
        array = (array - 100) / 100
        return array


@requires(ExtractZippedFiles9km)
class ThreeDayAverage9km(ThreeDayAverage36km):
    """
    """

    def output(self):
        return IntermediateTarget(
            path="hydrology_model/SPL3SMP_E_3_day_avg/", timeout=36000
        )


@requires(ScrapeNDVIData, CkanFile)
class MaskRasterNDVI(Task):

    """
    Mask NDVI data to South Sudan box
    """

    shpfile_key = luigi.Parameter(default="boundary", significant=False)
    shpfile_name = luigi.Parameter(default="box.shp", significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI_masked/", timeout=36000)

    def run(self):
        file_path_dict = self.input()[0]
        boundary_dict = self.input()[1]

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            ssudan_zip_bbox_path = boundary_dict[self.shpfile_key].path

            ssudan_bbox_path = extract_zipfile(
                ssudan_zip_bbox_path, self.shpfile_name, CACHE_DIR
            )

            with fiona.open(ssudan_bbox_path, "r") as shp:
                features = [feature["geometry"] for feature in shp]

            for rast_path in file_path_dict:
                src_path = file_path_dict[rast_path].path
                dst_file = os.path.join(tmpdir, f"{rast_path}.tif")
                mask_raster(src_path, dst_file, features)


@requires(MaskRasterNDVI)
class MonthlyNDVI(ThreeDayAverage36km):

    """
    Calculate NDVI monthly average from dekadal data
    """

    fewsnet_ndvi = luigi.BoolParameter(default=True, significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/monthly_ndvi", timeout=3600)


@requires(MonthlyNDVI, ThreeDayAverage36km)
class ReprojectNDVI36km(Task):

    """
    Resample NDVI from 250m to 36km spatial resolution
    """

    nodata = luigi.FloatParameter(default=-9999.0, significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI36km/", timeout=36000)

    def run(self):
        ndvi_input = self.input()[0]
        if type(ndvi_input) == dict:
            try:
                ndvi__dict = {k: v.path for k, v in ndvi_input.items()}
            except AttributeError:
                ndvi__dict = ndvi_input
        else:
            ndvi__dict = map_f_path(ndvi_input.path)

        SPL3SMP_input = self.input()[1]
        if isinstance(SPL3SMP_input, dict):
            try:
                SPL3SMP_file = list(SPL3SMP_input.values())[0].path
            except AttributeError:
                SPL3SMP_file = list(SPL3SMP_input.values())[0]
        else:
            SPL3SMP_map = map_f_path(SPL3SMP_input.path)
            SPL3SMP_file = list(SPL3SMP_map.values())[0]

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

            for key in ndvi__dict:
                reproject_like(
                    rast_file=ndvi__dict[key],
                    proj_file=SPL3SMP_file,
                    output_file=os.path.join(tmpdir, f"{key}.tif"),
                    nodata=self.nodata,
                )


@requires(MonthlyNDVI, ThreeDayAverage9km)
class ReprojectNDVI9km(ReprojectNDVI36km):

    """
    Resample NDVI from 250m to 9 km resolution
    """

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI9km/", timeout=36000)


@requires(MonthlyNDVI)
class ReprojectRetrivalNDVI2_25km(Task):

    """
    Resample NDVI from 250m to 2.25 km resolution
    """

    def output(self):
        return IntermediateTarget(path="hydrology_model/retrival_ndvi/", timeout=36000)

    def run(self):
        data_path = self.input()

        with self.output().temporary_path() as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

        for file in data_path:
            resample_raster(
                src_path=data_path[file].path,
                dst_path=os.path.join(tmpdir, f"{file}.tif"),
            )


@requires(ThreeDayAverage9km, ThreeDayAverage36km)
class AverageUsingWindow(Task):

    """
    Resample soil moisture data from 36km to 45km using a moving window
    """

    def output(self):
        return IntermediateTarget(path="hydrology_model/SM45km/", timeout=36000)

    def run(self):
        sm_9km = self.input()[0]
        sm_36km = self.input()[1]

        with self.output().temporary_path as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

        # Get the filepath of one of the target raster
        med_res_src = list(sm_9km.values())[0].path

        # Get raster metadata from the target raster
        with rasterio.open(med_res_src) as src:
            profile = src.profile
            profile["dtype"] = "float32"

        # Calculate the weighted average
        for key in sorted(sm_36km):
            array = weighted_average(sm_36km[key].path, med_res_src)

            dst_filename = os.path.join(self.output_path, f"{key}.tif")

            with rasterio.open(dst_filename, "w", **profile) as dst:
                dst.write(np.float32(array), 1)


@requires(ThreeDayAverage9km, ReprojectNDVI36km)
class AverageNDVI45km(AverageUsingWindow):

    """
    Resample NDVI data from 36km to 45km using a moving window
    """

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI45km/", timeout=36000)


@requires(ReprojectRetrivalNDVI2_25km, ThreeDayAverage9km)
class AverageRetrivalSM11_25km(AverageUsingWindow):

    """
    Resample soil moisture data from 9km to 11.25km using a moving window
    """

    def output(self):
        return IntermediateTarget(path="hydrology_model/SM11km/", timeout=36000)


@requires(ReprojectRetrivalNDVI2_25km, ReprojectNDVI9km)
class AverageRetrivalNDVI11_25km(AverageUsingWindow):

    """
    Resample NDVI data from 9km to 11.25km using a moving window
    """

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI11_25km/", timeout=36000)


@requires(AverageUsingWindow, CkanFile)
class MaskToSSudanSM45km(MaskRasterNDVI):

    """
    Mask 45km soil moisture data to South Sudan boundary
    """

    shpfile_key = luigi.Parameter(default="ss_boundary", significant=False)
    shpfile_name = luigi.Parameter(default="ss_admin0.shp", significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/SM45km_ss/", timeout=36000)


@requires(ThreeDayAverage9km, CkanFile)
class MaskToSSudanSM9km(MaskRasterNDVI):

    """
    Mask 9km soil moisture data to South Sudan boundary
    """

    shpfile_key = luigi.Parameter(default="ss_boundary", significant=False)
    shpfile_name = luigi.Parameter(default="ss_admin0.shp", significant=False)

    def output(self):
        return IntermediateTarget(
            path="hydrology_model/SPL3SMP_E_3_day_avg_ss/", timeout=36000
        )


@requires(ThreeDayAverage36km, CkanFile)
class MaskToSSudanSM36km(MaskRasterNDVI):

    """
    Mask 36km soil moisture data to South Sudan boundary
    """

    shpfile_key = luigi.Parameter(default="ss_boundary", significant=False)
    shpfile_name = luigi.Parameter(default="ss_admin0.shp", significant=False)

    def output(self):
        return IntermediateTarget(
            path="hydrology_model/SPL3SMP_3_day_avg_ss/", timeout=36000
        )


@requires(AverageNDVI45km, CkanFile)
class MaskToSSudanNDVI45km(MaskRasterNDVI):

    """
    Mask 45km NDVI data to South Sudan boundary
    """

    shpfile_key = luigi.Parameter(default="ss_boundary", significant=False)
    shpfile_name = luigi.Parameter(default="ss_admin0.shp", significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI45km_ss", timeout=36000)


@requires(ReprojectNDVI9km, CkanFile)
class MaskToSSudanNDVI9km(MaskRasterNDVI):

    """
    Mask 9km NDVI data to South Sudan boundary
    """

    shpfile_key = luigi.Parameter(default="ss_boundary", significant=False)
    shpfile_name = luigi.Parameter(default="ss_admin0.shp", significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI9km_ss/", timeout=36000)


@requires(AverageRetrivalSM11_25km, CkanFile)
class MaskToSSudanSM1Ikm(MaskRasterNDVI):

    """
    Mask 11.25km soil moisture data to South Sudan boundary
    """

    shpfile_key = luigi.Parameter(default="ss_boundary", significant=False)
    shpfile_name = luigi.Parameter(default="ss_admin0.shp", significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/SM11km_ss/")


@requires(AverageRetrivalNDVI11_25km, CkanFile)
class MaskToSSudanNDVI1Ikm(MaskRasterNDVI):

    """
    Mask 11.25km NDVI data to South Sudan boundary
    """

    shpfile_key = luigi.Parameter(default="ss_boundary", significant=False)
    shpfile_name = luigi.Parameter(default="ss_admin0.shp", significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI11_25km_ss/", timeout=36000)


@requires(ReprojectRetrivalNDVI2_25km, CkanFile)
class MaskToSSudanNDVI2km(MaskRasterNDVI):

    """
    Mask 2.25km NDVI data to South Sudan boundary
    """

    shpfile_key = luigi.Parameter(default="ss_boundary", significant=False)
    shpfile_name = luigi.Parameter(default="ss_admin0.shp", significant=False)

    def output(self):
        return IntermediateTarget(path="hydrology_model/NDVI_2_25km_ss/", timeout=36000)


@requires(
    MaskToSSudanSM9km, MaskToSSudanSM45km, MaskToSSudanNDVI45km, MaskToSSudanNDVI9km
)
class ExtractData(Task):

    """
    Prepare data for training the model
    """

    def output(self):
        return IntermediateTarget(path="hydrology_model/train_data", timeout=1_000_000)

    def run(self):
        sm_9km_map = self.input()[0]
        sm_45km_map = self.input()[1]
        ndvi_45km_map = self.input()[2]
        ndvi_9km_map = self.input()[3]

        index = 0
        for _, key in enumerate(sm_45km_map):
            try:
                ndvi_key = self.get_ndvi_key(key)
                # Stack the variables
                features = [
                    sm_45km_map[key].path,
                    ndvi_45km_map[ndvi_key].path,
                    ndvi_9km_map[ndvi_key].path,
                ]
                stack = pyspatialml.stack_from_files(features)

                # Load a soil moisture data
                training_px = rasterio.open(sm_9km_map[key].path)

                # Extract data
                temp = stack.extract_vector(response=training_px, field="sm")

                if index == 0:
                    df = temp
                else:
                    df = pd.concat([df, temp])

                index += 1
            except KeyError:
                pass

        with self.output().open("w") as output:
            output.write(df)

    def get_ndvi_key(self, sm_key):
        y, m = sm_key[2:4], sm_key[4:6]
        dekad = int(m) * 3
        ndvi_key = f"{y}{dekad}" if dekad > 9 else f"{y}0{dekad}"
        return ndvi_key


@requires(ExtractData)
class TrainDownscalingModel(Task):

    """
    Train neural network for downscaling the soil moisture
    """

    def output(self):
        return {
            "model": IntermediateTarget(
                path="hydrology_model/mlp_model.h5", timeout=36000
            ),
            "weight": IntermediateTarget(
                path="hydrology_model/weights.h5", timeout=36000
            ),
        }

    def run(self):
        X, y, xy = self.input().get()

        # Split the training and testing data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.25, random_state=42
        )

        # Multi-Layer Perceptrons
        model = mlp_model()

        # Save the model weight after every epoch
        checkpointer = ModelCheckpoint(
            filepath=self.output()["weight"].path, verbose=1, save_best_only=True
        )

        # Train the model
        model.fit(
            X_train,
            y_train,
            batch_size=128,
            epochs=20,
            verbose=0,
            validation_split=0.3,
            callbacks=[checkpointer],
        )

        # Save the model
        model.save(self.output()["model"].path)


@requires(
    TrainDownscalingModel, MaskToSSudanSM1Ikm, MaskToSSudanNDVI1Ikm, MaskToSSudanNDVI2km
)
class EstimateSoilMoisture(Task):

    """
    Downscale the soil moisture data
    """

    def output(self):
        return IntermediateTarget(
            path="hydrology_model/Retrived_SM_2_25km/", timeout=36000
        )

    def run(self):
        model_path = self.input()[0]["model"].path
        sm_11_25km = self.input()[1]
        ndvi_11_25km = self.input()[2]
        ndvi_2_25km = self.input()[3]

        for d in [sm_11_25km, ndvi_11_25km, ndvi_2_25km]:
            d = {k: v.path for k, v in d.items()}

        with self.output().temporary_path as tmpdir:
            os.makedirs(tmpdir, exist_ok=True)

        # Load the model
        model = load_model(model_path)

        for key in sm_11_25km:
            ndvi_key = self.get_ndvi_key(key)

            # Stack the variables
            variable_paths = [
                sm_11_25km[key],
                ndvi_11_25km[ndvi_key],
                ndvi_2_25km[ndvi_key],
            ]

            stack = pyspatialml.stack_from_files(variable_paths)

            file_path = os.path.join(tmpdir, f"{key}.tif")

            stack.predict(estimator=model, file_path=file_path, nodata=-9999.0)

    def get_ndvi_key(self, sm_key):
        y, m = sm_key[2:4], sm_key[4:6]
        dekad = int(m) * 3
        ndvi_key = f"{y}{dekad}" if dekad > 9 else f"{y}0{dekad}"
        return ndvi_key


@requires(TrainDownscalingModel)
class UploadDownscalingModelToCkan(Task):

    """
    Task for uploading dowscaling model to ckan
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "5a778a98-8ef4-4d69-97b8-d87682e00728"},
            resource={"name": "Dowscaling Soil Moisture Model"},
        )

    def run(self):
        file_path = self.input()["model"].path
        if self.output().exists():
            self.output().remove()
        self.output().put(file_path=file_path)
