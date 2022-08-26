import calendar
import logging
import os
import tempfile
from functools import partial
from multiprocessing import Pool, cpu_count

import descarteslabs as dl
import luigi
import numpy as np
import pandas as pd
import rasterio
from descarteslabs.catalog import properties as p
from luigi import Task
from luigi.util import inherits, requires
from rasterio.merge import merge
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint

from kiluigi.targets import FinalTarget, IntermediateTarget
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

from .data_pre import TrainingData
from .model import dice_coef, dice_coef_loss
from .model import mini_unet as estimator
from .utils import flood_mapping

logger = logging.getLogger("luigi-interface")


@requires(TrainingData)
class Train(Task):
    def output(self):
        dst = "models/hydrology_model/flood_mapping/mini_unet_weights.h5"
        return FinalTarget(path=dst, task=self)

    def run(self):
        with self.input().open() as src:
            data_map = src.read()

        X_train, y_train = data_map["train"]
        X_valid, y_valid = data_map["valid"]
        X_test, y_test = data_map["test"]

        # TODO: Find another way to deal with missing values
        X_train = np.nan_to_num(X_train)
        X_valid = np.nan_to_num(X_valid)
        X_test = np.nan_to_num(X_test)

        # TODO: Find another way to deal with missing values
        y_train = np.where(y_train == -1, 0, y_train)
        y_valid = np.where(y_valid == -1, 0, y_valid)
        y_test = np.where(y_test == -1, 0, y_test)

        y_train = y_train.astype("float32")
        y_valid = y_valid.astype("float32")
        y_test = y_test.astype("float32")

        model = estimator(n_classes=1, input_height=512, input_width=512, channels=2)
        model.compile(optimizer="adam", loss=dice_coef_loss, metrics=[dice_coef])

        callbacks = [
            ModelCheckpoint(tempfile.NamedTemporaryFile(suffix=".h5").name),
            EarlyStopping(monitor="loss", patience=5),
        ]
        model.fit(
            x=X_train,
            y=y_train,
            batch_size=8,
            epochs=30,
            validation_data=(X_valid, y_valid),
            callbacks=callbacks,
        )

        y_pred = model.predict(X_test)

        logger.info(f"Dice score is: {dice_coef(y_test, y_pred)}")

        with self.output().open("w") as out:
            model.save_weights(out.name)


@requires(Train)
@inherits(GlobalParameters)
class FloodMappingDescartesLab(Task):
    model_storage_key = luigi.Parameter(default="unet_flood_model")
    catalog_name = luigi.Parameter(default="flood_mapping")

    def output(self):
        return IntermediateTarget(task=self, timeout=31536000)

    def create_product(self):
        auth = dl.Auth()
        user = auth.payload["name"]
        catalog_client = dl.Catalog()
        try:
            product_id = catalog_client.add_product(
                f"{auth.namespace}:{user.lower().replace(' ', '')}:{self.catalog_name}",
                title="Flood Detection",
                description="Flood Detection from sentinel 1",
            )["data"]["id"]
            _ = catalog_client.add_band(
                product_id=product_id,
                name="flood_prob",
                jpx_layer=0,
                srcfile=0,  # first file
                srcband=1,  # just one band
                default_range=[0, 1],
                nbits=1,
                color="Palette",
                colormap_name="viridis",
                dtype="Float32",
                data_range=[0, 1],
                nodata=-9999,
                type="spectral",
            )
        except dl.client.exceptions.ConflictError:
            product_id = (
                f"{auth.namespace}:{user.lower().replace(' ', '')}:{self.catalog_name}"
            )
        return product_id

    def run(self):
        model_src = self.input().path
        # Upload the model to descartes storage
        storage_clinet = dl.Storage()
        if not storage_clinet.exists(self.model_storage_key):
            storage_clinet.set_file(
                key=self.model_storage_key, file_obj=model_src, storage_type="data"
            )
        try:
            geom_mask = [i["geometry"] for i in self.geography["features"]]
        except KeyError:
            geom_mask = [self.geography]

        # To monitor tasks visit https://monitor.descarteslabs.com/
        tasks_client = dl.client.services.tasks.Tasks()
        async_func = tasks_client.create_function(
            flood_mapping,
            name="sentinel-class-task",
            # image="us.gcr.io/dl-ci-cd/images/tasks/public/py3.8:v2021.03.03-6-g160ae345",
            image="us.gcr.io/dl-ci-cd/images/tasks/public/py3.7-gpu:v2021.03.03-6-g160ae345",
            gpus=1,
            retry_count=2,  # helps to bypass transient outage on Google's platform
            memory="20Gi",  # to avoid exceeding memory
        )
        month_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
        tasks = []
        for aoi_geometry in geom_mask:
            tiles = dl.raster.dltiles_from_shape(10, 512, 0, aoi_geometry)
            for month in month_list:
                start_date_str = month.replace(day=1).strftime("%Y-%m-%d")
                end_date_str = month.strftime("%Y-%m-%d")
                # run and send tasks to server
                for tile in tiles["features"]:
                    t = async_func(
                        tile=tile,
                        model_key=str(self.model_storage_key),
                        start_date=start_date_str,
                        end_date=end_date_str,
                        product_id=self.create_product(),
                    )
                    tasks.append(t)
        logger.info("waiting for the tasks running descartes platform to complete")
        async_func.wait_for_completion(show_progress=True)
        logger.info("Rerun failed tasks")
        dl.tasks.rerun_failed_tasks(async_func.group_id, retry_count=5)
        async_func.wait_for_completion(show_progress=True)

        # with self.output().open("w") as out:


@inherits(GlobalParameters)
class DownloadFloodMap(Task):
    spatial_resolution = luigi.TupleParameter(
        default=(0.008334173612994345, 0.008334173612994345)
    )

    def output(self):
        month_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
        return {i: FinalTarget(path=self.filename(i)) for i in month_list}

    def filename(self, month):
        country = self.country_level.replace(" ", "_").lower()
        month_str = month.strftime("%y_%m")
        return f"flood_mapping/flood_map_{country}_{month_str}.tif"

    @staticmethod
    def download_image(image, dst):
        image_id = image.id
        min_id = ":".join(image_id.split(":")[2:])
        dst_name = os.path.join(dst, min_id)
        dl.raster.raster(
            inputs=image_id,
            bands=["flood_prob"],
            srs="EPSG:4326",
            outfile_basename=dst_name,
            save=True,
        )
        return dst_name

    @staticmethod
    def get_month_range(month):
        first_day = month.replace(day=1).strftime("%Y-%m-%d")
        last_day = month.replace(day=calendar.monthrange(month.year, month.month)[1])
        last_day = last_day.strftime("%Y-%m-%d")
        return first_day, last_day

    def download_images(self, month):
        start_date_str, end_date_str = self.get_month_range(month)
        images = dl.catalog.Product.get(
            "2dc24dfc41704971a7dee5dbcdae5713273fcbf7:petermburu:flood_mapping"
        ).images()
        images_filter = images.filter(
            (p.acquired >= start_date_str) & (p.acquired <= end_date_str)
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(temp_dir, exist_ok=True)

            pool = Pool(cpu_count())
            download_func = partial(self.download_image, dst=temp_dir)
            results = pool.map(download_func, images_filter)
            pool.close()
            pool.join()

            files = [rasterio.open(f"{i}.tif") for i in results]
            meta = files[0].profile.copy()

            dest, out_transform = merge(files, res=self.spatial_resolution)

        _, h, w = dest.shape
        meta.update(transform=out_transform, height=h, width=w)
        return dest, meta

    def run(self):
        month_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
        for month in month_list:
            logger.info(f"Downloading data for {month}")
            arr, meta = self.download_images(month)
            with self.output()[month].open("w") as out:
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write(arr)
