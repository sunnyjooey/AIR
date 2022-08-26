import datetime
import io
import os
import shutil
import zipfile

import numpy as np
import rasterio
import requests
from keras import layers, models, optimizers
from keras.applications import resnet50
from keras.callbacks import EarlyStopping, ModelCheckpoint
from keras.preprocessing.image import ImageDataGenerator
from luigi.configuration import get_config
from luigi.date_interval import Custom as CustomDateInterval
from luigi.parameter import DateIntervalParameter
from luigi.util import requires
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform

import descarteslabs as dl
from descarteslabs.client.services.tasks import Tasks
from kiluigi.parameter import GeoParameter
from kiluigi.targets import IntermediateTarget
from kiluigi.tasks import ExternalTask, Task

CONFIG = get_config()


class DownloadEuroSATData(ExternalTask):
    """
    This task downloads and extracts the EuroSAT dataset (94.3MB)
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=1000000000)

    def run(self):
        outdir = CONFIG.get("core", "cache_dir")
        if not os.path.isdir(os.path.join(outdir, "2750")):
            request = requests.get("http://madm.dfki.de/files/sentinel/EuroSAT.zip")
            file = zipfile.ZipFile(io.BytesIO(request.content))
            file.extractall(outdir)
        with self.output().open("w") as dst:
            dst.write(os.path.join(outdir, "2750"))


@requires(DownloadEuroSATData)
class PrepareTrainTestData(Task):
    """
    This task performs an 80/20 train/test split of the EuroSAT data for training and validating the model.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=1000000000)

    def run(self):
        with self.input().open("r") as src:
            data_dir = src.read()

        # create separate directories w/ sub-folder for each class for train and test data
        # NOTE: needed for keras data generators
        train_dir = os.path.join(CONFIG.get("core", "cache_dir"), "EuroSAT/train")
        test_dir = os.path.join(CONFIG.get("core", "cache_dir"), "EuroSAT/test")

        classes = os.listdir(data_dir)
        if not os.path.exists(train_dir):
            os.makedirs(train_dir)
            for c in classes:
                os.makedirs(os.path.join(train_dir, c))
        if not os.path.exists(test_dir):
            for c in classes:
                os.makedirs(os.path.join(test_dir, c))

            for d in os.listdir(data_dir):
                class_path = os.path.join(data_dir, d)
                # move 80% of images in each class to training directory, 20% to test
                n_img = len(os.listdir(class_path))
                n_train = int(0.8 * n_img)
                train_ind = np.random.choice(range(n_img), size=n_train, replace=False)
                for i, f in enumerate(os.listdir(class_path)):
                    if i in train_ind:
                        shutil.move(
                            os.path.join(class_path, f), os.path.join(train_dir, d, f)
                        )
                    else:
                        shutil.move(
                            os.path.join(class_path, f), os.path.join(test_dir, d, f)
                        )

        with self.output().open("w") as dst:
            dst.write({"train_dir": train_dir, "test_dir": test_dir})


@requires(PrepareTrainTestData)
class TrainResNet(Task):
    """
    This task fine-tunes as ResNet50 model trained on the ImageNet dataset to produce land cover predictions on
    64x64 Sentinel-2 images from the EuroSAT dataset. It saves the model weights that result in the lowest
    validation error to an HDF5 file, which can be uploaded to the Descartes Labs servers.
    """

    def output(self):
        return IntermediateTarget(task=self, timeout=1000000000)

    def run(self):
        with self.input().open("r") as src:
            _input = src.read()
            train_dir = _input["train_dir"]
            test_dir = _input["test_dir"]

        # load ResNet50 trained on ImageNet
        resnet_model = resnet50.ResNet50(
            weights="imagenet", include_top=False, input_shape=(64, 64, 3)
        )

        # add fully connected layer and classification layer
        model = models.Sequential()
        model.add(resnet_model)
        model.add(layers.Flatten())
        model.add(layers.Dense(1024, activation="relu"))
        model.add(layers.Dropout(0.5))
        model.add(layers.Dense(10, activation="softmax"))

        # create train and test sets from directories
        train_datagen = ImageDataGenerator(
            rescale=1.0 / 255, shear_range=0.2, zoom_range=0.2, horizontal_flip=True
        )
        test_datagen = ImageDataGenerator(rescale=1.0 / 255)

        training_set = train_datagen.flow_from_directory(
            train_dir, target_size=(64, 64), batch_size=32, class_mode="categorical"
        )
        test_set = test_datagen.flow_from_directory(
            test_dir,
            target_size=(64, 64),
            batch_size=32,
            class_mode="categorical",
            shuffle=False,
        )

        # compile model
        model.compile(
            loss="categorical_crossentropy",
            optimizer=optimizers.RMSprop(lr=1e-4),
            metrics=["acc"],
        )

        # designate file to save model weights
        weights_path = os.path.join(
            CONFIG.get("core", "cache_dir"), "weights.best.hdf5"
        )

        # add checkpoints to save best iteration of model, stop training early if validation loss increases
        checkpoint = ModelCheckpoint(
            weights_path, monitor="val_loss", verbose=1, save_best_only=True, mode="min"
        )
        early_stop = EarlyStopping(monitor="val_loss", patience=3)
        callbacks_list = [checkpoint, early_stop]

        # train model
        model.fit_generator(
            training_set,
            steps_per_epoch=(training_set.samples / 32),
            epochs=20,
            callbacks=callbacks_list,
            validation_data=test_set,
            validation_steps=(test_set.samples / 32),
        )

        with self.output().open("w") as dst:
            dst.write(weights_path)


@requires(TrainResNet)
class LandCoverClassification(Task):

    """
    This task performs land cover classification for an arbitrary date range and geography using Descartes Labs servers.
    The model used to perform the classification is a ResNet50 trained on the EuroSAT dataset. The model achieves a
    91% classification accuracy on the 10-class dataset, whose classes are: Annual Crop, Forest, Herbaceous Vegetation,
    Highway, Industrial, Pasture, Permanent Crop, Residential, and River. A user could upload their own pre-trained
    model to the Descartes Labs Catalog, but it is currently using Alex's pre-trained model.

    The output of this task is the Descartes Labs product id, which will be needed to access the data layer after it has
    been created.
    """

    geography = GeoParameter(
        default={
            "type": "Polygon",
            "coordinates": (
                (
                    (29.37163939728741, 7.804534284421441),
                    (29.879894108623596, 7.7729874402695405),
                    (29.893914928246662, 7.345352441765989),
                    (29.364628987475875, 7.418961744787093),
                    (29.37163939728741, 7.804534284421441),
                ),
            ),
        }
    )
    time = DateIntervalParameter(
        default=CustomDateInterval(
            datetime.date.fromisoformat("2017-03-01"),
            datetime.date.fromisoformat("2017-06-01"),
        )
    )

    def output(self):
        return IntermediateTarget(task=self)

    def land_cover_classification(
        self,
        tile,
        model_key,
        start_date,
        end_date,
        product_id,
        tile_size=1024,
        cell_size=64,
    ):
        """
        This is the function that will be submitted to the DescartesLabs server to run in parallel. Given a tile,
        it downloads the Sentinel-2 imagery for a specified date range; creates an RGB composite median image, splits
        that image up into smaller cells of the correct dimensionality for prediction with a ResNet50 model; predicts
        the land cover classified for each cell; recombines those cells into a full tile; and uploads that tile to the
        DescartesLabs Catalog.

        :param tile: DescartesLabs Tile object
        :param model_key: name of pre-trained mode lin DL Storage
        :param start_date: YYYY-MM-DD
        :param end_date: YYYY-MM-DD
        :param product_id: name of data layer in DL Catalog
        :param tile_size:
        :param cell_size:
        :return: None (tile uploaded directly to DL Catalog
        """

        import descarteslabs as dl
        import numpy as np
        from keras.models import load_model

        def get_imagery(tile, start_date, end_date):
            """
            Find Sentinel-2 imagery for tile, create RGB composite

            :param tile: DescartesLabs Tile
            :param start_date: YYYY-MM-DD
            :param end_date: YYYY-MM-DD
            :return: np.array of median RGB composite image
            """
            scenes, ctx = dl.scenes.search(
                tile["geometry"],
                products=["sentinel-2:L1C"],
                start_datetime=start_date,
                end_datetime=end_date,
                cloud_fraction=0.1,
            )
            scene_ids = [s.properties["id"] for s in scenes]
            dltile_key = tile["properties"]["key"]

            # need to go through Raster client to get images with correct dimensionality, coloring
            raster_client = dl.Raster()
            stack, _ = raster_client.stack(
                scene_ids,
                dltile=dltile_key,
                bands=["red", "green", "blue"],
                scales=[[0, 4000], [0, 4000], [0, 4000]],
                srs="epsg:4326",
                data_type="Byte",
            )
            # take median value of stack
            stack_med = np.ma.median(stack, axis=0).astype("uint8")

            return stack_med

        def tile_to_cells(image, cell_size):
            """
            :param image: RGB numpy array
            :param cell_size: desired size of model input, must be divisor of tile size
            :return: numpy array of smaller images for input to model
            """
            # split tile into 64x64x3 cells for ResNet50 prediction
            n_cells = int(image.shape[0] / cell_size) ** 2
            images = np.empty(shape=(n_cells, cell_size, cell_size, 3), dtype=np.uint8)
            x, y = 0, 0
            for i in range(n_cells):
                x_end = x + cell_size
                y_end = y + cell_size
                images[i, :, :, :] = stack_med[x:x_end, y:y_end, :]
                x += cell_size
                if x >= tile_size:
                    x = 0
                    y += cell_size

            return images

        def predict_land_cover(model_key, input_arr, tile_size, cell_size):
            """

            :param model_key: name of model in DescartesLabs Storage
            :param input_arr: array of images for prediction
            :param tile_size: size of tile
            :param cell_size: size of sub-block
            :return: tile with land cover predictions for each block of size cell_size x cell_size
            """
            # predict land cover for entire tile

            # Get model from Storage and save it local to the Task
            new_model_filepath = "weights.best.hdf5"
            dl.Storage().get_file(key=model_key, file_obj=new_model_filepath)
            # Now the file you uploaded with set_file should be available at weights.best.hdf5
            model = load_model(new_model_filepath)

            preds = model.predict(input_arr / 255.0)
            pred_class = list(np.argmax(preds, axis=1))

            # assign land cover classification for each 64x64 cell
            out_arr = np.empty((tile_size, tile_size), dtype=np.int8)
            x, y = 0, 0
            n_cells = int(tile_size / cell_size) ** 2
            for i in range(n_cells):
                x_end = x + cell_size
                y_end = y + cell_size
                out_arr[x:x_end, y:y_end] = pred_class[i]
                x += cell_size
                if x >= tile_size:
                    x = 0
                    y += cell_size
            return out_arr

        def upload_to_catalog(arr, tile, product_id, start_date):
            """
            :param arr: numpy array of land cover predictions for tile
            :param tile: DescartesLabs Tile (for metadata)
            :param product_id: id of data layer in DL catalog
            :param start_date: YYYY-MM-DD, used for timestamping tile
            :return: None
            """
            props = tile["properties"]
            catalog = dl.Catalog()
            catalog.upload_ndarray(
                arr,
                product_id=product_id,
                image_id=props["key"],
                proj4=props["proj4"],
                geotrans=props["geotrans"],
                acquired=start_date,
            )

        stack_med = get_imagery(tile, start_date, end_date)
        images = tile_to_cells(stack_med, cell_size)
        lc_arr = predict_land_cover(model_key, images, tile_size, cell_size)
        upload_to_catalog(lc_arr, tile, product_id, start_date)

        return

    def create_product(self, product_id, product_title):
        """
        This function creates the land cover product in the Descartes Labs Catalog.

        :param product_id: DL product id
        :param product_title: Name of layer
        """
        # define the product
        dl.Catalog().add_product(
            product_id,
            title=product_title,
            description="Land cover classification based on 10-class EuroSAT dataset",
            start_datetime="2015-01-01",
            end_datetime="2018-12-31",
            writers=["darpa"],
        )["data"]["id"]

        dl.Catalog().add_band(
            product_id,
            "land_cover",
            jpx_layer=0,
            srcfile=0,
            srcband=1,
            nbits=8,
            dtype="Byte",
            nodata=255,
            data_range=[0, 9],
            type="class",
            default_range=(0, 9),
            colormap_name="viridis",
        )["data"]["id"]

        return

    def run(self):
        # get DL tiles for AOI
        tiles = dl.raster.dltiles_from_shape(
            resolution=10.0, tilesize=1024, pad=0, shape=self.geography
        )

        # sign in to DL
        # FLAG: need to figure out how this will work within Docker container
        auth = dl.Auth()
        user = auth.payload["name"]
        product_id = "{}:{}:world_modelers_land_cover".format(
            auth.namespace, user.lower().replace(" ", "")
        )
        product_title = "DARPA World Modelers EuroSAT Land Cover"

        # create product in DL catalog if it doesn't exit
        if product_id not in [p["id"] for p in dl.Catalog().own_products()]:
            self.create_product(product_id, product_title)

        # load pre-trained model into DL Storage
        with self.input().open("r") as src:
            weights_file = src.read()
        if not dl.Storage().exists("resnet50_eurosat"):
            dl.Storage().set_file(key="resnet50_eurosat", file_obj=weights_file)

        # load function that will be submitted to server, or create it if it doesn't already exist
        tasks_client = Tasks()
        async_func = tasks_client.get_function("lc-class-task")
        if async_func is None:
            async_func = tasks_client.create_function(
                self.land_cover_classification,
                name="lc-class-task",
                image="us.gcr.io/dl-ci-cd/images/tasks/public/py3.6/default:v2019.02.27",
                requirements=["keras==2.2.4"],
            )

        # send tasks to server
        tasks = []
        for tile in tiles["features"]:
            # check to see if tile already exists
            scenes, _ = dl.scenes.search(
                tile["geometry"],
                products=[product_id],
                start_datetime=self.time[0],
                end_datetime=self.time[1],
            )

            # if not, submit
            if len(scenes) == 0:
                t = async_func(
                    tile=tile,
                    model_key="resnet50_eurosat",
                    start_date=self.time[0],
                    end_date=self.time[1],
                    product_id=product_id,
                    tile_size=1024,
                )
                tasks.append(t)

        with self.output().open("w") as dst:
            dst.write(product_id)


@requires(LandCoverClassification)
class IdentifyCropLand(Task):
    """
    This task accesses the land cover data layer in the Descartes Labs Catalog for a given time and geography
    and outputs a raster that indicates where annual crop land in present. Note that this will probably fail for
    very large AOIs, as it merges land cover rasters for each 10km x 10km DL tile (each about 15kB) into a single
    raster.
    """

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        with self.input().open("r") as src:
            product_id = src.read()

        # get DL tiles for AOI
        tiles = dl.raster.dltiles_from_shape(
            resolution=10.0, tilesize=1024, pad=0, shape=self.geography
        )

        # temporary folder to store downloaded rasters
        outfolder = os.path.join(CONFIG.get("core", "cache_dir"), "lc_rasters")
        if not os.path.isdir(outfolder):
            os.makedirs(outfolder)

        for tile in tiles["features"]:
            # get land cover data for tile
            scenes, _ = dl.scenes.search(
                tile["geometry"],
                products=[product_id],
                start_datetime=self.time[0],
                end_datetime=self.time[1],
            )

            scene_ids = [s.properties["id"] for s in scenes]
            dltile_key = tile["properties"]["key"]

            # download raster for tile to temporary folder
            dl.Raster().raster(
                scene_ids,
                dltile=dltile_key,
                srs="epsg:4326",
                data_type="Byte",
                output_format="GTiff",
                save=True,
                outfile_basename=os.path.join(outfolder, dltile_key),
            )

        # merge rasters
        rasters = []
        for f in os.listdir(outfolder):
            rasters.append(rasterio.open(os.path.join(outfolder, f)))
        merged_img, merged_transform = merge(rasters)

        # mask out everything other than crop land
        merged_img[merged_img != 0] = rasters[0].nodata

        # write out merged rasters
        out_file = os.path.join(CONFIG.get("core", "cache_dir"), "crop_cover.tif")
        src_crs = rasters[0].crs
        dst_crs = {"init": "EPSG:4326"}
        w = merged_img.shape[2]
        h = merged_img.shape[1]
        left = merged_transform.c
        bottom = merged_transform.f - h
        right = merged_transform.c + w
        top = merged_transform.f
        transform_4326, width, height = calculate_default_transform(
            src_crs,
            dst_crs,
            width=w,
            height=h,
            left=left,
            bottom=bottom,
            right=right,
            top=top,
        )
        with rasterio.open(
            out_file,
            "w",
            driver="GTiff",
            width=width,
            height=height,
            count=1,
            crs=dst_crs,
            transform=transform_4326,
            dtype=rasterio.uint8,
            nodata=rasters[0].nodata,
        ) as dst:
            dst.write(np.squeeze(merged_img), 1)

        # delete temporary directory
        shutil.rmtree(outfolder)

        with self.output().open("w") as dst:
            dst.write(out_file)
