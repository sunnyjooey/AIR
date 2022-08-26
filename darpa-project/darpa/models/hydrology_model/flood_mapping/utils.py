import os

import numpy as np
import pandas as pd
import rasterio


def read_raster(raster_src):
    with rasterio.open(raster_src) as src:
        arr = src.read()
    return np.dstack(arr)


def get_data(df_src, img_dir, mask_dir):
    df = pd.read_csv(df_src, header=0, names=["img", "mask"])
    x_arr_list = []
    y_arr_list = []
    for _, row in df.iterrows():
        x = read_raster(os.path.join(img_dir, row["img"]))
        y = read_raster(os.path.join(mask_dir, row["mask"]))
        x_arr_list.append(x)
        y_arr_list.append(y)
    X = np.array(x_arr_list)
    y = np.array(y_arr_list)
    return X, y


def get_mean_std(X):
    band_1_mean = np.nanmean(X[:, :, :, 0])
    band_2_mean = np.nanmean(X[:, :, :, 1])
    band_1_std = np.nanstd(X[:, :, :, 0])
    band_2_std = np.nanstd(X[:, :, :, 1])
    return band_1_mean, band_2_mean, band_1_std, band_2_std


def scale_data(X, band_1_mean, band_2_mean, band_1_std, band_2_std):
    X[:, :, :, 0] -= band_1_mean
    X[:, :, :, 1] -= band_2_mean

    X[:, :, :, 0] /= band_1_std
    X[:, :, :, 1] /= band_2_std
    return X


def flood_mapping(tile, model_key, start_date, end_date, product_id):
    import tempfile
    import descarteslabs as dl
    import keras
    from keras.layers import Conv2D, Dropout, MaxPooling2D, Concatenate, UpSampling2D
    import numpy as np
    from shapely.geometry import shape

    def mini_unet(n_classes=1, input_height=512, input_width=512, channels=2):
        inputs = keras.Input(shape=(input_height, input_width, channels))

        conv1 = Conv2D(32, (3, 3), padding="same", activation="relu")(inputs)
        conv1 = Dropout(0.2)(conv1)
        conv1 = Conv2D(32, (3, 3), padding="same", activation="relu")(conv1)
        pool1 = MaxPooling2D((2, 2))(conv1)

        conv2 = Conv2D(64, (3, 3), activation="relu", padding="same")(pool1)
        conv2 = Dropout(0.2)(conv2)
        conv2 = Conv2D(64, (3, 3), activation="relu", padding="same")(conv2)
        pool2 = MaxPooling2D((2, 2))(conv2)

        conv3 = Conv2D(128, (3, 3), activation="relu", padding="same")(pool2)
        conv3 = Dropout(0.2)(conv3)
        conv3 = Conv2D(128, (3, 3), activation="relu", padding="same")(conv3)

        up1 = Concatenate()([UpSampling2D((2, 2))(conv3), conv2])
        conv4 = Conv2D(64, (3, 3), activation="relu", padding="same")(up1)
        conv4 = Dropout(0.2)(conv4)
        conv4 = Conv2D(64, (3, 3), activation="relu", padding="same")(conv4)

        up2 = Concatenate()([UpSampling2D((2, 2))(conv4), conv1])
        conv5 = Conv2D(32, (3, 3), activation="relu", padding="same")(up2)
        conv5 = Dropout(0.2)(conv5)
        conv5 = Conv2D(32, (3, 3), activation="relu", padding="same")(conv5)

        outputs = Conv2D(n_classes, (1, 1), activation="sigmoid", padding="same")(conv5)

        model = keras.Model(inputs, outputs)
        return model

    def convert_f_unitless_decibel(arr):
        # if arr.size > 0:
        # VV Polarization fraction. 0-255 is 0.0-0.5
        arr[..., 0] /= 510
        # VH Polarization fraction. 0-255 is 0.0-0.1
        arr[..., 1] /= 255
        # Convert to decibels
        arr = 10 * np.log(arr)
        return arr

    def get_imagery(tile, start_date=start_date, end_date=end_date):
        scenes, ctx = dl.scenes.search(
            tile["geometry"],
            products=["sentinel-1:GRD"],
            start_datetime=start_date,
            end_datetime=end_date,
        )
        raster_client = dl.Raster()
        # get raster stack only if scenes are found for the tile
        # if scenes:
        scene_ids = [s.properties["id"] for s in scenes]
        dltile_key = tile["properties"]["key"]
        #  raster client might make excessive parallel calls in the stack() , specify max_workers
        stack, raster_info = raster_client.stack(
            scene_ids,
            dltile=dltile_key,
            bands=["vv", "vh"],
            scales=None,
            max_workers=8,
            srs="epsg:4326",
            data_type="Float32",
        )
        try:
            nodata = raster_info[0]["bands"][0]["noDataValue"]
            stack = np.where(stack == nodata, np.nan, stack)
        except KeyError:
            print(tile["geometry"])
            print(start_date)
            print(end_date)
        # stack = np.where(stack == nodata, np.nan, stack)

        # else:
        #     stack = np.array([])
        return stack

    def predict_flooding(model, model_key, input_arr):
        temp = tempfile.NamedTemporaryFile(suffix=".h5")
        dl.Storage().get_file(model_key, temp.name)
        model.load_weights(temp.name)
        # if input_arr.size > 0:
        mean_vec = [-10.3977, -17.249363]
        std_vec = [4.043726, 4.759188]

        input_arr = input_arr.astype("float32")
        input_arr[..., 0] -= mean_vec[0]
        input_arr[..., 1] -= mean_vec[1]

        input_arr[..., 0] /= std_vec[0]
        input_arr[..., 1] /= std_vec[1]

        test_pred_arr = model.predict(input_arr, batch_size=64, verbose=1)
        test_pred_arr = np.nanmax(test_pred_arr, axis=0)
        test_pred_arr = np.squeeze(test_pred_arr)
        # else:
        #     test_pred_arr = np.array([])
        return test_pred_arr

    def upload_arr_to_dl_catalog(arr, tile, product_id, start_date):
        # if arr.size > 0:
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

    def check_if_detection_exist(tile, start_date, end_date, product_id):
        scenes, ctx = dl.scenes.search(
            aoi=tile,
            products=[product_id],
            start_datetime=start_date,
            end_datetime=end_date,
        )
        if scenes:
            return sum([round(i.coverage(shape(tile["geometry"])), 0) for i in scenes])
        else:
            return 0

    exist = check_if_detection_exist(tile, start_date, end_date, product_id)

    if exist == 0:
        arr = get_imagery(tile, start_date, end_date)
        arr = convert_f_unitless_decibel(arr)
        model = mini_unet(n_classes=1, input_height=512, input_width=512, channels=2)
        flood_arr = predict_flooding(model, model_key, arr)
        upload_arr_to_dl_catalog(flood_arr, tile, product_id, start_date)
    else:
        print("The tile exits")
