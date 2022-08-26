import calendar
import datetime
import io
import logging
import pickle
import warnings

import geopandas as gpd
import luigi
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from keras.callbacks import EarlyStopping
from keras.layers import LSTM, Dense
from keras.models import Sequential, load_model
from kiluigi.targets import CkanTarget, FinalTarget, IntermediateTarget
from luigi import ExternalTask, Task
from luigi.configuration import get_config
from luigi.util import inherits, requires
from shapely.geometry import shape
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from models.hydrology_model.river_discharge.data_pre import (
    DischargeTrainingDataset,
    GetPredictors,
    PullUpstreamArea,
)
from models.hydrology_model.river_discharge.utils import (
    chunk_list,
    convert_flooding_tiff_json,
    data_t_lstm_format,
    mask_dataset,
)
from utils.geospatial_tasks.functions.geospatial import geography_f_country
from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

RELATIVE_PATH = "models/hydrology_model/river_discharge/tasks"

logger = logging.getLogger("luigi-interface")

config = get_config()

DEFAULT_PROFILE = {
    "driver": "GTiff",
    "interleave": "pixel",
    "tiled": True,
    "compress": "DEFLATE",
}


@requires(DischargeTrainingDataset)
class RiverDischargeLSTMModell(Task):
    """ Train Long Short-Term Memory networks (LSTM) model.
    """

    def output(self):
        return {
            "model": IntermediateTarget(
                f"{RELATIVE_PATH}/river_discharge_model.h5", timeout=31536000
            ),
            "scaler": IntermediateTarget(
                f"{RELATIVE_PATH}/scaler.sav", timeout=31536000
            ),
        }

    @staticmethod
    def reshape_data(arr, length):
        """
        Reshape data into 3D.

        Parameters
        ----------
        arr : numpy.array
            Data to be used to train the model.
        length : int
            The number of time steps to be used.

        Returns
        -------
        X : numpy.array
            Independent features.
        y : numpy.array
            Dependent features.

        """
        X = None
        y = None
        pixel_count = arr.shape[0]
        for pixel in range(arr.shape[0]):
            logger.debug(f"pixel {pixel} of {pixel_count}")
            label = []
            x = []
            for i in range(length, arr.shape[1]):
                label.append(arr[pixel][i, 0])
                start_row = i - length
                start_col = 1
                end_col = arr.shape[2]
                x.append(arr[pixel][start_row:i, start_col:end_col])
            label = np.array(label).reshape(arr.shape[1] - length, 1)
            x = np.array(x)
            if X is None:
                X = x.copy()
                y = label.copy()
            else:
                X = np.concatenate((X, x))
                y = np.concatenate((y, label))

        return X, y

    def run(self):
        with self.input().open() as src:
            data = src.read()

        X, y = self.reshape_data(data, 60)

        # Drop nodata and missing values
        X = np.delete(X, np.argwhere(y == -9999.0)[:, 0], axis=0)
        y = np.delete(y, np.argwhere(y == -9999.0)[:, 0], axis=0)

        features = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16]
        X = X[:, :, features]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.33, random_state=42
        )

        scaler = StandardScaler()

        X_train = scaler.fit_transform(X_train.reshape(-1, X_train.shape[-1])).reshape(
            X_train.shape
        )

        model = Sequential()
        model.add(
            LSTM(
                units=256,
                recurrent_dropout=0.2,
                return_sequences=True,
                dropout=0.1,
                input_shape=(X_train.shape[1], X_train.shape[2]),
            )
        )
        model.add(
            LSTM(units=256, recurrent_dropout=0.2, return_sequences=False, dropout=0.1)
        )
        model.add(Dense(units=15))
        model.add(Dense(units=1))
        model.compile(optimizer="adam", loss="mean_squared_error", metrics=["mae"])
        callbacks = [EarlyStopping(monitor="val_loss", patience=4)]
        model.fit(
            X_train,
            y_train,
            epochs=30,
            batch_size=256,
            validation_split=0.2,
            callbacks=callbacks,
        )

        X_test = scaler.transform(X_test.reshape(-1, X_test.shape[-1])).reshape(
            X_test.shape
        )
        y_pred = model.predict(X_test)
        r2 = r2_score(y_test, y_pred)
        logger.info(f"The R squared is {r2}")
        model.save(self.output()["model"].path)
        pickle.dump(scaler, open(self.output()["scaler"].path, "wb"))  # noqa: S301


class PullModelFromCkan(ExternalTask):
    """Pull pickled model and scaler from CKAN
    """

    def output(self):
        return {
            "model": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "87f73627-5177-4b17-9c69-520da06c5274"},
            ),
            "x_scaler": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "5db8ebe4-e64d-4101-b2ce-8d029cf77e1b"},
            ),
            "y_scaler": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "4319873c-0c0e-4b22-bd6f-dba63e43b0a6"},
            ),
        }


class PullRainfallScenarioFromCkan(ExternalTask):
    """Pull rainfall scenario from CKAN
    """

    def output(self):
        return {
            "low": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "ba7f4399-e741-40ee-b616-2fe36e191214"},
            ),
            "mean": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "0e458999-0413-45b2-8aae-8a92a19b028a"},
            ),
            "high": CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "2ebefa59-53b5-46a3-be8e-49861a18a1b7"},
            ),
        }


@requires(GetPredictors, PullRainfallScenarioFromCkan, PullModelFromCkan)
@inherits(GlobalParameters)
class PredictRiverDisharge(DischargeTrainingDataset):
    """Predict river discharge
    """

    def output(self):
        try:
            return FinalTarget(
                path="hydrology_model/river_discharge.nc", task=self, ACL="public-read"
            )
        except TypeError:
            return FinalTarget(path="hydrology_model/river_discharge.nc", task=self)

    @staticmethod
    def predict(data, date_list, model, x_scaler, y_scaler):
        """
        Predic river discharge function

        Parameters
        ----------
        data : xarray.Dataset
            Xarray dataset with feature required to predict discharge.
        date_list : list
            Time window for the prediction.
        model : model
            Trained model.
        x_scaler : scaler
            Independent features scaler.
        y_scaler : scaler
            Dependant feature scaler.

        Returns
        -------
        y : xarray.DataArray
            Predicted river discharge dataarray.

        """
        if len(date_list) > 1:
            temp = xr.concat(
                [data_t_lstm_format(data, date, 60) for date in date_list], dim="date"
            )
        else:
            temp = data_t_lstm_format(data, date_list[0], 60)

        temp = temp.stack(z=("y", "x", "date"))
        arr = temp[["dis", "t2m", "chirps_rain", "up_area", "dem"]].to_array()
        arr = arr.transpose("z", "time", "variable")
        arr = arr.values

        shape = arr.shape
        arr = arr.reshape(-1, arr.shape[-1])
        arr = (arr - x_scaler.mean_) / x_scaler.var_ ** 0.5
        arr = arr.reshape(shape)
        # arr = x_scaler.transform(arr.reshape(-1, arr.shape[-1])).reshape(arr.shape)
        y = model.predict(arr)
        # y = y_scaler.inverse_transform(y)
        y = (y * y_scaler.var_ ** 0.5) + y_scaler.mean_
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            y[y < 0] = 0
        y = np.squeeze(y)
        z = temp["z"]
        y = xr.DataArray(y, coords=[z], dims=["z"])
        return y

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            geom = [geography]

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")

            with open(self.input()[2]["x_scaler"].path, "rb") as src:
                scaler = pickle.load(src)  # noqa: S301

            model = load_model(self.input()[2]["model"].path)

            with open(self.input()[2]["y_scaler"].path, "rb") as src:
                scaler_y = pickle.load(src)  # noqa: S301

        with self.input()[0].open() as src:
            src_byte = src.read()
            data = xr.open_dataset(io.BytesIO(src_byte))

        data = self.apply_rain_scenario(data, geom)
        data = self.apply_temp_scenario(data)
        date_list = pd.date_range(
            self.time.date_a, (self.time.date_b - datetime.timedelta(1))
        )
        logger.info("Predicting river disharge this might take a while")
        if len(date_list) <= 60:
            y = self.predict(data, date_list, model, scaler, scaler_y)
        else:
            # Predict chunk of dates to avoid memory error
            date_list = chunk_list(date_list, 60)
            y = xr.concat(
                [self.predict(data, i, model, scaler, scaler_y) for i in date_list], "z"
            )

        y = y.unstack("z")
        y = y.transpose("date", "y", "x")
        y = y.to_dataset(name="dis")
        with self.output().open("w") as out:
            y.to_netcdf(out.name)

    def apply_rain_scenario(self, data, geom):
        """
        Apply rainfall scenario.

        Parameters
        ----------
        data : xarray.dataset
            Predictors dataset.
        geom : list
            Spatial extent of the inputs.

        Returns
        -------
        data : xarray.dataset
            A dataset with rainfall scenario applied.

        """
        # FIXME: Take into account rainfall geography
        if self.rainfall_scenario == "normal":
            return data
        else:
            da = xr.open_dataset(self.input()[1][self.rainfall_scenario].path)
            sce_start = self.rainfall_scenario_time.date_a
            sce_end = self.rainfall_scenario_time.date_b - datetime.timedelta(1)
            day_f_year_list = [
                i.timetuple().tm_yday for i in pd.date_range(sce_start, sce_end)
            ]
            da = da.sel(dayofyear=day_f_year_list)
            da = mask_dataset(da, geom)
            dis_da = data["dis"]
            da = da.rio.reproject_match(dis_da)
            sce_arr = da["chirps_rain"].values
            data["chirps_rain"].loc[dict(time=slice(sce_start, sce_end))] = sce_arr
            return data

    def apply_temp_scenario(self, data):
        """
        Apply temperature scenario.

        Parameters
        ----------
        data : xarray.dataset
            Predictors datasetset.

        Returns
        -------
        data : xarray.dataset
            Predictors datasetset with applied temperature scenario.

        """
        # FIXME: Take into account rainfall geography
        if self.temperature_scenario == 0:
            return data
        else:
            sce_start = self.temperature_scenario_time.date_a
            sce_end = self.temperature_scenario_time.date_b - datetime.timedelta(1)
            sce_arr = data["t2m"].sel(time=slice(sce_start, sce_end)).data
            sce_arr = np.where(
                np.isnan(sce_arr), np.nan, sce_arr + self.temperature_scenario
            )
            data["t2m"].loc[dict(time=slice(sce_start, sce_end))] = sce_arr
            return data


@requires(PredictRiverDisharge)
class RiverDischargeToParquet(Task):
    """Convert river discharge netcdf to parquet format.
    """

    def output(self):
        try:
            final_target = FinalTarget(
                path=f"hydrology_model/river_discharge_{self.task_id}.parquet.gzip",
                format=luigi.format.Nop,
                ACL="public-read",
            )
        except TypeError:
            final_target = FinalTarget(
                path=f"hydrology_model/river_discharge_{self.task_id}.parquet.gzip",
                format=luigi.format.Nop,
            )
        return final_target

    def run(self):
        with self.input().open() as src:
            src_byte = src.read()
            ds = xr.open_dataset(io.BytesIO(src_byte))
        ds = ds.to_dataframe()
        ds = ds.reset_index()
        ds = ds.rename(columns={"x": "lon", "y": "lat", "dis": "river_discharge"})
        ds["rainfall_scenario"] = self.rainfall_scenario
        with self.output().open("w") as out:
            ds.to_parquet(out.name, compression="gzip")


class PullFloodThresholdFromCkan(ExternalTask):
    """Pull flood threhold from CKAN.
    """

    def output(self):
        return {
            5: CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "07c05dc9-1752-4dad-95e4-744a7fbf8944"},
            ),
            10: CkanTarget(
                dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
                resource={"id": "d8a15f3f-08ac-438c-b91d-87d948f6bfee"},
            ),
        }


class PullFloodHazardMapFromCkan(ExternalTask):
    """Pull flood hazard map from CKAN.
    """

    def output(self):
        return CkanTarget(
            dataset={"id": "f25f2152-e999-415f-956f-3a7c2eb2b723"},
            resource={"id": "d7c2bb4a-a081-48bd-bf0b-344dac8e2583"},
        )


@requires(
    PredictRiverDisharge,
    PullFloodThresholdFromCkan,
    PullFloodHazardMapFromCkan,
    PullUpstreamArea,
)
class RiverFlooding(Task):
    """Estimate riverine flooding.

    If the predicted river discharge exceed flood threshold and the basin is
    greater than 5,000 km2, the pixel is identified as flooded.
    The role of flood defences is not considered.
    """

    downscale = luigi.BoolParameter(default=False)

    def output(self):
        try:
            return FinalTarget(
                path="hydrology_model/flood_extent.nc", task=self, ACL="public-read"
            )
        except TypeError:
            return FinalTarget(path="hydrology_model/flood_extent.nc", task=self)

    @staticmethod
    def read_raster(f_src, geom):
        """
        Read raster file

        Parameters
        ----------
        f_src : str
            file path.
        geom : list
            Geometry list object.

        Returns
        -------
        da : xarray.DataArray
            Masked xarray.DataArray.

        """
        da = xr.open_rasterio(f_src)
        da = da.squeeze(dim="band")
        da = mask_dataset(da, geom)
        da = da.where(da != da.rio.nodata)
        return da

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            mask_geom = [i["geometry"] for i in geography["features"]]
        except KeyError:
            mask_geom = [geography]

        with self.input()[0].open() as src:
            dis_file = src.read()

        dis_ds = xr.open_dataset(io.BytesIO(dis_file))

        dis_da = dis_ds["dis"]
        dis_da.rio.write_crs({"init": "epsg:4326"}, inplace=True)
        dis_arr = dis_da.values

        # Flood threshold
        thres_da = self.read_raster(
            self.input()[1][self.return_period_threshold].path, mask_geom
        )
        thres_da = thres_da.rio.reproject_match(dis_da)
        thres_arr = thres_da.values

        # Upstream area
        area_da = self.read_raster(self.input()[3].path, mask_geom)
        area_da = area_da.rio.reproject_match(dis_da)
        area_arr = area_da.values
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            flood_arr = np.where(
                np.isnan(dis_arr),
                np.nan,
                np.where((dis_arr > thres_arr) & (area_arr > 5000000000), 1, 0),
            )

        flood_arr = flood_arr.astype("float32")
        flood_da = xr.DataArray(flood_arr, dims=dis_da.dims, coords=dis_da.coords)
        flood_da.rio.write_crs({"init": "epsg:4326"}, inplace=True)

        # Down-scale flood extent
        if self.downscale:
            hazard_da = self.read_raster(self.input()[2].path, mask_geom)
            flood_da = flood_da.rio.reproject_match(hazard_da)
            flood_da = flood_da.where(flood_da != flood_da.rio.nodata)
            hazard_arr = hazard_da.values
            flood_arr = flood_da.values
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                flood_arr = np.where(
                    np.isnan(flood_arr),
                    np.nan,
                    np.where((flood_arr == 1) & (hazard_arr > 0), 1, 0),
                )
            flood_da.data = flood_arr
        flood = flood_da.to_dataset(name="flood")
        with self.output().open("w") as out:
            flood.to_netcdf(out.name)


@requires(RiverFlooding)
class RiverFloodingToParquet(Task):
    """
    Convert river flooding netcdf to parquet.
    """

    def output(self):
        try:
            final_target = FinalTarget(
                path=f"hydrology_model/river_flooding_{self.task_id}.parquet.gzip",
                format=luigi.format.Nop,
                ACL="public-read",
            )
        except TypeError:
            final_target = FinalTarget(
                path=f"hydrology_model/river_flooding_{self.task_id}.parquet.gzip",
                format=luigi.format.Nop,
            )
        return final_target

    def run(self):
        with self.input().open() as src:
            src_byte = src.read()
        ds = xr.open_dataset(io.BytesIO(src_byte))
        ds = ds.to_dataframe()
        ds = ds.reset_index()
        ds = ds.rename(columns={"x": "lon", "y": "lat"})
        try:
            ds = ds.drop("spatial_ref", axis=1)
        except KeyError:
            pass
        ds["flood_threshold_scenario"] = self.return_period_threshold
        with self.output().open("w") as out:
            ds.to_parquet(out.name, compression="gzip")


@requires(RiverFlooding)
@inherits(GlobalParameters)
class MonthlyRiverFloodingTiff(Task):
    """Identify all pixels that have flooded in a given month.
     """

    def output(self):
        try:
            out_map = {
                i: FinalTarget(
                    f"hydrology_model/flood_{i}.tif", task=self, ACL="public-read"
                )
                for i in self.get_month()
            }
        except TypeError:
            out_map = {
                i: FinalTarget(f"hydrology_model/flood_{i}.tif", task=self)
                for i in self.get_month()
            }

        return out_map

    def run(self):
        with self.input().open() as src:
            src_byte = src.read()
        ds = xr.open_dataset(io.BytesIO(src_byte))
        da = ds["flood"]
        meta = {"driver": "GTiff", "dtype": "float32", "nodata": -9999.0, "count": 1}
        meta.update(
            crs=da.rio.crs,
            transform=da.rio.transform(),
            width=da.rio.width,
            height=da.rio.height,
        )
        for month, target in self.output().items():
            start, end = self.get_month_day_range(month)
            da_sel = da.sel(date=slice(start, end))
            da_sel = da_sel.sum(dim="date")
            arr = da_sel.values
            arr = np.where(arr > 1, 1, arr)
            start_str = start.strftime("%Y-%m-%d")
            with target.open("w") as out:
                with rasterio.open(out, "w", **meta) as dst:
                    dst.write(arr.astype(meta["dtype"]), 1)
                    dst.update_tags(Time=start_str)

    def get_month(self):
        date_list = pd.date_range(self.time.date_a, self.time.date_b, freq="M")
        return {i.strftime("%Y_%m") for i in date_list}

    def get_month_day_range(self, month):
        first_date = datetime.datetime.strptime(month, "%Y_%m").date()
        last_date = first_date.replace(
            day=calendar.monthrange(first_date.year, first_date.month)[1]
        )
        limit = self.time.date_b - datetime.timedelta(1)
        if last_date > limit:
            last_date = limit
        return first_date, last_date


@requires(MonthlyRiverFloodingTiff)
class ConvertMonthlyFloodingTiffToGeoJSON(Task):
    """Convert monthly flooding tiffs to GeoJSON.
     """

    def output(self):
        dst_file = f"hydrology_model/monthly_flooding_{self.rainfall_scenario}_rain_{self.temperature_scenario}_temp.geojson"
        try:
            return FinalTarget(
                dst_file, task=self, format=luigi.format.Nop, ACL="public-read"
            )
        except TypeError:
            return FinalTarget(dst_file, task=self, format=luigi.format.Nop)

    def run(self):
        data_map = self.input()
        json_data = []
        for month, src_target in data_map.items():
            first_date, last_date = self.get_month_day_range(month)
            temp = convert_flooding_tiff_json(src_target.path, first_date, last_date)
            json_data.extend(temp)
        out_json = {"type": "FeatureCollection", "features": json_data}
        gdf = gpd.GeoDataFrame.from_features(out_json)
        if gdf.empty:
            geography = geography_f_country(self.country_level)
            try:
                boundary_geo = shape(geography["features"][0]["geometry"])
            except KeyError:
                boundary_geo = shape(geography)
            gdf = gpd.GeoDataFrame(index=[0], geometry=[boundary_geo])
        with self.output().open("w") as out:
            gdf.to_file(out.name, driver="GeoJSON")

    def get_month_day_range(self, date_str):
        date = datetime.datetime.strptime(date_str, "%Y_%m")
        first_date = date.replace(day=1).strftime("%Y-%m-%d")
        last_day = calendar.monthrange(date.year, date.month)[1]
        last_date = date.replace(day=last_day).strftime("%Y-%m-%d")
        return first_date, last_date
