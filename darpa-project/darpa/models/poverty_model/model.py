# from urllib.parse import urlunparse, urlparse
# import json
from pathlib import Path

from kiluigi import Task, IntermediateTarget, FinalTarget
from luigi.format import Nop
from luigi.parameter import (
    NumericalParameter,
    IntParameter,
    DateParameter,
)
from luigi.date_interval import Date
from luigi.util import requires, inherits

import geopandas as gpd
import numpy as np
import pandas as pd
import datetime
import rasterio as rio
from rasterio.features import rasterize
from sklearn.decomposition import PCA
from sklearn.preprocessing import PowerTransformer
from tensorflow import keras as k, random
import xarray as xr

from .data import GetPredictors, GetTrainingData, GetAdminFeatures, GuessResponsePixels


@requires(GetTrainingData)
class Train(Task):

    seed = IntParameter(default=GuessResponsePixels().seed)
    predict_period = DateParameter(default=DateParameter().parse("2017-12-01"))

    def output(self):
        p = Path(*self.task_id.split("."))
        return {
            "model": IntermediateTarget(path=str(p / "model.h5"), format=Nop),
            "pca": IntermediateTarget(path=str(p / "pca.pickle")),
            "yeo": IntermediateTarget(path=str(p / "yeo.pickle")),
        }

    def run(self):
        random.set_seed(self.seed)  # FIXME: separate parameter?

        with self.input().open() as f:
            ds = f.read()

        # stack and split
        ds["location"] = np.arange(ds.sizes["location"])

        ds = (
            ds.stack(sample=["period", "location"])
            .dropna("sample", subset=["y0", "ya"])
            .transpose("sample", "var")
        )
        ds = ds.dropna(dim="sample")
        y0 = ds["y0"].load().data
        ya = ds["ya"].load().data

        # prediction periods before 2012 April will not have svdnb
        if self.predict_period < datetime.date(2012, 4, 1):
            num = ds["x"].drop_sel(var=("esacci", "svdnb")).load().data
        else:
            num = ds["x"].drop_sel(var=("esacci")).load().data
        yeo = PowerTransformer(method="yeo-johnson").fit(num)
        with self.output()["yeo"].open("w") as f:
            f.write(yeo)

        num_pow = yeo.transform(num)
        cat = ds["x"].sel(var=("esacci",)).load().data
        pca = PCA(5).fit(cat)
        with self.output()["pca"].open("w") as f:
            f.write(pca)
        cat = pca.transform(cat)

        ### this is the best result from hyper-parameter tuning
        ### with ax package (300 random sets of params), seed 1004
        ### cv loss was 0.9953203307283771
        PARAMS = {
            "learning_rate": 0.00021772028489022886,
            "dropout_rate": 0.3909687633368358,
            "num_hidden_layers": 7,
            "neurons_per_layer": 300,
            "batch_size": 128,
            "optimizer": "rms",
        }
        EPOCHS = 50

        # model inputs
        inputs = [
            k.layers.Input(shape=(num.shape[1],)),
            k.layers.Input(shape=(num_pow.shape[1],)),
            k.layers.Input(shape=(cat.shape[1],)),
        ]

        # model nodes
        nodes = [
            k.layers.BatchNormalization()(inputs[0]),
            inputs[1],
            inputs[2],
        ]
        nodes = k.layers.concatenate(nodes)

        for i in range(PARAMS["num_hidden_layers"]):
            nodes = k.layers.Dense(PARAMS["neurons_per_layer"], activation="relu")(
                nodes
            )
            nodes = k.layers.Dropout(PARAMS["dropout_rate"])(nodes)

        # model outputs
        # TODO: use the classification output in the second output, in some
        #  way that places all weight on the non-zero samples
        outputs = [
            k.layers.Dense(3, activation="softmax")(nodes),
            k.layers.Dense(2, activation="sigmoid")(nodes),
        ]

        # model configuration
        model = k.Model(inputs, outputs)
        if PARAMS["optimizer"] == "adam":
            model.compile(
                loss=["categorical_crossentropy", "mse"],
                optimizer=k.optimizers.Adam(learning_rate=PARAMS["learning_rate"]),
            )
        elif PARAMS["optimizer"] == "rms":
            model.compile(
                loss=["categorical_crossentropy", "mse"],
                optimizer=k.optimizers.RMSprop(learning_rate=PARAMS["learning_rate"]),
            )
        else:
            model.compile(
                loss=["categorical_crossentropy", "mse"],
                optimizer=k.optimizers.SGD(learning_rate=PARAMS["learning_rate"]),
            )

        # model fitting
        model.fit(
            [num, num_pow, cat],
            [
                k.utils.to_categorical(np.where((y0 > 0) & (y0 < 1), 2, y0)),
                np.stack([y0, ya], axis=1),
            ],
            epochs=EPOCHS,
            batch_size=PARAMS["batch_size"],
        )

        with self.output()["model"].open("w") as f:
            model.save(f.name, save_format="h5")


@requires(Train, GetPredictors)
class Predict(Task):

    threshold = NumericalParameter(
        default=0.5,
        min_value=0.0,
        max_value=1.0,
        var_type=float,
        description=(
            "Threshold value for predicted score of the above poverty-line "
            "(i.e. FGT=0) class. Scores below this threshold get a non-zero "
            "FGT prediction."
        ),
    )

    fldas = NumericalParameter(
        default=1.0,
        min_value=0.0,
        max_value=np.inf,
        var_type=float,
        description=(
            "This will multiply FLDAS data by the amount. "
            "Use this for testing the model. "
        ),
    )

    svdnb = NumericalParameter(
        default=1.0,
        min_value=0.0,
        max_value=np.inf,
        var_type=float,
        description=(
            "This will multiply SVDNB data by the amount. "
            "Use this for testing the model. "
        ),
    )

    def output(self):
        p = Path(*self.task_id.split("."))
        return {
            "FGT_0": IntermediateTarget(path=str(p / "fgt_0.tif")),
            "FGT_a": IntermediateTarget(path=str(p / "fgt_a.tif")),
        }

    def run(self):
        trained = self.input()[0]
        model = k.models.load_model(trained["model"].path)
        with trained["pca"].open() as f:
            pca = f.read()
        with trained["yeo"].open() as f:
            yeo = f.read()
        with self.input()[1].open() as f:
            ds = f.read()

        ### perturb the data for testing
        if self.fldas != 1 and "fldas" in GetPredictors().predictors.keys():
            # increase all fldas data
            period = "-".join(str(self.period).split("-")[:2])
            d1 = ds[(period, "fldas")]
            d1 = d1 * self.fldas
            ds[(period, "fldas")] = d1

        if self.svdnb != 1 and "svdnb" in GetPredictors().predictors.keys():
            if self.predict_period >= datetime.date(2012, 4, 1):
                # increase all fldas data
                period = "-".join(str(self.period).split("-")[:2])
                d1 = ds[(period, "svdnb")]
                d1 = d1 * self.svdnb
                ds[(period, "svdnb")] = d1

        ds = xr.concat([d for d in ds.values()], "var")
        # stack and split
        ds = ds.stack(location=["lat", "lon"])
        loc = ~ds.isnull().any("var").load()
        ds = ds.sel(location=loc).transpose("location", "var")

        num = ds.drop_sel(var=("esacci",)).load().data
        num_pow = yeo.transform(num)
        cat = ds.sel(var=("esacci",)).load().data
        cat = pca.transform(cat)

        # run model
        clsn, rgrn = model.predict([num, num_pow, cat])

        # zero-inflate the regression output
        clsn[:, 0] = np.where(
            clsn[:, 0] < self.threshold, 0, clsn[:, 0]
        )  # 0 - not poor needs to be above threshold to count as not poor
        clsn = clsn.argmax(axis=1)
        # y0 column
        rgrn[:, 0] = np.where(
            clsn == 0, 0, rgrn[:, 0]
        )  # if classified as 0 - not poor, regression is also 0 - not poor
        rgrn[:, 0] = np.where(
            clsn == 2, 1, rgrn[:, 0]
        )  # if classified as 2 - somewhat poor, regression is 1 - poor (not many cases)
        # ya column
        rgrn[:, 1] = np.where(
            clsn == 0, 0, rgrn[:, 1]
        )  # if classified as 0 - not poor, regression is also 0 - not poor
        ds = xr.DataArray(
            np.empty((loc.size, 2)),
            coords=loc.coords,
            dims=("location", "a"),
            attrs={k: ds.attrs[k] for k in ["crs", "transform"]},
        )
        ds[~loc] = np.nan
        ds[loc] = rgrn
        ds = ds.unstack("location")

        # write output to separate geotiffs
        profile = {
            "driver": "GTiff",
            "count": 1,
            "width": ds.sizes["lon"],
            "height": ds.sizes["lat"],
            "crs": ds.attrs["crs"],
            "transform": ds.attrs["transform"],
            "dtype": ds.dtype,
            "tiled": True,
            "compress": "deflate",
        }

        with self.output()["FGT_0"].open("w") as f:
            with rio.open(f.name, "w", **profile) as dst:
                dst.write(ds.isel(a=[0]).data)
        with self.output()["FGT_a"].open("w") as f:
            with rio.open(f.name, "w", **profile) as dst:
                dst.write(ds.isel(a=[1]).data)


@requires(Predict, GetAdminFeatures, GetPredictors)
class Aggregate(Task):
    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        ds = []
        da = xr.open_rasterio(self.input()[0]["FGT_0"].path, chunks={})
        da = da.sel(band=1, drop=True)
        da = da.rename({"y": "lat", "x": "lon"})
        da.name = "y0"
        ds.append(da)
        da = xr.open_rasterio(self.input()[0]["FGT_a"].path, chunks={})
        da = da.sel(band=1, drop=True)
        da = da.rename({"y": "lat", "x": "lon"})
        da.name = "ya"
        ds.append(da)

        # rasterize the admin boundaries
        gdf = gpd.read_file(self.input()[1].path).set_index("id", drop=True)
        da = xr.DataArray(
            data=rasterize(
                shapes=zip(gdf.geometry, gdf.index),
                out_shape=(da.sizes["lat"], da.sizes["lon"]),
                fill=np.nan,
                transform=da.attrs["transform"],
                dtype=np.dtype("float"),
            ),
            coords=da.coords,
            name="zone",
        )
        ds.append(da)

        # groupby weighted average
        with self.input()[2].open() as f:
            da = f.read()
        da = xr.concat([d for d in da.values()], "var")
        da = da.sel(var=("landscan", 1), drop=True).fillna(0)
        da.name = "weight"
        ds.append(da)
        ds = xr.merge(ds)
        ds = ds.groupby("zone").map(lambda _ds: _ds.weighted(_ds["weight"]).mean())
        ds = ds.drop("weight")
        ds.load()

        with self.output().open("w") as f:
            f.write(ds)


@inherits(Aggregate)
class Output(Task):
    def output(self):
        p = Path(*self.task_id.split("."))
        return FinalTarget(path=str(p.with_suffix(".csv")))

    def requires(self):
        fgt_a = self.fgt_a
        # FGT_0 is computed for any FGT_a, so only fgt_a is required and
        # this task may write only self.fgt_a
        if fgt_a == 0.0:
            fgt_a = 1.0

        # constrain prediction years to 2011 - 2017
        if self.predict_period > datetime.date(2017, 12, 31):
            predict_year = 2017
        elif self.predict_period < datetime.date(2011, 1, 1):
            predict_year = 2011
        else:
            predict_year = self.predict_period.year
        task = self.clone(
            Aggregate,
            period=Date(predict_year, self.predict_period.month, 1),
            fgt_a=fgt_a,
        )
        return [task, self.clone(GetAdminFeatures)]

    def run(self):
        with self.input()[0].open() as f:
            ds = f.read()
        gdf_location = self.input()[1]
        gdf = gpd.read_file(gdf_location.path)
        ds = ds.reindex({"zone": gdf["id"]})
        if self.fgt_a == 0:
            data = {"id": list(ds.zone.data), "output": list(ds.y0.data)}
        else:
            data = {"id": list(ds.zone.data), "output": list(ds.ya.data)}

        data = pd.DataFrame(data)
        data = pd.merge(data, gdf[["CC_3", "NAME_3"]], left_on="id", right_on="CC_3")
        data.drop("CC_3", axis=1, inplace=True)

        params = self.to_str_params()
        data.rename({"NAME_3": "admin3"}, axis=1, inplace=True)
        data["time"] = params["predict_period"]
        data["fgt_a"] = params["fgt_a"]
        data["threshold"] = params["threshold"]

        with self.output().open("w") as f:
            data.to_csv(f)
