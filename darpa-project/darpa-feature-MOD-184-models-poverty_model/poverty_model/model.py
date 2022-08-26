from urllib.parse import urlunparse, urlparse
import json
from pathlib import Path

from kiluigi import Task, IntermediateTarget, FinalTarget
from luigi.format import Nop
from luigi.parameter import NumericalParameter
from luigi.util import requires, inherits
from rasterio.features import rasterize
from tensorflow import keras as k, random
from sklearn.preprocessing import PowerTransformer
from sklearn.decomposition import PCA
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import xarray as xr

from .data import GetPredictors, GetTrainingData, GetAdminFeatures


def serializable(obj):
    """helper for json.dump to make certain objects serializable"""

    if hasattr(obj, "__geo_interface__"):
        return obj.__geo_interface__
    if isinstance(obj, FinalTarget):
        parts = list(urlparse(obj.path))
        parts[0] = "https"
        parts[1] = f"{parts[1]}.s3.amazonaws.com"
        return {"@id": urlunparse(parts)}
    return str(obj)


@requires(GetTrainingData)
class Train(Task):
    """
    fit a ML model to the training data
    """

    def output(self):
        p = Path(*self.task_id.split("."))
        return {
            "model": IntermediateTarget(path=str(p / "model.h5"), format=Nop),
            "pca": IntermediateTarget(path=str(p / "pca.pickle")),
            "yeo": IntermediateTarget(path=str(p / "yeo.pickle")),
        }

    def run(self):

        random.set_seed(23456)  # FIXME: to parameter, w/o conflict

        # load data
        with self.input().open() as f:
            ds = f.read()

        # stack and split
        ds["location"] = np.arange(ds.sizes["location"])
        ds = (
            ds.stack(sample=["period", "location"])
            .dropna("sample", subset=["y0", "ya"])
            .transpose("sample", "var")
        )
        y0 = ds["y0"].load().data
        ya = ds["ya"].load().data
        num = ds["x"].drop_sel(var=("esacci",)).load().data
        yeo = PowerTransformer(method="yeo-johnson").fit(num)
        with self.output()["yeo"].open("w") as f:
            f.write(yeo)
        num_pow = yeo.transform(num)
        cat = ds["x"].sel(var=("esacci",)).load().data
        pca = PCA(5).fit(cat)
        with self.output()["pca"].open("w") as f:
            f.write(pca)
        cat = pca.transform(cat)

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
        nodes = k.layers.Dense(64, activation="relu")(nodes)
        nodes = k.layers.Dropout(0.2)(nodes)
        nodes = k.layers.Dense(64, activation="relu")(nodes)
        nodes = k.layers.Dropout(0.2)(nodes)
        nodes = k.layers.Dense(64, activation="relu")(nodes)
        nodes = k.layers.Dropout(0.2)(nodes)

        # model outputs
        # TODO: use the classification output in the second output, in some
        #  way that places all weight on the non-zero samples
        outputs = [
            k.layers.Dense(3, activation="softmax")(nodes),
            k.layers.Dense(2, activation="sigmoid")(nodes),
        ]

        # model configuration
        model = k.Model(inputs, outputs)
        model.compile(
            loss=["categorical_crossentropy", "mse"], optimizer="RMSprop",
        )

        # model fitting
        model.fit(
            [num, num_pow, cat],
            [
                k.utils.to_categorical(np.where((y0 > 0) & (y0 < 1), 2, y0)),
                np.stack([y0, ya], axis=1),
            ],
            epochs=20,  # batch_size=1024,
        )

        with self.output()["model"].open("w") as f:
            model.save(f.name, save_format="h5")

        # # for validation
        # clsn, rgrn = model.predict([num, num_pow, cat])
        # with open("output/y_hat.pickle", "wb") as f:
        #     pickle.dump([clsn, rgrn], f)


@requires(Train, GetPredictors)
class Predict(Task):
    """
    run the ML model predict method over the desired spatio-temporal grid
    """

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
        clsn[:, 0] = np.where(clsn[:, 0] < self.threshold, 0, clsn[:, 0])
        clsn = clsn.argmax(axis=1)
        rgrn[:, 0] = np.where(clsn == 0, 0, rgrn[:, 0])
        rgrn[:, 0] = np.where(clsn == 2, 1, rgrn[:, 0])
        rgrn[:, 1] = np.where(clsn == 0, 0, rgrn[:, 1])
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
    """
    aggregate to the desired spatio-temporal units
    """

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
    """
    create structured output for indicator dataset recognizable by a dashboard
    """

    def output(self):
        p = Path(*self.task_id.split("."))
        return FinalTarget(path=str(p.with_suffix(".json")))

    def requires(self):

        fgt_a = self.fgt_a
        # FGT_0 is computed for any FGT_a, so only fgt_a is required and
        # this task may write only self.fgt_a
        if fgt_a == 0.0:
            fgt_a = 1.0
        periods = pd.period_range(self.period.date_a, self.period.date_b, freq="M")
        param = dict(Aggregate.get_params())["period"]
        tasks = {}
        for period in periods:
            tasks[str(period)] = self.clone(
                Aggregate, period=param.parse(f"{period}-01"), fgt_a=fgt_a,
            )

        return [tasks, self.clone(GetAdminFeatures)]

    def run(self):

        # combine aggregrates for each period
        da = []
        for period, target in self.input()[0].items():
            with target.open() as f:
                ds = f.read()
            if self.fgt_a == 0:
                _da = ds["y0"]
            else:
                _da = ds["ya"]
            _da["period"] = period
            da.append(_da)
        da = xr.concat(da, "period").transpose("zone", "period")

        # cast to dictionary
        location = self.input()[1]
        gdf = gpd.read_file(location.path)
        da = da.reindex({"zone": gdf["id"]})
        da = da.drop_vars("zone").rename({"zone": "location"})
        dd = da.to_dict()
        dd["coords"]["location"] = location

        # augment metadata
        # TODO: choose a standard (e.g. cf-json, STAC datacube) and use it, at
        #  present, the output is a mash-up of json-ld and xarray's default
        #  dictionary output (which is like cf-json, but not equivalent to).
        dd.pop("name")
        dd["attrs"]["name"] = "kidomain.poverty"
        dd["attrs"]["description"] = (
            "Proof-of-concept for gridded poverty estimation trained on HCES "
            "survey data."
        )
        dd["attrs"]["parameters"] = self.to_str_params()
        dd.update(serializable(self.output()))
        with self.output().open("w") as f:
            json.dump(dd, f, default=serializable)
