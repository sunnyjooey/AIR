from dateutil.relativedelta import relativedelta
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from zipfile import ZipFile

from affine import Affine
from kiluigi.targets import IntermediateTarget, FinalTarget, CkanTarget
from kiluigi.tasks import Task, ExternalTask
from luigi import (
    Parameter,
    IntParameter,
    DictParameter,
    DateIntervalParameter,
    ChoiceParameter,
    NumericalParameter,
)
from luigi.date_interval import Date
from luigi.format import Nop
from luigi.util import requires, inherits
from pyreadstat import read_dta
from rasterio.crs import CRS
from rasterio.features import rasterize
from rasterio.shutil import copy as rio_copy
from rasterio.warp import Resampling
from rasterio.windows import Window, from_bounds
from rasterio.vrt import WarpedVRT
from shapely.geometry import Polygon
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import xarray as xr

from utils import cog_tasks


class WarpPredictor(Task):
    """
    retrieve object from warehouse and warp to reference grid
    """

    path = Parameter()
    vrt_options = DictParameter()

    def output(self):
        return IntermediateTarget(task=self, format=Nop)

    def run(self):

        vrt_options = dict(self.vrt_options)
        vrt_options.update(
            {
                "crs": CRS.from_epsg(vrt_options["crs"]),
                "transform": Affine.from_gdal(*vrt_options["transform"]),
                "resampling": Resampling.bilinear,
            }
        )

        tmp = NamedTemporaryFile(suffix=".tif")

        with rio.open(self.path) as src:
            profile = src.profile
            tags = src.tags()
            # cast the the esacci lc dataset into binary bands for each class
            if "lccs_class#flag_values" in tags:
                flag_values = [
                    int(flag)
                    for flag in tags["lccs_class#flag_values"].strip("{}").split(",")
                ]
                with WarpedVRT(src, **vrt_options) as vrt:
                    bbox = vrt.bounds
                win = from_bounds(*bbox, src.transform)
                win = win.round_offsets("floor").round_shape("ceil")
                buffer = zip([-25, -50, 50, 100], win.flatten())
                win = Window(*[max(0, b + v) for b, v in buffer])
                x = src.read(window=win)
                profile.update(
                    {
                        "count": len(flag_values),
                        "transform": src.window_transform(win),
                        "height": win.height,
                        "width": win.width,
                        "dtype": "float32",
                        "nodata": None,
                    }
                )
                path = tmp.name
                with rio.open(path, "w", **profile) as dst:
                    for band, flag in enumerate(flag_values, start=1):
                        dst.write((x[0] == flag).astype(profile["dtype"]), band)
            else:
                path = self.path

        # copy the warped window to output
        with rio.open(path) as src, self.output().open("w") as dst:
            profile.update(**vrt_options)
            with WarpedVRT(src, **profile) as vrt:
                rio_copy(
                    vrt,
                    dst.name,
                    driver="GTiff",
                    TILED="YES",
                    COPY_SRC_OVERVIEWS="NO",
                    COMPRESS="DEFLATE",
                )

        tmp.close()


class PullResponse(ExternalTask):
    def output(self):
        return CkanTarget(
            dataset={"id": "ccb394c3-9eb4-4b3b-8f79-9f75a1fd1e46"},
            resource={"id": "c2fdbc60-e2ec-4f90-86ab-642f7c4833d9"},
        )


@requires(PullResponse)
class ExtractResponse(Task):
    def output(self):

        f = [
            "HCES_2011/Data/sim_1_2011.dta",
            "HCES_2016/Data/sim_1_v2(2).dta",
            "geolocalisation/ETH_Kebele_shapefiles.zip",
        ]
        p = Path(*self.task_id.split("."))

        return {_f: IntermediateTarget(path=str(p / _f), format=Nop) for _f in f}

    def run(self):

        src = ZipFile(self.input().path)
        for k, v in self.output().items():
            p = Path(v.path).parent
            p.mkdir(exist_ok=True, parents=True)
            if k.startswith("geolocalisation"):
                m = filter(lambda x: Path(x).stem == "Eth_Kebeles", src.namelist())
                with TemporaryDirectory() as tmp:
                    src.extractall(tmp, m)
                    p = Path(tmp)
                    with ZipFile(v.path, "w") as dst:
                        for f in p.glob("**/*.*"):
                            dst.write(f, f.name)
            else:
                info = src.getinfo(f"ETH_PSNP_EWS_Kimetrica_Data/{k}")
                info.filename = Path(info.filename).name
                src.extract(info, p)


@requires(ExtractResponse)
class GetResponse(Task):
    """
    combine the raw HCES data provided by WB into a merged dataset with
    a controlled vocabulary and standardized variables
    """

    train_location = DictParameter(
        default={
            "id": "ET",
            "type": "Feature",
            "geometry": None,
            "bbox": [33.0, 3.3, 48.0, 14.9],
        },
        description=(
            "A GeoJSON Feature specifying the spatial extent to query for "
            "training data. If the geometry is null, then a bbox must be "
            "specified."
        ),
    )
    # TODO: filter training data on the train_location parameter (as possible)
    train_period = DateIntervalParameter(
        default=DateIntervalParameter().parse("2011-01-01-2018-12-31"),
        description=(
            "A start and stop date specifying the temporal extent to query"
            "for training data, inclusive of both dates."
        ),
    )
    # TODO: filter training data on the train_period parameter

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):

        ds_ea_year = []
        cols = [
            "admin4",
            "ur",
            "year",
            "month",
            "weight",
            "y",
        ]

        # pattern of "RK_CODE" or "UK_ID" in feature collection
        def admin4(row):
            if row.cq11 == 14:
                s = "{cq11}01011{cq15:02}{cq16:03}"
            elif row.cq14 != 8:
                s = "{cq11}{cq12:02}{cq13:02}{cq14:02}"
            else:
                s = "{cq11}{cq12:02}{cq13:02}{cq16:03}"
            # float b/c id will be burned into a raster and int32 overflows
            return float(s.format(**row.to_dict()))

        # HCES_2011
        ds, dd = read_dta(self.input()["HCES_2011/Data/sim_1_2011.dta"].path)
        ds.loc[ds["hh_id"] == "3441111.", "hh_id"] = "34411116"
        # correction deduced by what's missing
        ds["hh_id"] = ds["hh_id"].str.lstrip("0").astype("int")
        ds = ds.groupby("hh_id").first()
        # TODO: understand why there were a few hundred duplicates
        ds["admin4"] = ds.apply(admin4, axis=1)
        ds = ds.to_xarray()
        # TODO: confirm year
        ds["year"] = 2011.0
        # TODO: confirm month, weight
        ds = ds.rename(month="month", popwgt="weight", UR="ur",)
        # TODO: confirm consumption expenditure and poverty line
        poverty_line = 3775.0
        ds["y"] = (poverty_line - ds["TOTALDEXPPERAE"]).clip(0) / poverty_line
        # Normalized difference between a poverty line (3775 bir?) deduced
        # from the `poor_WB` and `TOTALDEXPPERAE` columns, and the
        # expenditure column `TOTALDEXPPERAE`.
        ds_ea_year.append(ds[cols])

        # HCES_2016
        ds, dd = read_dta(self.input()["HCES_2016/Data/sim_1_v2(2).dta"].path)
        ds["hh_id"] = ds["hh_id"].str.lstrip("0").astype("int")
        ds = ds.set_index("hh_id", verify_integrity=True)
        ds["admin4"] = ds.apply(admin4, axis=1)
        ds = ds.to_xarray()
        # TODO: confirm year
        ds["year"] = 2016.0
        # TODO: confirm mongth, weight
        ds = ds.rename(month="month", wgtpop="weight",)
        # TODO: confirm consumption expenditure and poverty line
        poverty_line = 7436.0
        ds["y"] = (poverty_line - ds["rpatexpend"]).clip(0) / poverty_line
        # Normalized difference between a poverty line (7436) noted in the
        # `poor_WB` label and the expenditure column `rpatexpend`, deduced
        # by comparison against `poor_WB` result.
        ds_ea_year.append(ds[cols])

        # concatenate (not stacking, as there is no indication HHs are
        # surveyed at multiple times)
        ds = xr.concat(ds_ea_year, dim="hh_id")

        ds["admin4"] = ds["admin4"].where(ds["admin4"] != 3110101, 3111101)
        # 3 11 01 01 -> 3 11 11 01 (nothing else under 311 Z_CODE)
        ds["admin4"] = ds["admin4"].where(ds["admin4"] != 1060101, 1060201)
        # 1 06 01 01 -> 1 06 02 01 (nothing else under 106 Z_CODE)
        ds["admin4"] = ds["admin4"].where(ds["admin4"] != 15010101, 15010201)
        # 15 01 01 01 -> 15 01 02 01 (nothing has this valid RK_CODE)
        # TODO: confirm these substitutions made to match shapefile

        with self.output().open("w") as f:
            f.write(ds)


@requires(ExtractResponse)
class GetAdminFeatures(Task):

    location = DictParameter(
        default=GetResponse().train_location,
        description=(
            "A GeoJSON Feature specifying the spatial extent of the returned "
            'dataset. The "id" key may contain a valid ISO 3166-1 alpha-2 '
            'or 3166-2 code. The "geometry" may be empty if a "bbox" is '
            "provided."
        ),
    )
    admin_level = ChoiceParameter(choices=[1, 2, 3, 4], default=4, var_type=int)

    def output(self):
        p = Path(*self.task_id.split("."))
        return FinalTarget(path=str(p.with_suffix(".geojson")))

    def run(self):

        if self.location.get("id") != "ET":
            raise NotImplementedError

        # load feature collection, dropping somale killil
        shp = self.input()["geolocalisation/ETH_Kebele_shapefiles.zip"].path
        gdf = gpd.read_file(f"zip://{shp}")
        gdf = gdf.query("R_CODE != 5")
        gdf = gdf.to_crs(CRS.from_epsg(4326))

        # match column names
        if self.admin_level == 4:
            gdf["id"] = gdf["RK_CODE"]
            loc = gdf["RK_CODE"] == 0
            gdf.loc[loc, "id"] = pd.to_numeric(gdf.loc[loc, "UK_ID"])
        elif self.admin_level == 3:
            gdf["id"] = gdf["W_CODE"]
            loc = gdf["W_CODE"] == 0
            gdf.loc[loc, "id"] = gdf.loc[loc].apply(
                lambda x: int(f"{x['R_CODE']}{x['Z_NAME']}{x['KK_CODE']:03}"), axis=1,
            )
        elif self.admin_level == 2:
            gdf["id"] = gdf["Z_CODE"]
            loc = gdf["Z_CODE"].isin([1, 5])
            # Z_CODE of 5 appears to be a mistake, use Z_NAME
            gdf.loc[loc, "id"] = pd.to_numeric("14" + gdf.loc[loc, "Z_NAME"])
        elif self.admin_level == 1:
            gdf["id"] = gdf["R_CODE"]

        # remove extraneous columns
        cols = ["OBJECTID", "COUNT", "GlobalID"]
        if self.admin_level < 4:
            cols.extend(["RK_CODE", "RK_NAME", "UK_ID", "UK_CODE", "UK_NAME"])
        if self.admin_level < 3:
            cols.extend(["W_CODE", "W_NAME", "KK_CODE", "KK_NAME"])
        if self.admin_level < 2:
            cols.extend(["Z_CODE", "Z_NAME", "T_CODE", "T_NAME"])
        gdf = gdf.drop(columns=cols)

        # dissolve internal boundaries
        gdf = gdf.dissolve("id")
        # admin4 dissolve merges 2 nearby polygons having the same code

        # cleanup result of disjoint "borders" (thanks, ESRI)
        # FIXME: do better than simply dropping interiors, but dealing with
        #  the crappy dissolve will take serious shapely chops!
        gdf = gdf.explode()
        gdf["geometry"] = gdf["geometry"].apply(
            lambda g: Polygon(
                g.exterior, [i for i in g.interiors if i.convex_hull.area > 0.01]
            )
        )
        gdf = gdf.reset_index(level=0).dissolve("id")
        gdf = gdf.reset_index()

        with self.output().open("w") as f:
            f.write(gdf.to_json())


@inherits(GetAdminFeatures)
class GetPredictors(Task):
    """
    make predictor variables available over the entire prediction domain
    """

    # TODO: allow choice of bands

    period = DateIntervalParameter(
        default=GetResponse().train_period,
        description=(
            # note that calls to GetPredictors itself only use start date
            "A start and stop date specifying the temporal "
            "extent, inclusive of both dates, for the returned dataset."
        ),
    )
    predictors = DictParameter(
        default={
            "esacci": ["lc"],
            "fldas": [
                "Evap_tavg",
                "Qtotal_tavg",
                "Rainf_f_tavg",
                "SoilMoi00_10cm_tavg",
                "SoilMoi10_40cm_tavg",
                "SoilMoi40_100cm_tavg",
                "SoilMoi100_200cm_tavg",
                "Tair_f_tavg",
            ],
            "svdnb": ["avg_rade9h"],
            "landscan": ["lspop"],
        }
    )
    reference_grid = Parameter(default="landscan")

    def output(self):
        return IntermediateTarget(task=self)

    def requires(self):

        # get a bounding box on which to crop predictors
        if "bbox" in self.location:
            bbox = self.location["bbox"]
        elif self.location["type"] == "FeatureCollection":
            gdf = gpd.GeoDataFrame.from_features(
                self.location["features"], crs="epsg:4326"
            )
            bbox = gdf.total_bounds
        else:
            raise TypeError("dict must be a `FeatureCollection` or include " "a bbox")

        # match data from warehouse to reference grid over extent
        targets = {}
        for source, bands in self.predictors.items():
            module = getattr(cog_tasks, source)
            date = self.period.date_a
            wrapper = module.MigrateToWarehouse.from_str_params({"period": f"{date}"})
            target = None
            while not target:
                try:
                    target = wrapper.input().pop()
                except Exception as e:
                    # svdnb does not cover the 2011 response period
                    if e.args[0].startswith("Cannot map"):
                        date = wrapper.period.date_a + relativedelta(years=1)
                        wrapper = module.MigrateToWarehouse.from_str_params(
                            {"period": f"{date}"}
                        )
                    else:
                        raise e
            # TODO: replace above with call to data catalog (e.g. stacspec.org)
            #  but will still need to appropriately fill missing/unavailable
            targets[source] = target
            if source == self.reference_grid:
                with rio.open(target.path) as src:
                    win = from_bounds(*bbox, src.transform)
                    win = win.round_offsets("floor").round_shape("ceil")
                    win = Window(*[max(0, v) for v in win.flatten()])
                    vrt = {
                        "crs": src.crs.to_epsg(),
                        "transform": src.window_transform(win).to_gdal(),
                        "width": int(win.width),
                        "height": int(win.height),
                    }

        return {k: WarpPredictor(v.path, vrt) for k, v in targets.items()}

    def run(self):

        da = []
        for source, target in self.input().items():
            _da = (
                xr.open_rasterio(target.path, chunks={})
                .expand_dims({"source": [source]})
                .stack(var=("source", "band"))
            )
            _da = _da.where(_da != _da.attrs["nodatavals"][0], np.nan)
            da.append(_da)
        da = xr.concat(da, "var")
        da = da.rename({"x": "lon", "y": "lat"})

        with self.output().open("w") as f:
            f.write(da)


@inherits(GetResponse)
class GuessResponsePixels(Task):
    """
    b/c the households are anonymized, generate random but realistic
    coordinates ... this is not easy
    """

    fgt_a = NumericalParameter(
        default=1.0, min_value=0.0, max_value=np.inf, var_type=float
    )
    seed = IntParameter(default=8888)

    def output(self):
        return IntermediateTarget(task=self)

    def requires(self):
        return {
            "y": self.clone(GetResponse),
            "features": self.clone(GetAdminFeatures, id="ET", admin_level=4),
            "grid": GetPredictors(
                location=self.train_location,
                period=DateIntervalParameter().parse("2018-01-01"),
                predictors={"landscan": ["lspop"]},
            ),
        }

    def run(self):

        target = self.input()["features"]
        gdf = gpd.read_file(target.path).set_index("id", drop=True)

        # filter to response
        with self.input()["y"].open() as f:
            df = f.read().to_dataframe()
        gdf = gdf.filter(df["admin4"].unique(), axis=0)
        # FIXME: some data still getting lost where admin4 not in shapefile,
        #  ignoring R_CODE=5, there's 1261 households left out

        # load population data; it's the reference grid for training and
        # prediction, but also the weights for random response coordinates
        with self.input()["grid"].open() as f:
            grid = f.read().isel(var=0).drop("var")
        grid.name = "lspop"

        # rasterize features on population grid
        zone = xr.DataArray(
            data=rasterize(
                shapes=zip(gdf.geometry, gdf.index),
                out_shape=(grid.sizes["lat"], grid.sizes["lon"]),
                fill=np.nan,
                transform=grid.attrs["transform"],
                dtype=gdf.index.dtype,
            ),
            coords=[("lat", grid.coords["lat"]), ("lon", grid.coords["lon"])],
            name="zone",
        )

        # correct for polygons too small to contain a pixel
        for sliver in gdf.index.difference(np.unique(zone.data)):
            point = gdf.loc[sliver].geometry.centroid
            admin4 = zone.sel(lon=point.x, lat=point.y, method="nearest").data
            df.loc[df["admin4"] == sliver, "admin4"] = admin4

        # merge population and zone data arrays and choose response pixels,
        # while also calculating the weighted pixel average with fgt_a=0
        # (binary response) and with fgt_a=self.fgt_a (continuous response)
        ds = xr.merge([zone, grid])
        np.random.seed(self.seed)
        da = {}
        for (year, month), _df in df.groupby(["year", "month"]):
            _ds = ds.where(ds["zone"].isin(_df["admin4"].unique()))
            _da = _ds.groupby("zone").map(self.choose, df=_df)
            _da = _da.stack({"location": ["lat", "lon"]})
            da[(int(year), int(month))] = _da.dropna("location")

        with self.output().open("w") as f:
            f.write(da)

    def choose(self, ds, df):
        """
        function for xr.DatasetGroupBy that averages responses within randomly
        selected high population pixels
        """

        z = ds["zone"][0].data
        admin4 = df.loc[df["admin4"] == z].copy()
        n = admin4.shape[0]
        p = ds["lspop"].load().data
        np.divide(p, p.sum(), out=p, where=p > 0.0)
        iloc = np.repeat(np.arange(p.size), np.random.multinomial(n, p))
        np.random.shuffle(iloc)
        admin4.loc[:, "iloc"] = iloc
        y = admin4.groupby("iloc").apply(
            lambda g: pd.Series(
                [
                    np.average(np.where(g["y"], 1.0, 0.0), weights=g["weight"]),
                    np.average(
                        np.where(g["y"], g["y"] ** self.fgt_a, 0.0),
                        weights=g["weight"],
                    ),
                ],
                index=("y0", "ya"),
            )
        )
        y = y.reindex(range(p.size))

        return xr.Dataset(
            {k: (ds["zone"].dims, v) for k, v in y.items()}, coords=ds["zone"].coords,
        )


@requires(GuessResponsePixels)
class GetTrainingData(Task):
    """
    align response with predictor variables
    """

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):

        with self.input().open() as f:
            y = f.read()

        # run the predictor tasks for each time period
        tasks = {}
        for year, month in y.keys():
            task = GetPredictors(
                location=self.train_location, period=Date(year, month, 1),
            )
            yield task
            tasks[(year, month)] = task

        # merge the response and predictors
        ds = []
        for (year, month), task in tasks.items():
            with task.output().open() as f:
                da_x = f.read()
            da_x.name = "x"
            da_x = da_x.stack({"location": ["lat", "lon"]})
            da_y = y[(year, month)]
            _ds = xr.merge([da_y, da_x], join="left")
            # _ds.load()
            _ds["period"] = f"{year}-{month:02}"
            ds.append(_ds)
        ds = xr.concat(ds, "period")

        with self.output().open("w") as f:
            f.write(ds)
