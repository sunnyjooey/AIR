# from dateutil.relativedelta import relativedelta
import datetime
import os
from pathlib import Path
import re
from tempfile import NamedTemporaryFile, TemporaryDirectory
from zipfile import ZipFile

from kiluigi.targets import IntermediateTarget, FinalTarget, CkanTarget
from kiluigi.tasks import Task, ExternalTask
from luigi import (
    Parameter,
    IntParameter,
    DictParameter,
    DateIntervalParameter,
    NumericalParameter,
    ListParameter,
)

# from luigi.date_interval import Date
from luigi.format import Nop
from luigi.util import requires, inherits

from affine import Affine
import geopandas as gpd
import numpy as np
import pandas as pd
from pyreadstat import read_dta
import rasterio as rio
from rasterio.crs import CRS
from rasterio.features import rasterize
from rasterio.shutil import copy as rio_copy
from rasterio.warp import Resampling
from rasterio.windows import Window, from_bounds
from rasterio.vrt import WarpedVRT
from shapely.geometry import Polygon
import xarray as xr

from utils import cog_tasks


class PullResponse(ExternalTask):
    def output(self):
        return CkanTarget(
            # address, username, password, check_for_updates_every aren't being picked up from luigi.cfg
            address=os.environ["LUIGI_CKAN_ADDRESS"],
            username=os.environ["LUIGI_CKAN_USERNAME"],
            password=os.environ["LUIGI_CKAN_PASSWORD"],
            check_for_updates_every=300,
            dataset={"id": "00a2eae2-d874-4bc9-a8c6-5bace2cf01f0"},
            resource={"id": "af90b3fb-82c1-4dff-84c0-e477b185b110"},
        )


@requires(PullResponse)
class ExtractResponse(Task):
    def output(self):
        f = [
            "ETH_2011_ERSS_v02_M_CSV.zip",
            "ETH_2013_ESS_v03_M_STATA.zip",
            "ETH_2015_ESS_v03_M_CSV.zip",
            "gadm36_ETH_3.zip",
        ]
        p = Path(*self.task_id.split("."))
        return {_f: IntermediateTarget(path=str(p / _f), format=Nop) for _f in f}

    def run(self):
        src = ZipFile(self.input().path)
        for k, v in self.output().items():
            p = Path(v.path).parent
            p.mkdir(exist_ok=True, parents=True)
            k = k.split(".")[0]
            fdir = f"eth_new_lsms_shapefile/{k}/(.*?).(csv|dta|cpg|dbf|prj|shp|shx)"
            r = re.compile(fdir)
            m = list(filter(r.match, src.namelist()))
            with TemporaryDirectory() as tmp:
                src.extractall(tmp, m)
                p = Path(tmp)
                with ZipFile(v.path, "w") as dst:
                    for f in p.glob("**/*.*"):
                        dst.write(f, f.name)


@requires(ExtractResponse)
class GetResponse(Task):
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

    train_years = ListParameter(default=[2011, 2013, 2016])

    def output(self):
        return IntermediateTarget(task=self)

    def run(self):
        def admin3(row):
            s = "{saq01}{saq02:02}{saq03:02}"
            return float(s.format(**row.to_dict()))

        admin_lev = "admin3"

        ds_ea_year = []
        cols = [admin_lev, "year", "month", "weight", "y"]

        if 2011 in self.train_years:
            # World Bank Rural Socioeconomic Survey 2011-2012
            # Living Standards Measurement Study (LSMS)
            # https://microdata.worldbank.org/index.php/catalog/2053
            src = self.input()["ETH_2011_ERSS_v02_M_CSV.zip"].path
            data_dir = Path(src).parent
            data_dir = data_dir / "ETH_2011_ERSS_v02_M_CSV"
            data_dir.mkdir(exist_ok=True, parents=True)
            with ZipFile(src, "r") as zf:
                zf.extractall(data_dir)

            # Nominal annual consumption per adult equivalent and weight
            tot = pd.read_csv("{}/cons_agg_w1.csv".format(data_dir))
            tot = tot[["household_id", "nom_totcons_aeq", "pw"]]

            # region info
            reg = pd.read_csv("{}/sect1_hh_w1.csv".format(data_dir))
            reg = reg[
                ["household_id", "saq01", "saq02", "saq03", "saq04", "saq05", "saq06"]
            ]
            reg = reg.groupby("household_id").first()
            reg[admin_lev] = reg.apply(lambda row: admin3(row), axis=1)

            # month info
            dat = pd.read_csv("{}/sect_cover_hh_w1.csv".format(data_dir))
            dat = dat[["household_id", "hh_saq13_b"]]

            # concat
            d = pd.merge(tot, reg, on="household_id")
            d = pd.merge(d, dat, on="household_id")
            d["year"] = 2011

            # convert to xarray
            ds = d.to_xarray()
            ds = ds.rename(hh_saq13_b="month", pw="weight", household_id="uhhid")

            # poverty calc
            # World Bank PovcalNet: http://iresearch.worldbank.org/PovcalNet/
            # Poverty line in local currency for 2010.5
            poverty_line = 283.109 * 12
            ds["y"] = (poverty_line - ds["nom_totcons_aeq"]).clip(0) / poverty_line
            ds_ea_year.append(ds[cols])

        if 2013 in self.train_years:
            # World Bank Ethiopia Socioeconomic Survey 2013-2014
            # Living Standards Measurement Study (LSMS)
            # https://microdata.worldbank.org/index.php/catalog/2247
            src = self.input()["ETH_2013_ESS_v03_M_STATA.zip"].path
            data_dir = Path(src).parent
            data_dir = data_dir / "ETH_2013_ESS_v03_M_STATA"
            data_dir.mkdir(exist_ok=True, parents=True)
            with ZipFile(src, "r") as zf:
                zf.extractall(data_dir)

            # Nominal annual consumption per adult equivalent and weight
            tot = read_dta("{}/cons_agg_w2.dta".format(data_dir))[0]
            tot = tot[["household_id", "household_id2", "nom_totcons_aeq", "pw2"]]

            # region info
            reg = read_dta("{}/sect1_hh_w2.dta".format(data_dir))[0]
            reg = reg[
                [
                    "household_id",
                    "household_id2",
                    "saq01",
                    "saq02",
                    "saq03",
                    "saq04",
                    "saq05",
                    "saq06",
                ]
            ]
            reg = reg.groupby("household_id2").first()
            reg[admin_lev] = reg.apply(lambda row: admin3(row), axis=1)

            # month info
            dat = read_dta("{}/sect_cover_hh_w2.dta".format(data_dir))[0]
            dat = dat[["household_id", "household_id2", "hh_saq13_b"]]

            # concat
            d = pd.merge(tot, reg, on="household_id2")
            d = pd.merge(d, dat, on="household_id2")
            d["year"] = 2013

            # convert to xarray
            ds = d.to_xarray()
            ds = ds.rename(hh_saq13_b="month", pw2="weight", household_id2="uhhid")

            # poverty calc
            # World Bank PovcalNet: http://iresearch.worldbank.org/PovcalNet/
            # Month poverty line at $1.90 per day
            # World Bank ICP database: https://databank.worldbank.org/source/icp-2017
            # Purchasing power parity (PPP) (US$ = 1) 9020000:ACTUAL INDIVIDUAL CONSUMPTION rate for 2013
            poverty_line = 57.7917 * 6.5035 * 12
            ds["y"] = (poverty_line - ds["nom_totcons_aeq"]).clip(0) / poverty_line
            ds_ea_year.append(ds[cols])

        if 2016 in self.train_years:
            # Socioeconomic Survey 2015-2016, Wave 3
            # Living Standards Measurement Study (LSMS)
            # https://microdata.worldbank.org/index.php/catalog/2783
            src = self.input()["ETH_2015_ESS_v03_M_CSV.zip"].path
            data_dir = Path(src).parent
            data_dir = data_dir / "ETH_2015_ESS_v03_M_CSV"
            data_dir.mkdir(exist_ok=True, parents=True)
            with ZipFile(src, "r") as zf:
                zf.extractall(data_dir)

            # Nominal annual consumption per adult equivalent and weight
            tot = pd.read_csv("{}/cons_agg_w3.csv".format(data_dir))
            tot = tot[["household_id", "household_id2", "nom_totcons_aeq", "pw_w3"]]

            # region info
            reg = pd.read_csv("{}/sect1_hh_w3.csv".format(data_dir))
            reg = reg[
                ["household_id", "saq01", "saq02", "saq03", "saq04", "saq05", "saq06"]
            ]
            reg = reg.groupby("household_id").first()
            reg[admin_lev] = reg.apply(lambda row: admin3(row), axis=1)

            # month info
            dat = pd.read_csv("{}/sect_cover_hh_w3.csv".format(data_dir))
            dat = dat[["household_id", "hh_saq13_b"]]

            # concat
            d = pd.merge(tot, reg, on="household_id")
            d = pd.merge(d, dat, on="household_id")
            d["year"] = 2016

            # convert to xarray
            ds = d.to_xarray()
            ds = ds.rename(hh_saq13_b="month", pw_w3="weight", household_id2="uhhid")

            # poverty calc
            # World Bank PovcalNet: http://iresearch.worldbank.org/PovcalNet/
            # Poverty line in local currency for 2015.5
            poverty_line = 509.736 * 12
            ds["y"] = (poverty_line - ds["nom_totcons_aeq"]).clip(0) / poverty_line
            ds_ea_year.append(ds[cols])

        ### concat together and save
        ds = xr.concat(ds_ea_year, dim="uhhid")
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

    def output(self):
        p = Path(*self.task_id.split("."))
        return FinalTarget(path=str(p.with_suffix(".geojson")))

    def run(self):
        if self.location.get("id") != "ET":
            raise NotImplementedError

        # load feature collection
        shp = self.input()["gadm36_ETH_3.zip"].path
        gdf = gpd.read_file(f"zip://{shp}")
        gdf = gdf.to_crs(CRS.from_epsg(4326))
        gdf["CC_3"] = gdf["CC_3"].apply(lambda x: int(x))
        gdf["id"] = gdf["CC_3"]

        # remove extraneous columns
        cols = ["NAME_3", "CC_3", "geometry", "id"]
        gdf = gdf[cols]

        # dissolve internal boundaries
        # gdf = gdf.dissolve("id")
        # admin4 dissolve merges 2 nearby polygons having the same code

        # cleanup result of disjoint "borders" (thanks, ESRI)
        # FIXME: do better than simply dropping interiors, but dealing with
        # the crappy dissolve will take serious shapely chops!
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

    period = DateIntervalParameter(
        default=DateIntervalParameter().parse("2011-01-01-2017-12-31"),
        description=(
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
        if self.reference_grid not in self.predictors.keys():
            self.predictors = dict(self.predictors, **{self.reference_grid: []})

        # each predictor source needs either annual or monthly data
        intvl = {"esacci": "Y", "fldas": "M", "landscan": "Y", "svdnb": "M"}

        # make a list of predictor source - time interval needed
        period_dict = {}
        for source in self.predictors.keys():
            if source == "svdnb":
                # SVD only available from 2012 Apr
                if self.period.date_a >= datetime.date(2012, 4, 1):
                    period_dict.update(
                        {
                            "svdnb": pd.period_range(
                                self.period.date_a, self.period.date_b, freq="M"
                            )
                        }
                    )
                elif self.period.date_b >= datetime.date(2012, 4, 1):
                    period_dict.update(
                        {
                            "svdnb": pd.period_range(
                                datetime.date(2012, 4, 1), self.period.date_b, freq="M"
                            )
                        }
                    )
            else:
                period_dict.update(
                    {
                        source: pd.period_range(
                            self.period.date_a, self.period.date_b, freq=intvl[source]
                        )
                    }
                )

        # each like: ("fldas", "2016-04") or ("esacci", "2015")
        source_intervals = [
            (source, str(interval))
            for source in period_dict.keys()
            for interval in period_dict[source]
        ]
        return WarpPredictor(source_intervals=source_intervals)

    def run(self):
        # each predictor source - time period has one warped file
        # that is converted to an xarray
        # each key is like ('2011', 'esacci')
        da = {}
        for path in self.input().values():
            labels = path.path.split("/")[-1].split("_")
            source = labels[0].lower()
            period = labels[1]
            _da = (
                xr.open_rasterio(path.path, chunks={})
                .expand_dims({"source": [source]})
                .stack(var=("source", "band"))
                .rename({"x": "lon", "y": "lat"})
            )
            _da = _da.where(_da != _da.attrs["nodatavals"][0], np.nan)
            da[(period, source)] = _da
            # would be good to add period as a dimension
            # but creates trouble down the line
            # combine into one like: xr.concat([v for v in da.values()], "var")

        with self.output().open("w") as f:
            f.write(da)


@inherits(GetPredictors)
class WarpPredictor(Task):
    source_intervals = ListParameter()

    def output(self):
        output = {}
        for pair in self.source_intervals:
            source = pair[0].upper()
            interval = pair[1].replace("-", "/")
            filename = "{s}_{i}_WARPED".format(s=source, i=pair[1])
            p = Path(
                "models/poverty_model/data/geo_data", source, interval, filename
            ).with_suffix(".tif")
            output[(source, pair[1])] = FinalTarget(path=str(p), format=Nop)
        return output

    def requires(self):
        targets = []
        for source_interval_pair in self.source_intervals:
            source = source_interval_pair[0]
            interval = source_interval_pair[1]
            module = getattr(cog_tasks, source)
            # each predictor source has its own download method
            targets.append(
                module.MakeCOG(interval, self.predictors[source], self.location)
            )
        return targets

    def run(self):
        # this is the reference grid
        ref_path = [
            p.path
            for p in self.input()
            if re.compile(r"{}".format(self.reference_grid)).search(p.path.lower())
        ][-1]
        with rio.open(ref_path) as src:
            win = from_bounds(*self.location["bbox"], src.transform)
            win = win.round_offsets("floor").round_shape("ceil")
            win = Window(*[max(0, v) for v in win.flatten()])
            vrt_options = {
                "crs": src.crs,
                "transform": Affine.from_gdal(*src.window_transform(win).to_gdal()),
                "width": int(win.width),
                "height": int(win.height),
                "resampling": Resampling.bilinear,
            }

        # warp each to reference grid, formerly WarpPredictor class
        for path in self.input():
            ### added this because only fldas and svdnb need to be warped
            ### esacci and landscan are pulled already warped
            if re.search("fldas", path.path.lower()) or re.search(
                "svdnb", path.path.lower()
            ):
                tmp = NamedTemporaryFile(suffix=".tif")
                with rio.open(path.path) as src:
                    profile = src.profile
                    tags = src.tags()
                    # cast the the esacci lc dataset into binary bands for each class
                    if "lccs_class#flag_values" in tags:
                        flag_values = [
                            int(flag)
                            for flag in tags["lccs_class#flag_values"]
                            .strip("{}")
                            .split(",")
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
                        filepath = tmp.name
                        with rio.open(filepath, "w", **profile) as dst:
                            for band, flag in enumerate(flag_values, start=1):
                                dst.write((x[0] == flag).astype(profile["dtype"]), band)
                    else:
                        filepath = path.path

                # copy the warped window to output
                key = re.search(
                    "(?<=geo_data/)[A-Z]+/[0-9]{4}(?:/[0-9]{2})?", path.path
                )[0]
                key = key.split("/")
                key = (key[0], "-".join(key[1:]))
                with rio.open(filepath) as src, self.output()[key].open("w") as dst:
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


@inherits(GetResponse)
class GuessResponsePixels(Task):

    fgt_a = NumericalParameter(
        default=1.0, min_value=0.0, max_value=np.inf, var_type=float
    )

    train_years = ListParameter(default=[2011, 2013, 2016])

    seed = IntParameter(default=8888)

    def output(self):
        return IntermediateTarget(task=self)

    def requires(self):
        ref_year = "{}-01-01".format(str(max(self.train_years)))
        return {
            "y": self.clone(GetResponse),
            "features": self.clone(GetAdminFeatures, id="ET"),
            "grid": GetPredictors(
                location=self.train_location,
                period=DateIntervalParameter().parse(ref_year),
                predictors={"landscan": ["lspop"]},
            ),
        }

    def run(self):
        target = self.input()["features"]
        gdf = gpd.read_file(target.path).set_index("id", drop=True)
        admin_level = "admin3"

        # filter to response
        with self.input()["y"].open() as f:
            df = f.read().to_dataframe()
        df = df.loc[~df["y"].isnull(), :]
        gdf = gdf.filter(df[admin_level].unique(), axis=0)
        # FIXME: some data still getting lost where admin4 not in shapefile

        # load population data; it's the reference grid for training and
        # prediction, but also the weights for random response coordinates
        with self.input()["grid"].open() as f:
            grid_dct = f.read()
        grid = grid_dct[(str(max(self.train_years)), "landscan")]
        grid = grid.isel(var=0).drop("var")
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
            admin = zone.sel(lon=point.x, lat=point.y, method="nearest").data
            df.loc[df[admin_level] == sliver, admin_level] = admin

        # merge population and zone data arrays and choose response pixels,
        # while also calculating the weighted pixel average with fgt_a=0
        # (binary response) and with fgt_a=self.fgt_a (continuous response)
        ds = xr.merge([zone, grid])
        np.random.seed(self.seed)
        da = {}
        for (year, month), _df in df.groupby(["year", "month"]):
            _ds = ds.where(ds["zone"].isin(_df[admin_level].unique()))
            # make sure there is at least one valid zone
            if len(_ds["zone"].data[~np.isnan(_ds["zone"].data)]) > 0:
                _da = _ds.groupby("zone").map(
                    self.choose, df=_df, admin_level=admin_level
                )
                _da = _da.stack({"location": ["lat", "lon"]})
                da[(int(year), int(month))] = _da.dropna("location")

        with self.output().open("w") as f:
            f.write(da)

    def choose(self, ds, df, admin_level):
        """
        function for xr.DatasetGroupBy that averages responses within randomly
        selected high population pixels
        """

        z = ds["zone"][0].data
        admin = df.loc[df[admin_level] == z].copy()
        n = admin.shape[0]
        p = ds["lspop"].load().data
        np.divide(p, p.sum(), out=p, where=p > 0.0)
        iloc = np.repeat(np.arange(p.size), np.random.multinomial(n, p))
        np.random.shuffle(iloc)
        admin.loc[:, "iloc"] = iloc
        y = admin.groupby("iloc").apply(
            lambda g: pd.Series(
                [
                    np.average(
                        np.where(g["y"], 1.0, 0.0), weights=g["weight"]
                    ),  # everything NOT 0 including nan is 1, 0 is 0
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


@inherits(GuessResponsePixels)
class GetTrainingData(Task):
    def output(self):
        return IntermediateTarget(task=self)

    def requires(self):
        tasks = {"y": self.clone(GuessResponsePixels), "predictors": []}

        for year in self.train_years:
            train_period = "{}-01-01-{}-12-31".format(str(year), str(year))
            tasks["predictors"].append(
                self.clone(
                    GetPredictors, period=DateIntervalParameter().parse(train_period)
                )
            )

        return tasks

    def run(self):

        with self.input()["y"].open() as f:
            y = f.read()

        # merge the response and predictors
        x = {}
        for xs in self.input()["predictors"]:
            with xs.open() as f:
                xo = f.read()
            x.update(xo)

        ds = []
        for (year, month) in y.keys():
            period = f"{year}-{month:02}"
            period_keys = [
                key for key in x.keys() if (key[0] == period) | (key[0] == str(year))
            ]
            da_x = xr.concat([x[key] for key in period_keys], "var")
            da_x.name = "x"
            da_x = da_x.stack({"location": ["lat", "lon"]})
            da_y = y[(year, month)]
            _ds = xr.merge([da_y, da_x], join="left")
            _ds = _ds.dropna("location")
            _ds["period"] = period
            ds.append(_ds)
        ds = xr.concat(ds, "period")

        with self.output().open("w") as f:
            f.write(ds)
