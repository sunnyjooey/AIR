import json
from pathlib import Path

from luigi.util import requires
from kiluigi.tasks import Task, ExternalTask
from kiluigi.targets import CkanTarget, IntermediateTarget
import geopandas as gpd
import xarray as xr
import numpy as np
from rasterio.features import rasterize

from utils.scenario_tasks.functions.ScenarioDefinition import GlobalParameters

with (Path(__file__).parent / "data" / "ckan.json").open() as f:
    admin_features = json.load(f)["admin_features"]


@requires(GlobalParameters)
class GetAdminFeatures(ExternalTask):
    """
    Pull administrative boundaries from CKAN
    """

    def output(self):

        # FIXME: eliminate with iso-3116 standard in self.geography
        iso3166 = {
            "Ethiopia": "ET",
            "South Sudan": "SS",
        }

        try:
            resource = admin_features[iso3166[self.country_level]][self.admin_level]
        except KeyError:
            raise NotImplementedError

        return CkanTarget(**resource)


class RasterizeAdminFeatures(Task):
    """self.input() must be a list of:
         - reference xarray.DataArray
         - features
    """

    def output(self):
        return IntermediateTarget(task=self)

    def rasterize(self, da):

        # read feature data
        path = Path(self.input()[1].path)
        src = str(path)
        if path.suffix == ".zip":
            src = "zip://" + src
        gdf = gpd.read_file(src)
        geometry = gdf.geometry.copy().to_crs(da.attrs["crs"])

        # create raster dataset of feature indices matching input projection
        zone = xr.DataArray(
            data=rasterize(
                shapes=zip(geometry, geometry.index),
                out_shape=(da.sizes["x"], da.sizes["y"]),
                fill=-9999,
                transform=da.attrs["transform"],
                dtype=np.dtype(np.int32),
            ),
            dims=("x", "y"),
            name="zone",
        )
        ds = xr.merge([zone, gdf.to_xarray().rename_dims({"index": "location"})])

        return ds
