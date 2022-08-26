import os

import geopandas as gpd
import numpy as np
import rasterio
from kiluigi.parameter import Parameter
from kiluigi.targets import IntermediateTarget
from kiluigi.tasks import Task
from rasterio.mask import mask
from shapely.geometry import shape

from utils.geospatial_tasks.functions.geospatial import geography_f_country


class MaskDataToGeography(Task):

    """
    This is a generic task to mask data used to generate predictions from a model to a particular geography.

    The task requires an upstream data consolidation task that writes all input variables to one directory
    in either GeoJSON (.geojson) format for vector data or GeoTiff (.tif) format for raster data. There is also
    functionality to mask a pickled GeoDataFrame (.pickle) in the event that the upstream task only returns one file.
    A TypeError is raised if the input data is not in one of these file formats.

    It writes the masked data (with the same file names as the input) to an IntermediateTarget directory for
    downstream merging for model input.

    Parameters
    ----------
    country_level: Parameter
        The country name use to get national boundary used for masking the data.
    """

    country_level = Parameter()

    def datetime_schema(self, gdf):
        schema = gpd.io.file.infer_schema(gdf)
        schema["properties"]["Time"] = "datetime"
        return schema

    def mask_vector(self, vector_path, mask_geojson, outdir, name=None, pickled=False):
        if pickled:
            with self.input().open("r") as src:
                input_dict = src.read()
                name = list(input_dict.keys())[0]
                vector = input_dict[name]
        else:
            vector = gpd.read_file(vector_path)
        vector["geometry"] = vector["geometry"].buffer(0)
        vector["geometry"] = vector.intersection(shape(mask_geojson).buffer(0))
        vector = vector[vector["geometry"].notnull()]

        if "MultiPolygon" in set(vector.geometry.type):
            vector = self.explode_gdf(vector)

        schema = self.datetime_schema(vector)
        vector.to_file(os.path.join(outdir, name), schema=schema, driver="GeoJSON")

    def explode_gdf(self, gdf):
        gdf = gdf.drop([i for i in ["level_0", "level_1"] if i in gdf.columns], axis=1)
        exploded_gdf = gdf.explode().reset_index().rename(columns={0: "geometry"})
        exploded_gdf = exploded_gdf[["level_0", "level_1", "geometry"]].copy()
        gdf = gdf.drop("geometry", axis=1)
        gdf = exploded_gdf.merge(gdf, left_on="level_0", right_index=True)
        # drop level_0, and level_1 to avoid downstream issues and hassles
        gdf = gdf.set_geometry("geometry").drop(columns=["level_0", "level_1"])

        return gdf

    def mask_raster(self, raster_path, mask_geojson, outdir, name):
        with rasterio.open(raster_path) as src:
            meta = src.meta.copy()
            masked_img, masked_transform = mask(src, [mask_geojson], crop=True)
            height, width = masked_img.shape[-2:]
            meta.update(transform=masked_transform, height=height, width=width)
            masked_img = np.squeeze(masked_img)

        with rasterio.open(os.path.join(outdir, name), "w", **meta) as dst:
            if len(masked_img.shape) == 2:
                dst.write(masked_img.astype(meta["dtype"]), 1)
            else:
                for i in range(masked_img.shape[0]):
                    dst.write_band(i + 1, masked_img[i].astype(meta["dtype"]))

    def output(self):
        return IntermediateTarget(path=f"{self.task_id}/", timeout=3600)

    def overwrite_input(self):
        inputs = self.input()
        return inputs

    def get_input(self):
        input_object = self.overwrite_input()
        if type(input_object) is not list:
            input_object = [input_object]
        return input_object

    def run(self):
        geography = geography_f_country(self.country_level)
        try:
            mask_geom = geography["features"][0]["geometry"]
        except KeyError:
            mask_geom = geography
        for input_object in self.get_input():
            if input_object == []:
                pass
            else:
                input_path = input_object.path
                with self.output().temporary_path() as tmpdir:
                    os.makedirs(tmpdir)
                    if os.path.isfile(input_path):
                        name = os.path.split(input_path)[1]
                        self.select_mask_method_and_apply(
                            input_path, mask_geom, tmpdir, name
                        )
                    elif os.path.isdir(input_path):
                        for f in os.listdir(input_path):
                            file_path = os.path.join(input_path, f)
                            self.select_mask_method_and_apply(
                                file_path, mask_geom, tmpdir, f
                            )

    def select_mask_method_and_apply(self, file_path, mask_geom, outdir, name):
        """
        Select mask method depending on the file extension or GDAL recognition.
        """
        if file_path.endswith((".geojson", ".shp")):
            print(file_path)
            self.mask_vector(file_path, mask_geom, outdir, name)

        elif file_path.endswith(".pickle"):
            self.mask_vector(file_path, mask_geom, outdir, name, pickled=True)
        else:
            try:
                raster = rasterio.open(file_path)
                raster.close()
            except rasterio.RasterioIOError:
                raise TypeError(
                    f"Unable to mask {file_path}. All input must be vector data "
                    f"in GeoJSON format or pickled, or in a raster data (e.g. "
                    f"GeoTIFF) format."
                )
            self.mask_raster(file_path, mask_geom, outdir, name)
