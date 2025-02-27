import os

import fiona
import rasterio

from models.crop_model.util import get_rasters_dict

"""longitude/latitude"""


def xy_from_vector(v):
    args = v.split("::")
    return extract_vector_coords(args[1])


def get_site_raster_value(dataset, band, site):
    y, x = site
    row, col = dataset.index(y, x)
    return band[row, col]


def peer(run, sample_size=None):
    rasters = get_rasters_dict(run)
    sites = xy_from_vector(run["sites"])
    data = []
    nodata = []
    layers = list(rasters.keys())
    for raster in rasters.values():
        with rasterio.open(raster) as ds:
            if "int" in ds.dtypes[0]:
                nodata.append(int(ds.nodatavals[0]))
            else:
                nodata.append(ds.nodatavals[0])
            band = ds.read(1)
            data.append([get_site_raster_value(ds, band, site) for site in sites])
    peerless = list(
        filter(
            lambda x: x is not None,
            [
                read_layer_by_cell(i, data, nodata, layers, sites)
                for i in range(len(sites))
            ],
        )
    )
    if sample_size < 0:
        sample_size = None
    return peerless[:sample_size]


def read_layer_by_cell(idx, data, nodata, layers, sites):
    lat, lng = sites[idx]
    cell = {"lat": lat, "lng": lng, "xcrd": lng, "ycrd": lat}
    for i, c in enumerate(data):
        if c[idx] == nodata[i]:
            return None
        else:
            if layers[i] == "harvestArea" and c[idx] == 0:
                return None
            else:
                cell[layers[i]] = c[idx]
    return cell


def make_run_directory(rd):
    os.makedirs(rd, exist_ok=True)


def get_rio_profile(f):
    with rasterio.open(f) as source:
        profile = source.profile
    return profile


def get_shp_profile(f):
    pass


def extract_vector_coords(f):
    points = []
    with fiona.open(f) as source:
        for feature in source:
            if feature["geometry"]["type"] == "MultiPoint":
                points.append(feature["geometry"]["coordinates"][0])
            if feature["geometry"]["type"] == "Point":
                points.append(feature["geometry"]["coordinates"])
    return points


def find_vector_coords(f, x, y, a):
    coords = (y, x)
    with fiona.open(f) as source:
        for feature in source:
            if feature["geometry"]["type"] == "MultiPoint":
                if coords in feature["geometry"]["coordinates"]:
                    return feature["properties"][a]
            if feature["geometry"]["type"] == "Point":
                if coords == feature["geometry"]["coordinates"]:
                    return feature["properties"][a]
