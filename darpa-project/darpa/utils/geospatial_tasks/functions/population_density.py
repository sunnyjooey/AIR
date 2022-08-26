import os
from math import ceil

import numpy as np
import ogr
import osr


def create_fishnet_grid(
    outputGridfn, xmin, ymax, gridHeight, gridWidth, ncols, nrows, EPSG=4326
):
    """ This function creates a vector file based on a specified grid. Each grid cell is represneted
    by a vector shape.

    Parameters
    ----------
    outputGridfn : string
        Path and file name for the output fishnet vector file

    xmin : float
        The lower left corner of the grided area or the minimum x value (longitude) of the area to
        be gridded. This value is not the center of the grid cell but the corner.

    ymax : float
        The upper left corner of the gridded area or the maximum y value (latitude) of the area to
        be gridded. This value is not the center of the grid cell but the corner.

    gridHeight : float
       The height of a grid cell.

    gridWidth : float
        The width of a grid cell.

    ncols : integer
        The number of columns in the grid.

    nrows : integer
        The number of rows in the grid.

    EPSG : integer
        Spatial reference code for projection


    References
    ----------
    Adapted from GDAL cookbook: https://pcjericks.github.io/py-gdalogr-cookbook/
    """

    gridHeight = np.abs(gridHeight)

    xmin = float(xmin)
    xmax = float(xmin + gridWidth * ncols)
    ymin = float(ymax - gridHeight * nrows)
    ymax = float(ymax)
    gridWidth = float(gridWidth)
    gridHeight = float(gridHeight)

    # get rows
    rows = ceil((ymax - ymin) / gridHeight)
    # get columns
    cols = ceil((xmax - xmin) / gridWidth)

    # start grid cell envelope
    ringXleftOrigin = xmin
    ringXrightOrigin = xmin + gridWidth
    ringYtopOrigin = ymax
    ringYbottomOrigin = ymax - gridHeight

    # create output file
    spatialref = osr.SpatialReference()
    spatialref.ImportFromEPSG(EPSG)
    outDriver = ogr.GetDriverByName("ESRI Shapefile")
    if os.path.exists(outputGridfn):
        os.remove(outputGridfn)
    outDataSource = outDriver.CreateDataSource(outputGridfn)
    outLayer = outDataSource.CreateLayer(
        outputGridfn, spatialref, geom_type=ogr.wkbPolygon
    )
    featureDefn = outLayer.GetLayerDefn()

    # create grid cells
    countcols = 0
    while countcols < cols:
        countcols += 1

        # reset envelope for rows
        ringYtop = ringYtopOrigin
        ringYbottom = ringYbottomOrigin
        countrows = 0

        while countrows < rows:
            countrows += 1
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint(ringXleftOrigin, ringYtop)
            ring.AddPoint(ringXrightOrigin, ringYtop)
            ring.AddPoint(ringXrightOrigin, ringYbottom)
            ring.AddPoint(ringXleftOrigin, ringYbottom)
            ring.AddPoint(ringXleftOrigin, ringYtop)
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)

            # add new geom to layer
            outFeature = ogr.Feature(featureDefn)
            outFeature.SetGeometry(poly)
            outLayer.CreateFeature(outFeature)
            outFeature = None

            # new envelope for next poly
            ringYtop = ringYtop - gridHeight
            ringYbottom = ringYbottom - gridHeight

        # new envelope for next poly
        ringXleftOrigin = ringXleftOrigin + gridWidth
        ringXrightOrigin = ringXrightOrigin + gridWidth

    # Save and close DataSources
    outDataSource = None


def feature_overlap(grid_file, feature_file):
    """ This function finds the number of features (buildings) that fall within a grid cell. The
    number of features are written as fields of the original grid vector file and are saved to a
    field named N_BUILD.

    Parameters
    ----------
    grid_file : string
        Path and file name of the vector grid file.

    feature_file : string
        Path and file name of the vector file containing features (buildings)

    """

    # --------------------------
    # Read feature and grid file
    # --------------------------
    ff_driver = ogr.GetDriverByName("ESRI Shapefile")
    ff = ff_driver.Open(feature_file, 0)
    ff_layer = ff.GetLayer()

    gf_driver = ogr.GetDriverByName("ESRI Shapefile")
    gf = gf_driver.Open(grid_file, 1)
    gf_layer = gf.GetLayer()

    # -----------------------------------------------
    # Add attribute (Number of features) to grid file
    # -----------------------------------------------
    fieldDefn = ogr.FieldDefn("N_BUILD", ogr.OFTReal)
    fieldDefn.SetWidth(14)
    fieldDefn.SetPrecision(6)
    gf_layer.CreateField(fieldDefn)

    # ----------------------------------
    # Iterate over features in grid file
    # and assign number of intersecting
    # feature
    # ----------------------------------
    for feat in gf_layer:
        feat_geom = feat.GetGeometryRef()
        ff_layer.SetSpatialFilter(feat_geom)
        nblds = ff_layer.GetFeatureCount()
        feat.SetField("N_BUILD", nblds)
        gf_layer.SetFeature(feat)
