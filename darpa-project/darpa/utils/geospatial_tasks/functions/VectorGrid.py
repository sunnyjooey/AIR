import os
from collections import Counter

import numpy as np

import ogr
import osr


class VectorGrid(object):
    def __init__(self, ref_proj):
        assert (
            type(ref_proj) is dict
        ), "ref_proj should be a dictionary that is the json output by get_raster_proj_configfile()."

        self.ref_proj = ref_proj

    def create_vector_output_file(self, outputGridfn):
        """
        Create the output vector file.
        """
        EPSG = int(self.ref_proj["wkp"].split(":")[1])
        spatialref = osr.SpatialReference()
        spatialref.ImportFromEPSG(EPSG)
        outDriver = ogr.GetDriverByName("ESRI Shapefile")
        if os.path.exists(outputGridfn):
            os.remove(outputGridfn)
        outDataSource = outDriver.CreateDataSource(outputGridfn)
        outLayer = outDataSource.CreateLayer(
            outputGridfn, spatialref, geom_type=ogr.wkbPolygon
        )

        return (outLayer, outDataSource)

    def create_fishnet_grid(self, outputGridfn):
        """
        This function creates a vector file based on a specified grid. Each grid cell is represneted
        by a vector shape.

        References
        ----------
        Adapted from GDAL cookbook: https://pcjericks.github.io/py-gdalogr-cookbook/
        """

        (originx, pixel_width, _, originy, _, pixel_height) = self.ref_proj["pixel"]
        [ncols, nrows] = self.ref_proj["ncolsrows"]

        gridHeight = float(np.abs(pixel_height))
        gridWidth = float(pixel_width)

        xmin = float(originx)
        ymax = float(originy)

        #        originy + pixel_height * nrows

        # start grid cell envelope
        ringXleftOrigin = xmin
        ringXrightOrigin = xmin + gridWidth
        ringYtopOrigin = ymax
        ringYbottomOrigin = ymax - gridHeight

        # Get Layer from spatial reference
        (outLayer, outDataSource) = self.create_vector_output_file(outputGridfn)
        featureDefn = outLayer.GetLayerDefn()

        # create grid cells
        countcols = 0
        while countcols < ncols:
            countcols += 1

            # reset envelope for rows
            ringYtop = ringYtopOrigin
            ringYbottom = ringYbottomOrigin
            countrows = 0

            while countrows < nrows:
                countrows += 1

                # add new geom to layer
                poly = self.create_square(
                    ringYtop, ringYbottom, ringXleftOrigin, ringXrightOrigin
                )
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
        del outDataSource

    def create_square(self, ringYtop, ringYbottom, ringXleftOrigin, ringXrightOrigin):
        """
        Given four points create one polygon that will be one cell on the vector grid.
        """
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(ringXleftOrigin, ringYtop)
        ring.AddPoint(ringXrightOrigin, ringYtop)
        ring.AddPoint(ringXrightOrigin, ringYbottom)
        ring.AddPoint(ringXleftOrigin, ringYbottom)
        ring.AddPoint(ringXleftOrigin, ringYtop)

        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)

        return poly

    def feature_overlap(self, grid_file, feature_file, count_flg="count"):
        """
        This function finds the number of features (buildings) that fall within a grid cell. The
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
        fieldDefn = ogr.FieldDefn("CONFLICT", ogr.OFTReal)

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

            # Count the number of incidents
            if count_flg == "count":
                ncount = ff_layer.GetFeatureCount()
                feat.SetField("CONFLICT", ncount)
                gf_layer.SetFeature(feat)

            # Count the number of fatalities
            elif count_flg == "fatalities":
                num_measure = 0
                for point in ff_layer:
                    num_measure = num_measure + point.GetFieldAsInteger("FATALITIES")
                feat.SetField("CONFLICT", num_measure)
                gf_layer.SetFeature(feat)

            # Pick the most frequent event type
            elif count_flg == "type":
                event_types = []
                for point in ff_layer:
                    event_types.append(point.GetFieldAsString("EVENT_TYPE"))
                if len(Counter(event_types).most_common()) == 0:
                    event_type = None
                else:
                    event_type = Counter(event_types).most_common()[0][0]
                feat.SetField("CONFLICT", event_type)
                gf_layer.SetFeature(feat)


def create_proj(ncols):
    height = 11.801189835999992
    width = 8.756059882999978
    pixel_width = width / ncols
    nrows = height / pixel_width

    output = {
        "nrows": int(nrows),
        "ncols": int(ncols),
        "pixel_width": pixel_width,
        "pixel_height": -pixel_width,
    }
    return output


# admin0 = gpd.read_file('/Users/alicampion/Documents/git_repositories/conflict-mapping/ss_admin0/ss_admin0.shp')
# f, ax = plt.subplots(figsize=[10,10])
# test.plot(column='EVENT_TYPE', cmap='afmhot_r', ax=ax, legend=True)
# admin0.plot(ax=ax, alpha=0.1, color='k')
# ax.set_aspect('equal')
# ax.grid(True)
