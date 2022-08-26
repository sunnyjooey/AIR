import affine
import numpy as np
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.warp import calculate_default_transform

from models.hydrology_model.soil_moisture.data_cleaning import (
    AverageUsingMovingWindowMixin,
)
from utils.test_utils import LuigiTestCase


class AverageUsingMovingWindowTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()
        """
        Prepare input rasterfile to `average_using_window` method
        """
        # Create Low resolution raster
        low_res_array = np.array([4, 10, 5, 7, 8, 1, 6, 2, 9]).reshape(3, 3)
        low_res_array = low_res_array.astype("float32")
        raster_metadata = {
            "transform": affine.Affine(0.05, 0.0, 24.15, 0.0, -0.05, 12.25),
            "driver": "GTiff",
            "width": 3,
            "height": 3,
            "count": 1,
            "crs": CRS.from_epsg(4326),
            "dtype": "float32",
            "nodata": 0,
        }
        self.low_res = MemoryFile()
        with self.low_res.open(**raster_metadata) as dst:
            dst.write(low_res_array, 1)

        # Get transform for medium resolution raster
        transform, width, height = calculate_default_transform(
            src_crs=CRS.from_epsg(4326),
            dst_crs=CRS.from_epsg(4326),
            width=3,
            height=3,
            left=24.15,
            bottom=12.1,
            right=24.299999999999997,
            top=12.25,
            resolution=(0.0125, 0.0125),
        )

        # Create medium resolution raster
        med_res_array = np.arange(1, 145).reshape(12, 12)
        med_res_array = med_res_array.astype("float32")
        med_res_rast_meta = {
            "transform": transform,
            "driver": "GTiff",
            "width": 12,
            "height": 12,
            "count": 1,
            "crs": CRS.from_epsg(4326),
            "dtype": "float32",
            "nodata": 0,
        }
        self.med_res = MemoryFile()
        with self.med_res.open(**med_res_rast_meta) as dst:
            dst.write(med_res_array, 1)

    def test_average_using_window(self):

        dst_file = MemoryFile()
        AverageUsingMovingWindowMixin().average_using_window(
            self.med_res, self.low_res, dst_file
        )
        actual_output = np.squeeze(dst_file.open().read())
        # Expected output
        expected_output = np.array(
            [
                [1.44, 1.92, 3.12, 3.84, 4.56, 5.28, 5.4, 4.8, 4.2, 3.6, 2.4, 1.8],
                [1.92, 2.56, 4.16, 5.12, 6.08, 7.04, 7.2, 6.4, 5.6, 4.8, 3.2, 2.4],
                [2.76, 3.68, 5.6, 6.6, 7.6, 8.6, 8.52, 7.44, 6.36, 5.28, 3.36, 2.52],
                [3.12, 4.16, 6.0, 6.8, 7.6, 8.4, 8.04, 6.88, 5.72, 4.56, 2.72, 2.04],
                [3.48, 4.64, 6.4, 7.0, 7.6, 8.2, 7.56, 6.32, 5.08, 3.84, 2.08, 1.56],
                [3.84, 5.12, 6.8, 7.2, 7.6, 8.0, 7.08, 5.76, 4.44, 3.12, 1.44, 1.08],
                [4.08, 5.44, 6.8, 6.8, 6.8, 6.8, 5.96, 5.12, 4.28, 3.44, 2.08, 1.56],
                [3.96, 5.28, 6.4, 6.2, 6.0, 5.8, 5.32, 5.04, 4.76, 4.48, 3.36, 2.52],
                [3.84, 5.12, 6.0, 5.6, 5.2, 4.8, 4.68, 4.96, 5.24, 5.52, 4.64, 3.48],
                [3.72, 4.96, 5.6, 5.0, 4.4, 3.8, 4.04, 4.88, 5.72, 6.56, 5.92, 4.44],
                [
                    2.88,
                    3.84,
                    4.16,
                    3.52,
                    2.88,
                    2.24,
                    2.72,
                    3.84,
                    4.96,
                    6.08,
                    5.76,
                    4.32,
                ],
                [
                    2.16,
                    2.88,
                    3.12,
                    2.64,
                    2.16,
                    1.68,
                    2.04,
                    2.88,
                    3.72,
                    4.56,
                    4.32,
                    3.24,
                ],
            ]
        )

        np.testing.assert_array_almost_equal(expected_output, actual_output, 4)
