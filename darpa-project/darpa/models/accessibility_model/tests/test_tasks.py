import numpy as np

from models.accessibility_model.utils import calculate_accesibility_surface
import os
import luigi
import pandas as pd
import psycopg2

from models.accessibility_model.tasks import (
    FrictionSurface,
    PushCSVToDatabase_TravelTimeToTowns,
)
from utils.test_utils import LuigiTestCase

BASE_DIR = os.path.dirname(__file__)


class FrictionSurfaceTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()
        # Create fake input data
        self.landspeed = np.linspace(1, 5, 25).reshape(5, 5)
        self.landspeed[0, 0] = -9999.0
        self.dem = np.linspace(1000, 2500, 25).reshape(5, 5)
        self.dem[4, 4] = -9999.0
        self.slope = np.radians(np.linspace(0, 85, 25).reshape(5, 5))
        self.slope[4, 4] = -9999.0

    def test_adjust_landspeed_for_elevation(self):
        # Expected land speed adjusted for elevation
        expected_adj_speed = np.array(
            [
                -9999.0,
                1.0577,
                1.2008,
                1.3418,
                1.481,
                1.6182,
                1.7535,
                1.887,
                2.0185,
                2.1483,
                2.2762,
                2.4023,
                2.5266,
                2.6492,
                2.77,
                2.8891,
                3.0064,
                3.1221,
                3.2361,
                3.3484,
                3.4591,
                3.5682,
                3.6756,
                3.7815,
                -9999.0,
            ]
        ).reshape(5, 5)
        computed_adj_speed = FrictionSurface().adjust_landspeed_for_elevation(
            self.landspeed, -9999.0, self.dem, -9999.0
        )
        np.testing.assert_array_almost_equal(computed_adj_speed, expected_adj_speed, 4)

    def test_adjust_land_speed_for_slope(self):
        # Expected land speed for slope
        expected_slope_adj_speed = np.array(
            [
                -9999.0,
                1.1708,
                1.3330,
                1.4940,
                1.6538,
                1.8123,
                1.9696,
                2.1257,
                2.2805,
                2.4342,
                2.5867,
                2.7380,
                2.8882,
                3.0371,
                3.1849,
                3.3316,
                3.4771,
                3.6214,
                3.7646,
                3.9067,
                4.0477,
                4.1875,
                4.3262,
                4.4638,
                -9999.0,
            ]
        ).reshape(5, 5)

        computed_slope_adj_speed = FrictionSurface().adjust_land_speed_for_slope(
            self.landspeed, -9999.0, self.slope, -9999.0
        )

        np.testing.assert_array_almost_equal(
            computed_slope_adj_speed, expected_slope_adj_speed, 4
        )
        # Test for slope greater than 90 degrees
        invalid_slope = np.radians(np.linspace(91, 300, 25).reshape(5, 5))
        with self.assertRaises(ValueError):
            FrictionSurface().adjust_land_speed_for_slope(
                self.landspeed, -9999.0, invalid_slope, -9999.0
            )


class CalculateAccesibilitySurfaceTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()
        # Create input frictions
        self.friction_array = np.array([4, 10, 5, 7, 8, 1, 6, 2, 9]).reshape(3, 3)

        # Create destination array
        self.dest_array = np.zeros((3, 3))
        self.dest_array[0, 0] = 1
        self.dest_array[1, 2] = 1

    def test_travel_time_to_area_destination(self):

        actual_output = calculate_accesibility_surface(
            self.friction_array, self.dest_array, res=1000.0, dest_pixel=1
        )

        # Expected output
        expected_output = np.array(
            [0.0, 7000.0, 3000.0, 5500, 4500, 0, 6121.3203, 2121.3203, 5000.0]
        ).reshape(3, 3)

        np.testing.assert_array_almost_equal(expected_output, actual_output, 4)


class PushCSVToDatabase_TravelTimeToTownsTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()
        d = {
            "ADMIN0NAME": ["South Sudan", "South Sudan"],
            "ADMIN0PCOD": ["SS", "SS"],
            "ADMIN1NAME": ["Central Equatoria", "Central Equatoria"],
            "ADMIN1PCOD": ["SS92", "SS92"],
            "ADMIN2NAME": ["Morobo", "Kajo-keji"],
            "ADMIN2PCOD": ["SS9205", "SS9206"],
            "ADMIN2PC_1": ["SS3205", "SS3206"],
            "ADMIN2REFN": ["Morobo", "Kajo-keji"],
            "ADMIN2RE_1": ["Morobo", "Kajo-keji"],
            "ADMIN_1": ["Central Equatoria", "Central Equatoria"],
            "ADMIN_2": ["Morobo", "Kajo-keji"],
            "DATE": ["2008-12-01", "2008-12-01"],
            "SHAPE_AREA": [0.10612485978, 0.20506760264],
            "SHAPE_LENG": [2.04776827249, 1.96151662221],
            "VALIDON": ["2016-01-14", "2016-01-14"],
            "travel_time": [424.537536621094, 470.096710205078],
        }
        self.df = pd.DataFrame(data=d)

    def test_output(self, *args):
        task = PushCSVToDatabase_TravelTimeToTowns(
            visualizations_table="test_accessibility_model"
        )
        with task.input().open("w") as f:
            pd.DataFrame(self.df).to_csv(f, index=False)
        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)
        connection = psycopg2.connect(
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            host=os.environ["PGHOST"],
            port="5432",
            database=os.environ["PGDATABASE"],
        )
        cursor = connection.cursor()
        table_name = "test_accessibility_model"
        cursor.execute(
            psycopg2.sql.SQL("select * from visualization_data.{};").format(
                psycopg2.sql.SQL(table_name)
            )
        )
        records = cursor.rowcount
        self.assertNotEqual(records, 0)
