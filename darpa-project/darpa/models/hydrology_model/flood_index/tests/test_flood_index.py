import numpy as np
import richdem as rd
import pandas as pd
import psycopg2
import luigi
import os

from models.hydrology_model.flood_index.tasks import (
    UpstreamRunoffMixin,
    PushCSVToDatabase_EuropeanRunoffIndex,
)
from utils.test_utils import LuigiTestCase


class UpstreamRunoffLongtermAverageTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()
        self.surface_array = rd.rdarray(
            np.array(
                [
                    0.8,
                    0.4,
                    0.2,
                    0.5,
                    1.1,
                    -9999.0,
                    0.9,
                    0.7,
                    0.3,
                    0.5,
                    0.8,
                    0.7,
                    0,
                    -9999.0,
                    0,
                    1.2,
                ]
            ).reshape(4, 4),
            no_data=-9999.0,
        )

        self.subsurface_array = rd.rdarray(
            np.array(
                [
                    0.7,
                    0.2,
                    0.5,
                    0.7,
                    0.4,
                    0.1,
                    -9999.0,
                    0,
                    0.3,
                    1.1,
                    0.2,
                    0.7,
                    0.1,
                    0,
                    0.2,
                    0.1,
                ]
            ).reshape(4, 4),
            no_data=-9999.0,
        )
        self.rain_fall = rd.rdarray(
            np.array(
                [20, 0, -9999.0, 40, 30, 0, 30, 0, 55, 100, 0, -9999.0, 150, 20, 30, 1]
            ).reshape(4, 4),
            no_data=-9999.0,
        )
        dem_array = np.linspace(10, 34, 16).reshape(4, 4)
        dem_array[1, 1] = -9999.0
        self.dem_rdarray = rd.rdarray(dem_array, no_data=-9999.0)
        self.upstream_cells = np.array(
            [
                1.0,
                3.0,
                1.0,
                1.0,
                2,
                -1,
                2.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
                1.0,
            ]
        ).reshape(4, 4)

        self.runoff_map = {}
        self.runoff_map["20010101"] = np.array(
            [
                0.0001,
                -9999,
                0.0012,
                0.0,
                -9999,
                0.0,
                0.0002,
                0.0,
                0.00001,
                0.00005,
                0,
                0.0,
                0.0001,
                0.0003,
                -9999.0,
                -9999.0,
            ]
        ).reshape(4, 4)

        self.runoff_map["20010102"] = np.array(
            [
                0.0002,
                0.0001,
                -9999.0,
                0,
                0.00012,
                0.0003,
                -9999.0,
                0.0001,
                -9999.0,
                0.0003,
                -9999.0,
                0.0002,
                -9999.0,
                0.00015,
                -9999.0,
                -9999.0,
            ]
        ).reshape(4, 4)

        self.runoff_map["20010103"] = np.array(
            [
                0.00003,
                0.0001,
                -9999.0,
                -9999.0,
                0.0002,
                0.0015,
                0.0003,
                -9999.0,
                0.0001,
                -9999.0,
                0.0003,
                0.0012,
                0.0,
                0.00032,
                0.00001,
                -9999.0,
            ]
        ).reshape(4, 4)

    def test_runoffcoefficient(self):
        expected_result = np.array(
            [
                0.075,
                0,
                -9999.0,
                0.03,
                0.05,
                -9999.0,
                -9999.0,
                0,
                0.010909,
                0.016,
                0,
                -9999.0,
                0.000666666,
                -9999.0,
                0.006666666666666667,
                1.3,
            ]
        ).reshape(4, 4)
        computed_result = UpstreamRunoffMixin().runoffcoefficient(
            self.surface_array, self.subsurface_array, self.rain_fall
        )
        np.testing.assert_array_almost_equal(computed_result, expected_result, 4)

    def test_upstream_runoff(self):
        expected_result = np.array(
            [
                129600.0,
                0.0,
                0.0,
                103680.0,
                133920.0,
                -9999.0,
                0.0,
                0.0,
                51840.0,
                138240.0,
                0.0,
                0.0,
                8640.0,
                0.0,
                17280.0,
                112320.0,
            ]
        ).reshape(4, 4)
        computed_result = UpstreamRunoffMixin().upstream_runoff(
            self.dem_rdarray,
            self.rain_fall,
            self.surface_array,
            self.subsurface_array,
            self.upstream_cells,
            flow_method="D8",
        )
        np.testing.assert_array_almost_equal(computed_result, expected_result, 4)


class PushCSVToDatabase_EuropeanRunoffIndexTestCase(LuigiTestCase):
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
            "eric": [0.013043199665845, 1.39289422804723e-05],
        }
        self.df = pd.DataFrame(data=d)

    def test_output(self, *args):
        task = PushCSVToDatabase_EuropeanRunoffIndex(
            visualizations_table="test_hydrology_model"
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
        table_name = "test_hydrology_model"
        cursor.execute(
            psycopg2.sql.SQL("select * from visualization_data.{};").format(
                psycopg2.sql.SQL(table_name)
            )
        )
        records = cursor.rowcount
        self.assertNotEqual(records, 0)
