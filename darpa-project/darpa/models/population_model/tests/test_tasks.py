import datetime
from io import StringIO

import luigi
import pandas as pd
from luigi import Task
from luigi.date_interval import Custom as CustomDateInterval
from luigi.parameter import DateIntervalParameter

from kiluigi.targets import IntermediateTarget
from models.population_model.tasks import EstimatePopulation
from utils.test_utils import LuigiTestCase, MockTarget

counties_mock_target = IntermediateTarget(
    "population_model/ss_census_rates_county.csv", backend_class=MockTarget
)

payams_mock_target = IntermediateTarget(
    "population_model/ss_census_rates_payam.csv", backend_class=MockTarget
)


class global_param_mock(Task):
    time = DateIntervalParameter(
        default=CustomDateInterval(
            datetime.date.fromisoformat("2013-01-01"),
            datetime.date.fromisoformat("2013-06-01"),
        )
    )


class EstimatePopulationTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()

        self.census_data = [
            {
                "GEO_ID": "SS2010",
                "year": 2017,
                "admin0": "South Sudan",
                "growth_rate": 0.01,
                "cdr": 0.03,
                "death_rate": "NaN",
                "cbr": 0.05,
                "birth_rate": "NaN",
                "in_migration": 0,
                "out_migration": 100,
                "admin1": "Central Equatoria",
                "admin2": "Juba",
                "btotl": 1782,
                "b0004": 20,
                "b0509": 20,
                "b1014": 40,
                "b1519": 48,
                "b2024": 100,
                "b2529": 200,
                "b3034": 240,
                "b3539": 300,
                "b4044": 250,
                "b4549": 170,
                "b5054": 100,
                "b5559": 80,
                "b6064": 70,
                "b6569": 50,
                "b7074": 30,
                "b7579": 30,
                "b8084": 20,
                "b8589": 10,
                "b9094": 4,
                "b95pl": 0,
                "ftotl": 1000,
                "mtotl": 782,
            },
        ]

    def test_output(self):
        task = EstimatePopulation()

        with task.input()[0].open("w") as f:
            f.write(pd.DataFrame(self.census_data))

        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)
        output = task.output().open("r").read()
        out_df = pd.read_csv(StringIO(output), sep=",")

        # Check expected columns
        self.assertIn("btotl", out_df.columns)
        self.assertIn("year", out_df.columns)

        # Check that the Total_Population value is sensible
        self.assertTrue(out_df["btotl"].any() > 0)
