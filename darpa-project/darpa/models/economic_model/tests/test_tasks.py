from utils.test_utils import LuigiTestCase
import psycopg2.sql
import os
from models.economic_model.tasks import PushModelResults
import luigi
import pandas as pd


"""
This unittest module will test the economic model
"""

BASE_DIR = os.path.dirname(__file__)


class PushCSVToDatabase_OutputEconomicModelResultsTestCase(LuigiTestCase):
    def setUp(self):
        super().setUp()
        self.df = [{"col1": [1, 2], "col2": [3, 4]}]

    def test_output(self, *args):
        task = PushModelResults(visualizations_table="test_economic_model")
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
        table_name = "test_economic_model"
        cursor.execute(
            psycopg2.sql.SQL("select * from visualization_data.{};").format(
                psycopg2.sql.SQL(table_name)
            )
        )
        records = cursor.rowcount
        self.assertNotEqual(records, 0)

    def tearDown(self):
        connection = psycopg2.connect(
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            host=os.environ["PGHOST"],
            port="5432",
            database=os.environ["PGDATABASE"],
        )
        cursor = connection.cursor()
        table_name = "test_economic_model"
        cursor.execute(
            psycopg2.sql.SQL("drop table visualization_data.{};").format(
                psycopg2.sql.SQL(table_name)
            )
        )
