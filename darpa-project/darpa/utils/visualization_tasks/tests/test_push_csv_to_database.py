from utils.test_utils import LuigiTestCase
from utils.visualization_tasks.push_csv_to_database import (
    PushCSVToDatabase,
    get_sql_datatypes_from_dataframe,
)
import pandas as pd
import sqlalchemy
import luigi
from luigi.util import requires
import psycopg2
import psycopg2.sql
import os


class DummyCSVFileTask(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget("students.csv")

    def run(self):
        data = {
            "student_name": ["Jane", "Tom", "Pam"],
            "age": [23, 21, 22],
            "date_of_registration": ["12/10/2019", "12/02/2019", "12/11/2019"],
        }
        df = pd.DataFrame(data, columns=["student_name", "age", "date_of_registration"])
        df.to_csv(self.output().path, index=False)


@requires(DummyCSVFileTask)
class PushStudentsDataToDatabase(PushCSVToDatabase):
    temporal_fields = luigi.ListParameter(default=["date_of_registration"])


class PushCSVToDatabaseTestCase(LuigiTestCase):
    def test_get_sql_datatypes_from_dataframe(self):
        data = {
            "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
            "Price": [22000, 25000, 27000, 35000],
            "DateColumn": ["15/02/2019", "20/02/2019", "25/02/2019", "10/02/2019"],
            "PerformanceIndex": [4.3, 56.9, 1.0, 67.3],
        }
        df = pd.DataFrame(
            data, columns=["Brand", "Price", "DateColumn", "PerformanceIndex"]
        )
        df = df.astype(
            dtype={
                "Brand": "object",
                "Price": "int64",
                "DateColumn": "object",
                "PerformanceIndex": "float",
            }
        )
        lst_temporal_fields = ["DateColumn"]
        dtypedict = get_sql_datatypes_from_dataframe(df, lst_temporal_fields)
        assert isinstance(dtypedict["Brand"], type(sqlalchemy.types.VARCHAR()))
        assert isinstance(dtypedict["Price"], type(sqlalchemy.types.INT()))
        assert isinstance(dtypedict["DateColumn"], type(sqlalchemy.types.Date()))
        assert isinstance(
            dtypedict["PerformanceIndex"],
            type(sqlalchemy.types.Float(precision=3, asdecimal=True)),
        )

    def setUp(self):
        super().setUp()

    def test_output(self):
        task = PushStudentsDataToDatabase()
        luigi.build([task], local_scheduler=True, no_lock=True, workers=1)
        # check for existence of the table
        connection = psycopg2.connect(
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            host=os.environ["PGHOST"],
            port="5432",
            database=os.environ["PGDATABASE"],
        )
        cursor = connection.cursor()
        table_name = str(task.visualizations_table)
        cursor.execute(
            psycopg2.sql.SQL("select * from visualization_data.{};").format(
                psycopg2.sql.Identifier(table_name)
            )
        )
        records = cursor.rowcount
        self.assertNotEqual(records, 0)
