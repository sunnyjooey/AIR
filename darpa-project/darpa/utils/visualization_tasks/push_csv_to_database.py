import datetime
import os
import re

import luigi
import pandas as pd
import sqlalchemy
from luigi.configuration import get_config

CONFIG = get_config()
VISUALIZATION_DATABASE_SCHEMA = "visualization_data"


def get_sql_datatypes_from_dataframe(df, lst_temporal_fields):
    dtypedict = {}

    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.VARCHAR()})
        if "datetime" in str(j) or str(i) in lst_temporal_fields:
            dtypedict.update({i: sqlalchemy.types.Date()})
        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict


class PushCSVToDatabase(luigi.Task):
    """
    - This Task - meant to be overriden, pushes a CSV file to a Postgres database.
    - One use case for this is to avail it as a data source for Superset visualizations.
    Example usage:
    @requires(RunMalnutritionScenarios)
    class CSVToDatabase_RunMalnutritionScenarios(PushCSVToDatabase):
        temporal_fields = luigi.ListParameter(default=["Time"])
    where : 'RunMalnutritionScenarios' is a task whose output is a CSV file and 'Time' is a temporal field in the CSV.
    If a different database engine is in use other than the internal Docker one, the following parameters may be
    passed to the Task : pg_user, pg_password, pg_host, pg_database
    e.g
    @requires(RunMalnutritionScenarios)
    class CSVToDatabase_RunMalnutritionScenarios(PushCSVToDatabase):
        pg_user = "hussein"
        pg_password = "huss2019"
        pg_host = "3.99.529.18"
        pg_database = "darpa"
    """

    temporal_fields = luigi.ListParameter(default=[], significant=False)
    pg_user = luigi.Parameter(default=os.environ.get("PGUSER", ""), significant=False)
    pg_password = luigi.Parameter(
        default=os.environ.get("PGPASSWORD", ""), significant=False
    )
    pg_host = luigi.Parameter(default=os.environ.get("PGHOST", ""), significant=False)
    pg_port = 5432
    pg_database = luigi.Parameter(
        default=os.environ.get("PGDATABASE", ""), significant=False
    )
    visualizations_table = luigi.Parameter(default="", significant=False)
    lst_param_names = []

    def run(self):
        self.database_engine = "postgresql://{}:{}@{}:{}/{}".format(
            self.pg_user, self.pg_password, self.pg_host, self.pg_port, self.pg_database
        )

        with self.input().open("r") as f:
            df = pd.read_csv(f)
        df["task_id"] = self.task_id
        df["run_date_time"] = datetime.datetime.utcnow()

        # This gets a list of fields that are temporal so that when pushing to the DB the correct data type can be used.
        lst_temporal_fields = []
        for field in self.temporal_fields:
            field_name = self.temporal_fields[self.temporal_fields.index(field)]
            field_name = field_name.lower()
            # replacing parentheses with _ due to this bug: https: // github.com / psycopg / psycopg2 / issues / 542
            # also replacing other non database friendly characters with _
            field_name = re.sub("[(): ,']", "_", field_name)
            lst_temporal_fields.append(field_name)

        # Add all of the significant parameters including the Scenario related parameters
        params = dict(self.get_params())
        self.lst_param_names = []
        for param_name, param_value in self.param_kwargs.items():
            if params[param_name].significant:
                param_name = "model_parameter_{}".format(param_name.lower())
                param_name = re.sub("[(): ,']", "_", param_name)
                data = ["{}".format(param_value)] * len(df.index)
                s = pd.Series(data)
                df[param_name] = s.values

        df.columns = df.columns.str.lower()
        # Replacing parentheses with _ due to this bug: https: // github.com / psycopg / psycopg2 / issues / 542
        df.columns = df.columns.str.replace("[(): ,']", "_")

        # The database table is named after the name of the model. We can obtain that from the task_id Task ID is
        # typically in this form :
        # models.malnutrition_model.tasks
        # .CSVToDatabase_MalnutritionModel___coordinates______2017_05_01______2017_01_01_____d6e88567ab
        if len(self.visualizations_table) == 0:
            if "." in self.task_id:
                self.visualizations_table = self.task_id.split(".")[1]
            else:
                self.visualizations_table = self.task_id

        # Determine the datatype of the table column based on the dataframe column (
        # https://stackoverflow.com/questions/34383000/pandas-to-sql-all-columns-as-nvarchar)
        dict_datatypes = get_sql_datatypes_from_dataframe(df, lst_temporal_fields)
        dict_datatypes["visualization_data_row_id"] = sqlalchemy.types.INT()

        df["visualization_data_row_id"] = df.index

        df.to_sql(
            self.visualizations_table,
            self.database_engine,
            VISUALIZATION_DATABASE_SCHEMA,
            if_exists="append",
            index=False,
            dtype=dict_datatypes,
        )
