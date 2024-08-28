"""Facilitate connection to Google BigQuery"""

import logging
from types import ModuleType

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
import pydata_google_auth
import pandas_gbq as pdgbq

from .secrets import SecretManager

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


class GBQConnection:
    def __init__(self, env: str = None):
        # TODO: use another auth flow for non-local envs
        self.creds = pydata_google_auth.get_user_credentials(SCOPES, auth_local_webserver=True)
        self.secrets = SecretManager(env)
        self.project_id = self.secrets.get_project_id()

    def __connect(self):
        client = bigquery.Client(credentials=self.creds, project=self.project_id)
        return dbapi.Connection(client=client)

    def runDDL(self, sqlScript: str):
        conn = self.__connect()
        cur = conn.cursor()
        logging.debug(sqlScript[:250] + "...")
        cur.execute(sqlScript)
        conn.commit()
        logging.info(f"{cur.rowcount} rows were affected")
        cur.close()
        conn.close()

    def tryDDL(self, sqlScript: str):
        conn = self.__connect()
        try:
            cur = conn.cursor()
            logging.debug(sqlScript[:250] + "...")
            cur.execute(sqlScript)
            conn.commit()
            logging.info(f"{cur.rowcount} rows were affected")
            cur.close()
            conn.close()
        except Exception as e:
            logging.error(e)

    def getTableSchema(self, tableName: str, schemaName: str = "ds_salesforce"):
        """
        Returns schema of given table as data frame.
            Parameters:
                table_name (str): name of table to check.
                schemaName (str): schema to create the table under (default is staging)
            Returns:
                (dataframe) dataframe of columns, and data type for table
        """
        ds_sf_col_sql = f"""SELECT COLUMN_NAME, DATA_TYPE
        FROM {schemaName}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{tableName.lower()}';"""

        return self.runQuery(ds_sf_col_sql)

    def getTablesBySchema(self, schemaName: str):
        """
        Gets list of tables given a schema name.
            Parameters:
                schemaName (str): schema to query for tables
            Returns:
                list of table names in the schema
        """
        sqlQuery = f"""SELECT DISTINCT table_schema,table_name
        FROM {schemaName}.information_schema.tables
        ORDER BY table_name"""

        schemaTables = self.runQuery(sqlQuery)

        tablesList = schemaTables["table_name"].tolist()

        return tablesList

    def runQuery(self, query: str, **kwargs) -> pd.DataFrame:
        conn = self.__connect()
        logging.debug(query[:250])
        return pd.read_sql(query, conn, **kwargs)


def pandas_gbq(env: str = None) -> ModuleType:
    creds = pydata_google_auth.get_user_credentials(SCOPES, auth_local_webserver=True)
    pdgbq.context.credentials = creds

    secrets = SecretManager(env)
    assert secrets.is_local_env, "pandas_gbq only intended for local use"
    pdgbq.context.project = secrets.get_project_id()
    return pdgbq
