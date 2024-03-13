"""Transfers data to, from, and within Redshift and s3"""

import psycopg2
import pandas as pd
import boto3
from s3Ops import S3Connection
from psycopg2.errors import DependentObjectsStillExist
from collections.abc import Iterable
import requests
import numpy as np


class RedShiftConnection:
    def __init__(self, database, userName, password, host):
        self.database = database
        self.userName = userName
        self.password = password
        self.host = host
    
    def __connect(self):
        return psycopg2.connect(
            database=self.database,
            user=self.userName,
            password=self.password,
            host=self.host,
            port="5432",
        )

    def runDDL(self, sqlScript):
        conn = self.__connect()
        cur = conn.cursor()
        print(sqlScript[:250] + "...")
        cur.execute(sqlScript)
        conn.commit()
        print(f"{cur.rowcount} rows were affected")
        cur.close()
        conn.close()

    def tryDDL(self, sqlScript):
        conn = self.__connect()
        try:
            cur = conn.cursor()
            print(sqlScript[:250] + "...")
            cur.execute(sqlScript)
            conn.commit()
            print(f"{cur.rowcount} rows were affected")
            cur.close()
            conn.close()
        except Exception as e:
            print(e)

    def getTableSchema(self, tableName, schemaName="ds_salesforce"):
        """
        Returns schema of given table as data frame.
            Parameters:
                table_name (str): name of table to check.
                schemaName (str): schema to create the table under (default is staging)
            Returns:
                (dataframe) dataframe of columns, and data type for table
        """
        ds_sf_col_sql = f"""SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE
        TABLE_SCHEMA = '{schemaName}' AND
        TABLE_NAME   = '{tableName.lower()}';"""

        return self.runQuery(ds_sf_col_sql)

    def runQuery(self, query) -> pd.DataFrame:
        conn = self.__connect()
        print(query[:250])
        return pd.read_sql(query, conn)


class moveTableToSchema:
    def __init__(
        self, redshift_database, redshift_username, redshift_password, redshift_host
    ):
        self.redshift_database = redshift_database
        self.redshift_username = redshift_username
        self.redshift_password = redshift_password
        self.redshift_host = redshift_host

        self.redShiftConn = RedShiftConnection(
            database=self.redshift_database,
            userName=self.redshift_username,
            password=self.redshift_password,
            host=self.redshift_host,
        )

    def getTablesBySchema(self, schemaName: str):
        """
        Gets list of tables given a schema name.
            Parameters:
                schemaName (str): schema to query for tables
            Returns:
                list of table names in the schema
        """
        sqlQuery = f"""SELECT DISTINCT table_schema,table_name
        from information_schema.tables
        WHERE table_schema = '{schemaName}'
        ORDER BY table_name"""

        schemaTables = self.redShiftConn.runQuery(sqlQuery)

        tablesList = schemaTables["table_name"].tolist()

        return tablesList

    def truncTransferFromStaging(self, newSchema, table, stagingSchema="staging"):
        """
        Truncates table and transfers from staging schema (default staging) to a new schema using a sql transaction.
        Used to replace entire table contents without needing to drop the table so it keeps it's structure and dependencies.
            Parameters:
                newSchema (str): schema the data is getting moved to
                stagingSchema (str): staging schema where the data is getting moved from
                table (str): name of the table getting updated
        """
        print(f"Moving {table} from {stagingSchema} to {newSchema}")
        sql = f"""begin transaction;
            truncate table {newSchema}.{table};
            insert into {newSchema}.{table} (select * from {stagingSchema}.{table});
            drop table {stagingSchema}.{table};
        end transaction;
        """
        self.redShiftConn.runDDL(sql)

    def upsert_records(self, schemaName="ds_salesforce", staging_schemaName="staging", tableName="", id='id', delete=True):
        """
        Upserts (update + insert) records in a given table using a sql transaction.
            Parameters:
                schemaName (str): schema the data is getting moved to
                staging_schemaName (str): schema where the new data staged
                tableName (str): name of the table getting updated
        """
        if delete:
            delete_sql = f' where {staging_schemaName}.{tableName}.isdeleted=0;'
        else:
            delete_sql = ';'
        upsert_sql = f"""
            begin transaction;
                delete from {schemaName}.{tableName}
                    using {staging_schemaName}.{tableName}
                    where {schemaName}.{tableName}.{id} = {staging_schemaName}.{tableName}.{id};
                insert into {schemaName}.{tableName}
                    select * from {staging_schemaName}.{tableName}{delete_sql}
                drop table {staging_schemaName}.{tableName};
            end transaction;
            """
        self.redShiftConn.runDDL(upsert_sql)

    def findDependentObj(self, schemaName: str, tableName: str) -> np.ndarray:
        """
        Finds any dependent objects (views) of a given table.
        Useful when doing a drop cascade to see what is getting dropped.
            Parameters:
                schemaName (str): schema the data is getting moved to
                tableName (str): name of the table getting updated

            Returns:
                list of table names in the schema
        """
        find_dependent_tbls_query = f"""SELECT dependent_ns.nspname as dependent_schema
        , dependent_view.relname as dependent_view
        FROM pg_depend
        JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
        JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
        JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
        JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
            AND pg_depend.refobjsubid = pg_attribute.attnum
        JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
        JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
        WHERE
        source_ns.nspname = '{schemaName.lower()}'
        AND source_table.relname = '{tableName.lower()}'
        AND pg_attribute.attnum > 0
        ORDER BY 1,2;
        """
        df = self.redShiftConn.runQuery(find_dependent_tbls_query)
        df['schema.table'] = df['dependent_schema'] + '.' + df['dependent_view']
        return df['schema.table'].unique()

    def dropReplaceTables(self, oldSchema, newSchema, tableName, cascade=False):
        """
        Drop and replaces a given table in a given schema using a sql transaction.
        Will recreate views if cascade is set to True.
        Useful for when a table has schema changes.
            Parameters:
                newSchema (str): schema the data is getting moved to
                oldSchema (str): schema where the data is getting moved from
                tableName (str): name of the table getting updated
                cascade (bool): do drop cascade?

        """
        def recreateViews(views_to_recreate: Iterable):
            """
            Recreates views using a stored procedure. Sends a slack alert if the procedure is not found.
                Parameters:
                    views_to_recreate (Iterable): list (or Iterable) of views to recreate
            """
            for view in views_to_recreate:
                try:
                    print(f'Recreating {view}')
                    schema, v = view.split('.')
                    self.redShiftConn.runDDL(f'CALL {schema}.create_{v}();')
                except Exception as e:
                    print(e)
                    headers = {'Content-type': 'application/json'}
                    response = requests.post(
                        'https://hooks.slack.com/services/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',
                        headers=headers,
                        json={'text': f'{e}'}
                    )
                    if response.status_code != 200:
                        print(f'[ERROR] Could not send Slack Message. Response Status Code is {response.status_code}.')
        dropReplaceSQL = f"""begin transaction;
            DROP TABLE IF EXISTS {newSchema}.{tableName};
            CREATE TABLE IF NOT EXISTS {newSchema}.{tableName} (like {oldSchema}.{tableName});
            INSERT INTO {newSchema}.{tableName} (SELECT * FROM {oldSchema}.{tableName});
            DROP TABLE {oldSchema}.{tableName};
        end transaction;
        """
        try:
            self.redShiftConn.runDDL(dropReplaceSQL)
        except DependentObjectsStillExist as e:
            print(f"WARNING: Dependent Objects still exist. Cascade is set to {cascade}.\n")
            print(e)
            if cascade:
                views_to_recreate = self.findDependentObj(newSchema, tableName)
                dropCascadeSQL = f"""begin transaction;
                    DROP TABLE IF EXISTS {newSchema}.{tableName} CASCADE;
                 {dropReplaceSQL[dropReplaceSQL.find('CREATE'):]}"""
                self.redShiftConn.runDDL(dropCascadeSQL)
                print(f"Will recreate {len(views_to_recreate)} views: {views_to_recreate}")
                recreateViews(views_to_recreate)


class SaveSchemaToS3:
    def __init__(
        self,
        aws_bucket,
        aws_access_key,
        aws_secret_key,
        redshift_database,
        redshift_username,
        redshift_password,
        redshift_host,
    ):
        self.aws_bucket = aws_bucket
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name="us-east-1",
        )
        self.redshift_database = redshift_database
        self.redshift_username = redshift_username
        self.redshift_password = redshift_password
        self.redshift_host = redshift_host

        self.redShiftConn = RedShiftConnection(
            database=self.redshift_database,
            userName=self.redshift_username,
            password=self.redshift_password,
            host=self.redshift_host,
        )


    def saveSchemaTableToS3(self, dataFrame, schemaName, tableName):
        s3Conn = S3Connection(self.aws_bucket, self.aws_access_key, self.aws_secret_key)

        s3Conn.saveFiletoS3(dataFrame, "%s" % schemaName, tableName, True)

    def saveMultipleSchemasToS3(self, schemaNameList):
        for schemaName in schemaNameList:
            tableNameList = self.getTablesBySchema(schemaName)
            for table in tableNameList:
                getData = self.redShiftConn.runQuery(
                    "SELECT * FROM %s.%s" % (schemaName, table)
                )
                # print(getData.head())
                print(f"{schemaName}.{table}")
                self.saveSchemaTableToS3(getData, schemaName, table)

    def saveBigTabletoS3(self, schemaName, table, numPartitions):
        getData = self.redShiftConn.runQuery(f"SELECT * FROM {schemaName}.{table}")
        partitonSize = getData.shape[0] // numPartitions
        print(
            f"{schemaName}.{table} Row Count: {getData.shape[0]}, Partition Size: {partitonSize}"
        )

        for i in range(numPartitions):
            print(i)
            filename = table + "_0" + str(i)
            if i == (numPartitions - 1):
                self.saveSchemaTableToS3(getData, schemaName, filename)
            else:
                df = getData.head(partitonSize)
                getData.drop(df.index[:partitonSize], inplace=True)
                self.saveSchemaTableToS3(df, schemaName, filename)
