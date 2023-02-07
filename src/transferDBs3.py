import psycopg2
import pandas as pd
import boto3
from s3Ops import S3Connection


class RedShiftConnection:
    def __init__(self, database, userName, password, host):
        self.database = database
        self.userName = userName
        self.password = password
        self.host = host

    def runDDL(self, sqlScript):
        conn = psycopg2.connect(
            database=self.database,
            user=self.userName,
            password=self.password,
            host=self.host,
            port="5432",
        )
        cur = conn.cursor()
        print(sqlScript[:250] + "...")
        cur.execute(sqlScript)
        conn.commit()
        print(f"{cur.rowcount} rows were affected")
        cur.close()
        conn.close()

    def tryDDL(self, sqlScript):
        conn = psycopg2.connect(
            database=self.database,
            user=self.userName,
            password=self.password,
            host=self.host,
            port="5432",
        )
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
        conn = psycopg2.connect(
            database=self.database,
            user=self.userName,
            password=self.password,
            host=self.host,
            port="5432",
        )

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

    def truncTransferFromStaging(self, newSchema, table, stagingSchema="staging"):
        print(f"Moving {table} from {stagingSchema} to {newSchema}")
        sql = f"""begin transaction;
            truncate table {newSchema}.{table};
            insert into {newSchema}.{table} (select * from {stagingSchema}.{table});
            drop table {stagingSchema}.{table};
        end transaction;
        """
        self.redShiftConn.runDDL(sql)

    # Upsert i.e. replace modified records (Upsert = update & insert)
    def upsert_records(
        self, schemaName="ds_salesforce", staging_schemaName="staging", tableName=""
    ):
        upsert_sql = f"""
            begin transaction;
                delete from {schemaName}.{tableName}
                    using {staging_schemaName}.{tableName}
                    where {schemaName}.{tableName}.id = {staging_schemaName}.{tableName}.id;
                insert into {schemaName}.{tableName}
                    select * from {staging_schemaName}.{tableName}
                    where {staging_schemaName}.{tableName}.isdeleted=0;
                drop table {staging_schemaName}.{tableName};
            end transaction;
            """
        self.redShiftConn.runDDL(upsert_sql)

    def dropReplaceTables(self, oldSchema, newSchema, tableName):
        dropReplaceSQL = f"""begin transaction;
            DROP TABLE IF EXISTS {newSchema}.{tableName};
            CREATE TABLE IF NOT EXISTS {newSchema}.{tableName} (like {oldSchema}.{tableName});
            INSERT INTO {newSchema}.{tableName} (SELECT * FROM {oldSchema}.{tableName});
            DROP TABLE {oldSchema}.{tableName};
        end transaction;
        """
        self.redShiftConn.runDDL(dropReplaceSQL)


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

    def getTablesBySchema(self, schemaName):
        sqlQuery = f"""
        select DISTINCT table_schema,table_name
        from information_schema.tables
        WHERE table_schema = '{schemaName}'
        ORDER BY table_name
        """

        schemaTables = self.redShiftConn.runQuery(sqlQuery)

        tablesList = schemaTables["table_name"].tolist()

        return tablesList

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
