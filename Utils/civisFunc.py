"""Utility functions and wrapper functions for the Civis API"""

import requests
import json
from datetime import datetime
import civis
import pandas as pd
from civis.base import EmptyResultError, CivisJobFailure
import os


class api:
    def __init__(self, civisToken):
        self.civisToken = civisToken
        self.urlPath = "https://api.civisanalytics.com/"

    def headers(self):
        header = {}
        header["Content-type"] = "application/json"
        header["Authorization"] = f"Bearer {self.civisToken}"
        return header

    def queryGet(self, url):
        response = requests.get(url=f"{self.urlPath}/{url}", headers=self.headers())
        return json.loads(response.content)

    def queryPost(self, url, postBody):
        response = requests.post(
            url=f"{self.urlPath}/{url}",
            headers=self.headers(),
            data=json.dumps(postBody),
        )

        return json.loads(response.content)

    def queryPut(self, url, postBody):
        response = requests.put(
            url=f"{self.urlPath}/{url}",
            headers=self.headers(),
            data=json.dumps(postBody),
        )
        print(f"status_code: {response.status_code}")

        return json.loads(response.content)


class util:
    def __init__(self):
        pass

    def getData(self, sqlQuery) -> pd.DataFrame:
        """

        Using Civis API to obtain the query that was sent.


            @param sqlQuery: sqlQuery.
            @return: A DataFrame from the query requested.

        """

        db = os.getenv("DB_NAME")

        try:
            print("Running Query....")
            print(sqlQuery)

            return civis.io.read_civis_sql(sqlQuery, database=db, use_pandas=True)
        except EmptyResultError:
            print("No Data Retrieved .. ")
            raise
            # return pd.DataFrame()

        except CivisJobFailure as jobError:
            print(jobError)
            raise

    def executeScript(self, sqlQuery):
        """

        Using Civis API to obtain the query that was sent.


            @param sqlQuery: sqlQuery.
            @return: A DataFrame from the query requested.

        """

        db = os.getenv("DB_NAME")

        print("Running Query....")
        print(sqlQuery)

        return civis.io.read_civis_sql(sqlQuery, database=db, use_pandas=False)

    def saveDataFrameToCivisRedshift(
        self, dataFrame, dataFrameName, existingRows="drop"
    ):
        """

        Saving a DataFrame to Civis using their API


            @param dataFrame: Data Frame you wish to save.
            @param dataFrameName: The name of the DataFrame.
            @param existingRows: What to do when table already exist. Default is Drop
            @return: A saved Table to the Data Base.

        """

        db = os.getenv("DB_NAME")

        return civis.io.dataframe_to_civis(
            dataFrame,
            database=db,
            table=dataFrameName,
            existing_table_rows=existingRows,
            headers=True,
        )

    def getCivisFileUrl(self, filename, dataFrame, expireTime):
        """

        Saving a DataFrame to S3 and the saved s3 link.

            @param filename: Name of the File which we want the s3 to
            @param dataFrame: The DataFrame to save.
            @param expireTime: The amount of seconds before the s3 link expires.
            @return: A url of the save s3 link.

        """

        client = civis.APIClient()

        dataFrame.to_csv(str(filename) + "_report.csv", index=False)
        filetocivis = civis.io.file_to_civis(
            str(filename) + "_report.csv", filename, expires_at=None
        )
        my_file = client.files.get(filetocivis, link_expires_at=expireTime)

        os.remove(str(filename) + "_report.csv")

        return my_file["file_url"]

    def sendEmail(self, subject, body, emailAddressList, patch_id=126048778):
        """
        Function to send an email

            @param subject: The title of the email
            @param body: The body of the email
            @param emailAddressList: A list of emails to send the message to
            @return: Id of the email sent.

        """

        client = civis.APIClient()
        my_script = client.scripts.patch_sql(
            patch_id,
            notifications={
                "success_email_subject": subject,
                "success_email_body": body,
                "success_email_addresses": emailAddressList,
            },
        )

        print("Email sent to " + ", ".join(emailAddressList))
        return client.scripts.post_run(my_script.id)

    def saveDataFrame(self, dataFrame, dataFrameName):
        return self.saveDataFrameToCivisRedshift(dataFrame, dataFrameName)

    def saveDataFrameAndGetFileUrl(self, dataFrame, dataFrameName):
        """

        Save a DataFrame to s3 and return the s3 url alongside the length of each dataframe

            @param dataFrame: DataFrame to Save
            @param dataFrameName: The DataFrame Name which we want to save the table to
            @return: Id of the email sent.

        """

        expires_time = datetime.datetime.now() + datetime.timedelta(hours=36)

        self.saveDataFrameToCivisRedshift(dataFrame, dataFrameName)

        tableName = dataFrameName.split(".")[1]

        filename = tableName + "_" + str(datetime.datetime.now().date()) + ".csv"

        print("DataFrame Saved ....")

        s3Url = self.getCivisFileUrl(filename, dataFrame, expires_time)

        return (
            "\n\nThere are %s entries part of \n %s. The url is: %s. \nThe SQL Code is: SELECT * FROM %s"
            % (len(dataFrame), dataFrameName, s3Url, dataFrameName)
        )

    def saveDataFrameAndGetFileUrlCsv(self, dataFrame, dateFrameInCivis, filename):
        """

        Save a DataFrame to s3 and return the s3 url alongside the length of each dataframe

            @param dataFrame: DataFrame to Save
            @param dataFrameName: The DataFrame Name which we want to save the table to
            @return: Id of the email sent.

        """

        expires_time = datetime.datetime.now() + datetime.timedelta(hours=36)

        self.saveDataFrameToCivisRedshift(dataFrame, dateFrameInCivis)

        print("DataFrame Saved ....")

        s3Url = self.getCivisFileUrl(filename, dataFrame, expires_time)

        return (
            "\n\nThere are %s entries part of \n %s. The url is: %s. \nThe SQL Code is: SELECT * FROM %s"
            % (len(dataFrame), dateFrameInCivis, s3Url, dateFrameInCivis)
        )

    def commentCivisTable(self, schema, table_name, dic_column_and_comment):
        db = os.getenv("DB_NAME")
        query_list = [
            f"""comment on column {schema}.{table_name}.{key} is '{dic_column_and_comment[key]}';"""
            for key in dic_column_and_comment.keys()
        ]
        query = "\n".join(query_list)

        print(query)
        update_columns_comments = civis.io.query_civis(query, database=db)

        return update_columns_comments