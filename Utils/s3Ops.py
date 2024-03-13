"""Transfers data to and from s3"""

from typing import List
import tempfile
import json
import s3fs
import boto3
import io
import re
import pandas as pd


class S3Connection:
    def __init__(self, aws_bucket, aws_access_key, aws_secret_key):
        self.aws_bucket = aws_bucket
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name="us-east-1",
        )

        self.client = boto3.client(
            "s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
        )

    def saveDataFrametoS3(
        self, data_frame, directory_to_save, file_name, delimiter="|", header=False
    ):
        """This method will take a DataFrame and save it to the according directory with a given file
        name.

        @param data_frame: The Data Frame that we hope to save
        @param directory_to_save: The directory to which the dataFrame will be saved to
        @param file_name: What the fileName will be called
        @return: None will be returned as we are pushing the dataframe to S3.
        """

        if ".gz" in file_name:
            print(f"Compressing File {file_name}")
            data_frame.to_csv(
                file_name,
                index=False,
                header=header,
                sep=delimiter,
                chunksize=1000000,
                compression={"method": "gzip", "compresslevel": 1},
            )
            self.client.upload_file(
                file_name, self.aws_bucket, f"{directory_to_save}/{file_name}"
            )
        else:
            csv_buffer = io.StringIO()
            data_frame.to_csv(csv_buffer, sep=delimiter, index=False, header=header)
            content = csv_buffer.getvalue()
            s3 = self.session.resource("s3")

            object = s3.Object(
                self.aws_bucket, "%s/%s" % (directory_to_save, file_name)
            )
            object.put(Body=content)

    def saveRawFiletoS3(self, content, directory_to_save, file_name):
        # csv_buffer = StringIO()
        # content = csv_buffer.getvalue()

        s3 = self.session.resource("s3")

        object = s3.Object(self.aws_bucket, "%s/%s" % (directory_to_save, file_name))
        object.put(Body=content)

    def saveJsontoS3(self, list_json_to_save, directory_to_save, file_name):
        """ """

        def convert(name):
            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name.replace(" ", "_"))
            return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

        tmp = tempfile.NamedTemporaryFile()

        with open(tmp.name, "w") as f:
            # [f.write(json.dumps(json_)) for json_ in list_json_to_save]
            for json_ in list_json_to_save:
                f.write(json.dumps({convert(_[0]): _[1] for _ in json_.items()}))
                f.write("\n")

        s3 = self.session.resource("s3")
        # s3 = self.client
        bucket = s3.Bucket(self.aws_bucket)
        key = bucket.Object("%s/%s.json" % (directory_to_save, file_name))

        with open(tmp.name, "rb") as f:
            key.upload_fileobj(f)

    def saveRawJsonToS3(self, jsonDic, directory_to_save, file_name):
        s3 = self.session.resource("s3")
        object = s3.Object(self.aws_bucket, "%s/%s" % (directory_to_save, file_name))
        object.put(Body=(bytes(json.dumps(jsonDic).encode("UTF-8"))))

    def partitionDfSaveS3(
        self, data_frame, directory_to_save, file_to_save, date_loaded, header=False
    ):
        """
        This method saves a dataFrame into partitions. First, we format the date_time column within
        the dataset. From there we group the data (which is the partition values). We loop through
        the groupings to save each partition values.


            @param data_frame: The Data Frame that we hope to save
            @param directory_to_save: The directory in which we want to save the data
            @param date_loaded: The load day; this will serve as the first partition value.
            @return: None will be returned as we are pushing the dataframe to S3.
        """

        def formatColumnsToLower(data_frame):
            data_frame_column_formatted = [
                column.lower().replace(" ", "_") for column in data_frame.columns
            ]
            data_frame.columns = data_frame_column_formatted

        formatColumnsToLower(data_frame)

        # print(data_frame.dtypes)

        self.saveFiletoS3(
            data_frame,
            "%s/%s/ds=%s" % (directory_to_save, file_to_save, date_loaded),
            "%s.csv" % file_to_save,
            header,
        )

    def loadCsv(self, directory, fileName, headers=True) -> pd.DataFrame:
        s3 = self.client
        obj = s3.get_object(Bucket=self.aws_bucket, Key="%s/%s" % (directory, fileName))

        return (
            pd.read_csv(obj["Body"])
            if headers
            else pd.read_csv(obj["Body"], header=headers)
        )

    def loadMultipleCsv(self, directory, sub_dir):
        s3 = self.client
        s3_resource = self.session.resource("s3")
        my_bucket = s3_resource.Bucket(self.aws_bucket)
        response = [
            file
            for file in my_bucket.objects.filter(Prefix="%s/%s/" % (directory, sub_dir))
        ]
        df_list = []
        for file in response:
            obj = s3.get_object(Bucket=self.aws_bucket, Key=file.key)
            obj_df = pd.read_csv(obj["Body"])
            df_list.append(obj_df)
        return pd.concat(df_list)

    def loadMultipleCsvList(self, directory):
        s3 = self.client
        s3_resource = self.session.resource("s3")
        my_bucket = s3_resource.Bucket(self.aws_bucket)
        response = [
            file for file in my_bucket.objects.filter(Prefix="%s/" % (directory))
        ]
        response = [file for file in response if "csv" in file.key]

        obj = s3.get_object(Bucket=self.aws_bucket, Key=response[0].key)
        obj_df_inital = pd.read_csv(obj["Body"])
        cols = obj_df_inital.columns
        print("initial cols", cols)

        df_list = []
        for file in response:
            obj = s3.get_object(Bucket=self.aws_bucket, Key=file.key)
            obj_df = pd.read_csv(obj["Body"])
            obj_df = obj_df[cols]
            obj_df["vendor"] = file.key.split("/")[1]
            df_list.append(obj_df)

        return pd.concat(df_list)

    def loadCsvSkipRows(self, directory, fileName, skipRows) -> pd.DataFrame:
        s3 = self.client
        obj = s3.get_object(Bucket=self.aws_bucket, Key="%s/%s" % (directory, fileName))
        return pd.read_csv(obj["Body"], skiprows=skipRows)

    def saveDFToPartition(self, data, directoryPath):
        fss3 = s3fs.S3FileSystem(
            anon=False, key=self.aws_access_key, secret=self.aws_secret_key
        )
        myopen = fss3.open
        # fileName = f'{self.aws_bucket}/{directoryPath}/ds={dataloaded}/filename.parq.gzip'
        fileName = f"{self.aws_bucket}/{directoryPath}/data.parq.gzip"

        write(
            fileName,
            data,
            compression="GZIP",
            open_with=myopen,
            object_encoding="utf8"
            # object_encoding={'batch_date_transaction_amount_grouped': 'json'}
        )

    def loadTxtFile(self, directory, fileName) -> str:
        s3 = self.client
        obj = s3.get_object(Bucket=self.aws_bucket, Key="%s/%s" % (directory, fileName))
        return obj["Body"].read().decode("utf-8")

    def getFilesFromDirectory(self, directoryPrefix) -> List[str]:
        s3_resource = self.session.resource("s3")
        my_bucket = s3_resource.Bucket(self.aws_bucket)
        return [
            object_summary.key
            for object_summary in my_bucket.objects.filter(
                Prefix="%s/" % directoryPrefix
            )
        ]

    def downloadFile(self, objectname, filename):
        self.client.download_file(self.aws_bucket, objectname, filename)
