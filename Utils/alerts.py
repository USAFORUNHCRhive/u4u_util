"""This file sends alerts via email or slack"""

import boto3
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def create_email(
        subject, body, email_list, filename=None, attachment_dataframe=None
    ):
        """
        This method is using ses to send an email. We use a domain created by AWS Route 53; which was verified usingn ses.
        Once the domain is verified, this method can send emails to any domains. Any email can be attributed to the
        verification domain if its created by domain53.

        :param subject: Email Subject
        :param body: Email body
        :param email_list: list of emails to send message to.
        :param filename: Optional filename of if a csv is attached for a given email.
        :param attachment_dataframe: Optional dataframe to attach to email.
        :return: aws ses response if email was sent successfully.
        """

        def convert_dataframe_to_bytes(dataframe):
            """

            This method is used to convert a pandas dataframe into a byte type which is necessary to attach a file to an email.

            :param dataframe: pandas dataframe in which we hope to convert.
            :return: dataframe in byte type format.
            """

            with io.StringIO() as string_csv_buffer:
                dataframe.to_csv(string_csv_buffer, index=False)
                return string_csv_buffer.getvalue()

        ses_client = boto3.client("ses", region_name="us-east-1")

        message = MIMEMultipart()
        message["Subject"] = subject

        body = MIMEText(body, "plain")
        message.attach(body)

        if filename:
            part = MIMEApplication(convert_dataframe_to_bytes(attachment_dataframe))
            part.add_header("Content-Disposition", "attachment", filename=filename)
            message.attach(part)

        response = ses_client.send_raw_email(
            Source="hive@u4u-email.com",
            Destinations=email_list,
            RawMessage={"Data": message.as_string()},
        )

        print(response)

#TODO add slack funtionality