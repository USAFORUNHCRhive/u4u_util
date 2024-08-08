"""This file sends alerts via email or slack"""
import os
from typing import List, Dict

import boto3
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from slack_sdk.web import WebClient
from slack_sdk.errors import SlackApiError
from Utils.logging import logging

logger = logging.setup_logging()


def create_email(subject, body, email_list, filename=None, attachment_dataframe=None):
    """
    This method is using ses (AWS simple email service) to send an email.
    We use a domain created by AWS Route 53; which was verified using ses.
    Once the domain is verified, this method can send emails to any domains. Any email can be attributed to the
    verification domain if it is created by domain53.

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

    logger.info(response)


class SlackMessaging:
    SLACK_TOKEN = os.getenv('SLACK_APP_TOKEN')
    SLACK_CHANNEL = os.getenv('SLACK_CHANNEL')

    def __init__(self):
        self.client = WebClient(token=SlackMessaging.SLACK_TOKEN)

    # ========================================
    # Message related methods
    # ========================================
    def message_post(self, text, channel: str = SLACK_CHANNEL, blocks: List[Dict[str, str]] = None) -> None:
        """
        Sends a text to Slack workspace given channel and optional blocks param.
        :param text: Message to send
        :param channel: Channel to send message to
        :param blocks: See SlackAPI documentation for more details.
        """
        try:
            response = self.client.chat_postMessage(
                channel=channel,
                text=text,
                blocks=blocks
            )
            logger.info(f"Sent message to Slack: {text}")
        except SlackApiError as e:
            assert e.response["error"]
            logger.error(f"Error posting message to Slack: {text}, response: {e.response['error']}")

    # ========================================
    # File related methods
    # ========================================
    def file_post(self,
                  title: str = None,
                  file: str = None,
                  initial_comments: str = None,
                  channel: str = SLACK_CHANNEL) -> None:
        """
        Uploads and posts file to specified Slack channel.
        :param title: Title to post file with.
        :param file: File name of file to upload.
        :param initial_comments: Additional comments for context.
        :param channel: Channel to send file to.
        """
        try:
            response = self.client.files_upload_v2(
                channel=channel,
                file=file,
                title=title,
                initial_comment=initial_comments
            )
            logger.info(f"Sent file to Slack: {title} in {channel}")
        except SlackApiError as e:
            assert e.response["error"]
            logger.error(f"Error sending {title} to Slack:, response: {e.response['error']}")

