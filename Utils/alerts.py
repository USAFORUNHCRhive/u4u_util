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
    Once the domain is verified, this method can send emails to any domains.
    Any email can be attributed to the verification domain if it is created by domain53.

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
        Source="hive@unrefugees.org",
        Destinations=email_list,
        RawMessage={"Data": message.as_string()},
    )

    logger.info(response)


class SlackMessaging:
    SLACK_TOKEN = os.getenv('SLACK_API_TOKEN')

    def __init__(self):
        self.client = WebClient(token=SlackMessaging.SLACK_TOKEN)

    # ========================================
    # Message related methods
    # ========================================
    def validate_slack_token(self) -> bool:
        """
        This method checks whether the current Slack API key is valid by checking the env variable SLACK_API_TOKEN.
        If it is invalid, a SlackApiError is raised and the error message is sent via email to engineering and Hive.
        :return: bool()
        """
        try:
            response = self.client.auth_test()
            if response["ok"]:
                logger.debug("Token is valid.")
                return True
        except SlackApiError as e:
            logger.error(f"Error: Unable to validate Slack Token: {e.response['error']}")
            # Send email notification:
            create_email(
                subject="Slack Error: API Token Invalid",
                body=f"Slack Error: {e.response['error']}",
                email_list=["katie@unrefugees.org",
                            "acheng@usaforunhcr.org",
                            "hive@unrefugees.org"]
            )
            return False

    def message_post(self, text, channels: List[str], blocks: List[Dict[str, str]] = None) -> None:
        """
        Sends a text to Slack workspace given channel and optional blocks param.
        :param text: Message to send
        :param channels: Channels to send message to as a List
        :param blocks: See SlackAPI documentation for more details.
        """
        if self.validate_slack_token() is True:  # Check if API is valid
            for channel in channels:
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

    def message_single_direct_msg(self, text, users: List[str]) -> None:
        """
        Sends a text to a list of users using their userID's, individually
        :param text: Message to send
        :param users: Channels to send message to as a List
        """
        if self.validate_slack_token() is True:
            try:
                for user in users:
                    response = self.client.conversations_open(
                        users=user
                    )
                    dm_channel = response['channel']['id']
                    self.client.chat_postMessage(text=text, channel=dm_channel)
                    logger.info(f"Direct message sent to {users} separately with message:")
                    logger.info(f"{text}")
            except SlackApiError as e:
                assert e.response["error"]
                logger.error(f"Error posting direct message to Slack: {text}, response: {e.response['error']}")

    def message_group_direct_msg(self, text, users: List[str]) -> None:
        """
        Sends a text to a list of users using their userID's as a group conversation
        :param text: message contents to send
        :param users: Channels to send message to as a List
        """
        if len(users) < 8 and self.validate_slack_token() is True:
            try:
                response = self.client.conversations_open(
                    users=users
                )
                logger.info(f"Sent message to Slack: {text}")
                dm_channel = response['channel']['id']
                self.client.chat_postMessage(text=text, channel=dm_channel)
                logger.info(f"Direct group message sent to {users} with message:")
                logger.info(f"{text}")
            except SlackApiError as e:
                assert e.response["error"]
                logger.error(f"Error posting message to Slack: {text}, response: {e.response['error']}")

    # ========================================
    # File related methods
    # ========================================
    def file_post(self,
                  channel: str = None,
                  title: str = None,
                  file: str = None,
                  initial_comments: str = None) -> None:
        """
        Uploads and posts file to specified Slack channel.
        :param title: Title to post file with.
        :param file: File name of file to upload.
        :param initial_comments: Additional comments for context.
        :param channel: Channel to send file to.
        """
        if self.validate_slack_token() is True:
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

    # ========================================
    # Slack Block Methods
    # ========================================

    # TODO: Add block functions for users to interact with external API's from Slack
    # TODO: As of 08/16/24: Cannot users direct messages-- requires additional scope for API
        # TODO: EVALUATE API TIER LEVEL (Affects rate limits)
    # TODO: Future function to input channel/ user name instead of their ID.
    # TODO: Add scripted test cases for Slack


