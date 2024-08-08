from typing import List, Dict

from slack_sdk.web import WebClient, SlackResponse
from slack_sdk.errors import SlackApiError
from .config import SlackConfig

from slack_utilities import logging

logger = logging.setup_logging()


class SlackMessaging(SlackConfig):
    def __init__(self, token):
        self.client = WebClient(token=SlackConfig.SLACK_TOKEN)

    # ========================================
    # Message related tasks
    # ========================================
    def message_post(self, text: str, channel: str = None, blocks: List[Dict[str, str]] = None) -> None:
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

    def message_edit(self, channel: str, ts: str, text: str) -> None:
        """
        Edits a message in the Slack workspace.
        :param channel: Channel to edit message in
        :param ts: Message ID
        :param text: Modified text
        """
        try:
            response = self.client.chat_update(
                channel=channel,
                ts=ts,
                text=text,
            )
            logger.info(f"Message edited in {channel} TSID: {ts}")
        except SlackApiError as e:
            assert e.response["error"]
            logger.error(f"Error modifying {ts}, response: {e.response['error']}")

    def message_append(self, channel: str, ts: str, text: str) -> None:
        """
        Appends a message to an already posted message in the Slack workspace.
        :param channel: Channel
        :param ts: TS ID to append
        :param text: text to append to original message.
        """
        def get_message():
            try:
                response = self.client.conversations_history(
                    channel=channel,
                    latest=ts,
                    limit=1,
                    inclusive=True
                )
                message_content = response['messages'][0]
                return message_content
            except SlackApiError as e:
                logger.error(f"Error getting message from Slack: {ts}, response: {e.response['error']}")
        message = get_message()
        if message is not None:
            appended_text = message + "\n" + text
            try:
                response = self.client.chat_update(
                    channel=channel,
                    ts=ts,
                    text=appended_text,
                )
            except SlackApiError as e:
                logger.error(f"Error appending message: {ts}, response: {e.response['error']}")
        else:
            logger.error("Message not found. Check TS ID")

    def message_thread(self, channel: str, thread_ts: str, text: str) -> None:
        """
        Creates a threaded message to provide greater context to messages.
        :param channel: channel to send message to
        :param thread_ts: thread id
        :param text: text to send
        """
        try:
            response = self.client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=text,
            )
            logger.info(f"Sent message to Slack: {text}")
        except SlackApiError as e:
            assert e.response["error"]
            logger.error(f"Error sending message to Slack: {text}, response: {e.response['error']}")

    def message_delete(self, channel: str, ts: str) -> None:
        """
        Delete previously sent message.
        :param channel: Channel name
        :param ts: ts ID
        """
        try:
            response = self.client.chat_delete(
                channel=channel,
                ts=ts,
            )
            logger.info(f"Message deleted in {channel}, tsID: {ts}")
        except SlackApiError as e:
            assert e.response["error"]
            logger.error(f"Error deleting {ts} from Slack: {channel}")

    # ========================================
    # File related tasks
    # ========================================
    def file_post(self, channel: str, file: str, title: str, initial_comments: str) -> None:
        """
        Uploads and posts file to specified Slack channel.
        :param channel: Channel to send file to.
        :param file: File name of file to upload.
        :param title: Title to post file with.
        :param initial_comments: Additional comments for context.
        :return:
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

