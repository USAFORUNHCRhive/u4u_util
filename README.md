# U4U Code Utility Library 

## alerts.py
_________
### Introduction
Alerts is a python based package to simplify notifications between U4U's internal and external systems with AWS SES and `slack_sdk`.

### Basic Usage
Define Environment variables `SLACK_APP_TOKEN` and `SLACK_CHANNEL`.
Import the `SlackMessaging` class in the `alerts.py` file. Instantiate the `SlackWebClient` class. Check docstrings for detailed guidance.
* Methods
  * `message_post(text)` - Sends message to Slack Channel
  * `file_post(title, filename, comments)` - Uploads a file to a channel as an attachment.
