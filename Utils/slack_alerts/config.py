"""
Grabs SLACK_API_TOKEN from environment variables.
"""

import os

class SlackConfig:
    SLACK_TOKEN = os.getenv('SLACK_APP_TOKEN')