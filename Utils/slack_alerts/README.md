# U4U Slack Alerts

## Introduction
Slack alerts is a python based package to simplify interactions between U4U's internal and external systems with Slack's API using `slack-sdk`. 

## Basic Usage
Import the `base.py` script in the `slack_alerts` library. Instantiate the `SlackWebClient` class. Check docstrings for detailed guidance.
* Message related tasks:
  * `message_post` - Sends message to Slack Channel
  * `message_edit` - Modifies sent message
  * `message_append` - Adds additional text to a previously sent message
  * `message_thread` - Creates a thread 
  * `message_delete` - Deletes a previously sent message
  * `file_post` - Uploads a file to a channel as an attachment.
  

## Logs
#### v1.0.0 
* Can send messages and file attachments through U4U's Slack workspace.
* Messages can be updated, appended, deleted and threaded for added context.

## Future Improvements
* Setup a flask server to handle external events
  * Integration of blocks to improve monitoring and response to events.
* Event Handlers/ Listeners
* Incorporate delays to accommodate Slack's rate limits.