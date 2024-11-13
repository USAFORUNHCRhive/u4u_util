*Nov 13, 2024 -- This Repository is Archived and has moved to the new org [here](https://github.com/USA-for-UNHCR/U4U_Utils)*

# U4U Code Utility Library 

## Introduction
Alerts is a python based package to simplify notifications between U4U's internal and external systems with AWS SES and `slack_sdk`.

## Environments
Users can specify the sets of credentials/secrets to use and the GCP project to connect to.
Set the environment variable `ENV` and use `secrets` to fetch secrets by key.
Environment identifiers are case insensitive.

* `DEV_BST`:
    * valid env var values: `DEVB`, `DEV_B`, `DEV_BST`
    * connect to BST dev project
    * use secrets read from local environment, including local user GCP creds
* `DEV_HIVE`:
    * valid env var values: `DEVH`, `DEV_H`, `DEV_HIVE`
    * connect to HIVE dev project
    * use secrets read from local environment, including local user GCP creds
* `ANALYTICS`:
    * valid env var values: `ANA`, `ANALYTICS`
    * connect to PROD project
    * use secrets read from local environment, including local user GCP creds
    * user accounts have restricted access to tables and buckets in PROD
* `STAGING`:
    * valid env var values: `STG`, `STAGING`
    * connect to STAGING project
    * use secrets read from Google Secret Manager, including staging service account GCP creds
    * keys must be prefixed with `STG_`
    * [open question: where to fetch creds to connect to GSM? which creds?]
* `PROD`:
    * valid env var values: `PROD`, `PRODUCTION`
    * connect to PROD project
    * use secrets read from Google Secret Manager, including production service account GCP creds
    * keys must be prefixed with `PROD_`
    * [open question: where to fetch creds to connect to GSM? which creds?]

##### alerts.py
Define Environment variables `SLACK_APP_TOKEN` and `SLACK_CHANNEL`.
Import the `SlackMessaging` class in the `alerts.py` file. Instantiate the `SlackWebClient` class. Check docstrings for detailed guidance.
* Methods
  * `message_post(text)` - Sends message to Slack Channel
  * `file_post(title, filename, comments)` - Uploads a file to a channel as an attachment.
