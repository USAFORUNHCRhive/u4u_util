# u4u_util
U4U code utility library

# Environments
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
