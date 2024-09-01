""" fetch secrets by key from appropriate source according to environment """

from enum import Enum
import os


# short identifiers, also used as key prefixes in Secret Manager
ENV_DEV_BST = "DEVB"
ENV_DEV_HIVE = "DEVH"
ENV_ANALYTICS = "ANA"
ENV_STAGING = "STG"
ENV_PROD = "PROD"

TEAM_PREFIXES = {"HIVE", "BST"}
ENV_MAP = {  # normalization of allowed env specifiers to enum, e.g. dev-bst --> DEV_BST
    "DEVB": "DEV_BST",
    "DEV_B": "DEV_BST",
    "DEVBST": "DEV_BST",
    "DEV_BST": "DEV_BST",
    "BST": "DEV_BST",
    "DEVH": "DEV_HIVE",
    "DEV_H": "DEV_HIVE",
    "DEVHIVE": "DEV_HIVE",
    "DEV_HIVE": "DEV_HIVE",
    "HIVE": "DEV_HIVE",
    "ANA": "ANALYTICS",
    "ANALYTICS": "ANALYTICS",
    "STG": "STAGING",
    "STAGING": "STAGING",
    "PROD": "PROD",
    "PRODUCTION": "PROD",
}
PROJECT_IDS = {
    ENV_DEV_BST: "u4u-ds-dev-00",
    ENV_DEV_HIVE: "u4u-ds-dev-01",
    ENV_ANALYTICS: "u4u-ds-prod-00",
    ENV_STAGING: "u4u-ds-stg-00",
    ENV_PROD: "u4u-ds-prod-00",
}


class Env(Enum):
    """ values are the key prefixes used in Secret Manager """
    DEV_BST = ENV_DEV_BST
    DEV_HIVE = ENV_DEV_HIVE
    ANALYTICS = ENV_ANALYTICS
    STAGING = ENV_STAGING
    PROD = ENV_PROD


class SecretManager():
    # read from local environment, not Secret Manager
    local_envs = {Env.DEV_BST, Env.DEV_HIVE, Env.ANALYTICS}

    project_ids = {env: PROJECT_IDS[env.value] for env in Env}

    def __init__(self, env: str = None):
        if env is None:
            if "ENV" not in os.environ:
                raise KeyError("`ENV` must be specified")
            else:
                env = os.environ["ENV"]
        self.env = self._get_env(env)
        self.is_local_env = self.env in self.local_envs

    def _get_env(self, env: str) -> Env:
        """
        translate environment id string to Env object

        @param env: string identifier for an environment
        @return: Env object
        """
        key = env.replace("-", "_").upper()
        if key not in ENV_MAP:
            raise ValueError(f"'{env}' is not a valid ENV identifier")
        return Env[ENV_MAP[key]]

    def _make_full_key(self, key: str) -> str:
        """
        prepend the environment to the key

        @param key: the name of the secret with format {team}_{secret_name}
        @return: name of the secret in Secret Manager with format {env}_{team}_{secret_name}
        """
        return f"{self.env.value}_{key}"

    def _get_from_secret_manager(self, key: str, strict: bool) -> str:
        """
        read secret from Secret Manager

        @param key: the name of the secret with format {team}_{secret_name}
        @param strict: if False, return None if key does not exist, else raise
        @return: value of secret from Secret Manager
        """
        # TODO: implement
        # full_key = self._make_full_key(key)
        # check for existence, raise if strict, else return None
        # client = _make_secret_manager_client(...)
        # fetch and return secret
        # TODO: permit specifying secret version instead of always the latest
        raise NotImplementedError

    def _get_from_env(self, key: str, strict: bool) -> str:
        """
        read secret from local environment variables

        @param key: the name of the secret with format {team}_{secret_name}
        @param strict: if False, return None if key does not exist, else raise
        @return: value of secret from local environment variables
        """
        if strict:
            return os.environ[key]
        else:
            return os.environ.get(key)

    def get(self, key: str, strict: bool = True) -> str:
        """
        Retrieve a secret by key, raise on missing key if strict.
        Fetches from Secret Manager or local environment variables as appropriate
        according to the environment. Input key converted to ALLCAPS before use.

        @param key: the name of the secret with format {team}_{secret_name}
        @param strict: if False, return None if key does not exist, else raise
        @return: value of secret
        """
        key_normalized = key.upper()

        assert any(
            key_normalized.startswith(pre) for pre in TEAM_PREFIXES
        ), "secret key must be prefixed with team"
        # TODO: check key format

        if self.is_local_env:
            return self._get_from_env(key_normalized, strict)
        else:
            return self._get_from_secret_manager(key_normalized, strict)

    def get_project_id(self, env: str = None) -> str:
        """
        Get the project_id for an environment

        @param env: optional, name of an environment, case insensitive
        @return: project_id
        """
        env = self.env if env is None else self._get_env(env)
        return self.project_ids[env]


def _make_secret_manager_client():
    """ authenticate and construct a SecretManagerServiceClient object """
    pass
