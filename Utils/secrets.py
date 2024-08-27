""" fetch secrets by key from appropriate source according to environment """

from enum import Enum
import os


TEAM_PREFIXES = {"HIVE", "BST"}


class Env(Enum):
    """ values are the key prefixes used in Secret Manager """
    DEV = "DEV"
    ANALYTICS = "ANA"
    BILLING = "BILL"
    STAGING = "STG"
    PROD = "PROD"


class SecretManager():
    local_envs = {Env.DEV, Env.ANALYTICS}

    def __init__(self, env: str = None):
        if env is None:
            env = os.environ.get("ENV", "dev")
        self.env = Env[env.upper()]
        self.is_local_env = self.env in self.local_envs

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


def _make_secret_manager_client():
    """ authenticate and construct a SecretManagerServiceClient object """
    pass
