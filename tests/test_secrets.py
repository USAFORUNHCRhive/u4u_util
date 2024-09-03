""" tests for Secret Manager utils """

import os
import pytest
from Utils.secrets import (
    ENV_DEV_BST,
    ENV_DEV_HIVE,
    ENV_ANALYTICS,
    ENV_STAGING,
    ENV_PROD,
    PROJECT_IDS,
    Env,
    SecretManager,
)


class TestSecretManager:
    def test_init(self):
        """ check expected values for all environments """
        env_init = os.environ.get("ENV")
        if "ENV" in os.environ:
            del os.environ["ENV"]

        # test requirement to specify ENV
        with pytest.raises(KeyError):
            sm = SecretManager()

        # test reading ENV from env var
        os.environ["ENV"] = "devb"
        sm = SecretManager()
        assert sm.env == Env.DEV_BST
        assert sm.is_local_env

        # reset env var
        del os.environ["ENV"]
        if env_init is not None:
            os.environ["ENV"] = env_init

        # test reading ENV from arg
        sm = SecretManager("stg")
        assert sm.env == Env.STAGING
        assert not sm.is_local_env

    def test_get_env(self):
        """ ensure normalizes environments properly and errors on others """
        # test reading each env, case insensitivity
        sm = SecretManager("PROD")
        for key, env in [
            ("devb", Env.DEV_BST),
            ("DEV_B", Env.DEV_BST),
            ("DEV-Bst", Env.DEV_BST),
            ("DEVh", Env.DEV_HIVE),
            ("dev-H", Env.DEV_HIVE),
            ("DEV_hIvE", Env.DEV_HIVE),
            ("ana", Env.ANALYTICS),
            ("Analytics", Env.ANALYTICS),
            ("STG", Env.STAGING),
            ("staging", Env.STAGING),
            ("prod", Env.PROD),
            ("PRODUCTION", Env.PROD),
        ]:
            assert sm._get_env(key) == env

        # test error with bad env
        with pytest.raises(ValueError):
            sm._get_env("dev")

    def test_get_local(self):
        """ ensure it reads local environment vars correctly """
        good_key = "HIVE_TESTSECRET"
        bad_key = "HIVE_TESTSECRET_BAD"
        sm = SecretManager("devb")

        assert good_key not in os.environ
        assert bad_key not in os.environ
        os.environ[good_key] = "123"

        assert sm.get(good_key) == "123"
        assert sm.get(good_key.upper()) == "123"
        assert sm.get(good_key, strict=False) == "123"
        assert sm.get(good_key.upper(), strict=False) == "123"

        with pytest.raises(KeyError):
            sm.get(bad_key) == "123"

        with pytest.raises(KeyError):
            sm.get(bad_key.upper()) == "123"

        assert sm.get(bad_key, strict=False) is None
        assert sm.get(bad_key.upper(), strict=False) is None

        del os.environ[good_key]

    @pytest.mark.skip(reason="method not yet implemented")
    def test_get_sec_mgr(self):
        """ Anything we can test without mocking? """
        pass

    def test_make_full_key(self):
        sm_local = SecretManager("devb")
        sm_prod = SecretManager("prod")

        test_key = "HIVE_SECRETKEY"

        assert sm_local._make_full_key(test_key) == f"{ENV_DEV_BST}_{test_key}"
        assert sm_prod._make_full_key(test_key) == f"{ENV_PROD}_{test_key}"

    def test_get(self):
        """ test assertion on key format works """
        sm_local = SecretManager("devb")
        sm_prod = SecretManager("prod")

        good_key = "hive_secretkey"
        bad_key = good_key[1:]
        assert good_key.upper() not in os.environ
        assert bad_key.upper() not in os.environ
        os.environ[good_key.upper()] = "123"

        assert sm_local.get(good_key) == "123"
        # not yet implemented
        # sm_prod.get(good_key)

        with pytest.raises(AssertionError):
            sm_local.get(bad_key, strict=True)

        with pytest.raises(AssertionError):
            sm_local.get(bad_key, strict=False)

        with pytest.raises(AssertionError):
            sm_prod.get(bad_key, strict=True)

        with pytest.raises(AssertionError):
            sm_prod.get(bad_key, strict=False)

        # TODO: test malformed keys

        del os.environ[good_key.upper()]

    def test_get_project_id(self):
        """ test correct retrieval of project IDs """
        sm_local = SecretManager("devb")
        sm_prod = SecretManager("prod")

        assert sm_local._get_project_id() == PROJECT_IDS[ENV_DEV_BST]
        assert sm_prod._get_project_id() == PROJECT_IDS[ENV_PROD]

        for env_str, pid in [
            ("devb", PROJECT_IDS[ENV_DEV_BST]),
            ("devh", PROJECT_IDS[ENV_DEV_HIVE]),
            ("Analytics", PROJECT_IDS[ENV_ANALYTICS]),
            ("staging", PROJECT_IDS[ENV_STAGING]),
            ("prod", PROJECT_IDS[ENV_PROD]),
        ]:
            assert sm_local._get_project_id(env_str) == pid
            assert sm_prod._get_project_id(env_str) == pid
