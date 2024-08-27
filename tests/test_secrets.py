""" tests for Secret Manager utils """

import os
import pytest
from Utils.secrets import PROJECT_IDS, Env, SecretManager


class TestSecretManager:
    def test_init(self):
        """ ensure it reads environments properly and errors on others """
        env_init = os.environ.get("ENV")
        if "ENV" in os.environ:
            del os.environ["ENV"]

        # test reading each env, case insensitivity
        for key, env, is_local_env in [
            (None, Env.DEV, True),
            ("dev", Env.DEV, True),
            ("DEV", Env.DEV, True),
            ("analytics", Env.ANALYTICS, True),
            ("Analytics", Env.ANALYTICS, True),
            ("billing", Env.BILLING, False),
            ("staging", Env.STAGING, False),
            ("prod", Env.PROD, False),
        ]:
            sm = SecretManager(key)
            assert sm.env == env
            assert sm.is_local_env == is_local_env

        # test reading env var when env not supplied
        os.environ["ENV"] = "dev"
        sm = SecretManager()
        assert sm.env is Env.DEV
        assert sm.is_local_env

        # test error with bad env
        with pytest.raises(KeyError):
            sm = SecretManager("other")

        if env_init is not None:
            os.environ["ENV"] = env_init

    def test_get_local(self):
        """ ensure it reads local environment vars correctly """
        good_key = "HIVE_TESTSECRET"
        bad_key = "HIVE_TESTSECRET_BAD"
        sm = SecretManager("dev")

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
        sm_local = SecretManager("dev")
        sm_prod = SecretManager("prod")

        test_key = "HIVE_SECRETKEY"

        assert sm_local._make_full_key(test_key) == f"DEV_{test_key}"
        assert sm_prod._make_full_key(test_key) == f"PROD_{test_key}"

    def test_get(self):
        """ test assertion on key format works """
        sm_local = SecretManager("dev")
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
        sm_local = SecretManager("dev")
        sm_prod = SecretManager("prod")

        assert sm_local.get_project_id() == PROJECT_IDS["DEV"]
        assert sm_prod.get_project_id() == PROJECT_IDS["PROD"]

        for env_str, pid in [
            ("dev", PROJECT_IDS["DEV"]),
            ("DEV", PROJECT_IDS["DEV"]),
            ("analytics", PROJECT_IDS["ANA"]),
            ("Analytics", PROJECT_IDS["ANA"]),
            ("billing", PROJECT_IDS["BILL"]),
            ("staging", PROJECT_IDS["STG"]),
            ("prod", PROJECT_IDS["PROD"]),
        ]:
            assert sm_local.get_project_id(env_str) == pid
            assert sm_prod.get_project_id(env_str) == pid
