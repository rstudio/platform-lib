import api_helpers
import os
import pytest


@pytest.fixture
def lib_env():
    # Optional env vars.
    test_dir = os.getenv('TEST_DIR', '')

    return api_helpers.LibEnv(test_dir)
