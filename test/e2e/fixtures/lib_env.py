import api_helpers
import os
import pytest


@pytest.fixture
def lib_env():
    # Optional env vars.
    test_dir = os.getenv('TEST_DIR', '')
    asset_dir = os.getenv('ASSET_DIR', f"{test_dir}/assets")

    return api_helpers.LibEnv(test_dir, asset_dir)
