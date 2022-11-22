import json
import psutil
import pytest
import re
import signal
import time

import sys
sys.path.append("..")
from api_helpers import RunResources


"""
Include fixtures.
"""
pytest_plugins = [
    'fixtures.lib_env',
]


@pytest.fixture(autouse=True)
def run_before_and_after_tests(lib_env):

    # Setup code

    # Run test. Add teardown code after this line.
    yield


def test_command(lib_env):
    """
    This test validates that a command works
    """

    with RunResources(lib_env) as res:

        # Run with an unknown command
        args = [f"{lib_env.asset_dir}/testlog", "log", "--message", "expected output"]
        output = res.run_command(args)
        assert re.search('{"level":"info","msg":"expected output","time":".+"}', output)
