from interruptingcow import timeout
import json
import os
import signal
import subprocess
import tempfile


class LibEnv:
    """
    LibEnv is a helper for interacting with processes
    """

    def __init__(self, test_dir: str, asset_dir: str):

        self.test_dir = test_dir
        self.asset_dir = asset_dir


class RunResources:
    """
    RunResources encapsulates a RunWithResult class in order to enforce
    cleanup of temporary resources (files) after tests.
    """

    def __init__(self, env: LibEnv):
        self.env = env

    def __enter__(self):

        class RunWithResult:
            """
            RunWithResult runs a command and yields StdOut line by line.
            """

            def __init__(self, env: LibEnv):
                self.env = env
                self.stderr = ''
                self.return_code = -1
                self.process = None

                # Make a temporary file for logs
                self.tempfile = tempfile.mktemp()

            def cleanup(self):
                """
                Cleans up temp file associated with test.
                :return:
                """
                try:
                    os.unlink(self.tempfile)
                except (ValueError, Exception) as e:
                    print(f"Could not delete temp file {self.tempfile}: {e}")
                    pass

            def run_command(self, args):
                """
                Runs a command with provided args. Returns output of process,
                including STDOUT and STDERR.
                :param args:
                :return:
                """

                env = {
                    'HOME': f"{os.environ.get('HOME')}"
                }

                self.process = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output = b''.join(self.process.stdout.readlines()).decode('utf-8')
                return output


        self.res = RunWithResult(self.env)
        return self.res

    def __exit__(self, exc_type, exc_value, traceback):
        self.res.cleanup()

