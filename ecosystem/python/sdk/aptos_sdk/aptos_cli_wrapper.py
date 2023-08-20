# Copyright © Aptos Foundation
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import tempfile
import threading
import time
from typing import List

from .async_client import FaucetClient, RestClient

# Assume that the binary is in the global path if one is not provided.
DEFAULT_BINARY = os.getenv("APTOS_CLI_PATH", "aptos")
LOCAL_FAUCET = "http://127.0.0.1:8081"
LOCAL_NODE = "http://127.0.0.1:8080/v1"

# Assume that the node failed to start if it has been more than this time since the process started
MAXIMUM_WAIT_TIME_SEC = 30


class AptosCLIWrapper:
    """Tooling to make easy access to the Aptos CLI tool from within Python."""

    @staticmethod
    def start_node() -> AptosInstance:
        AptosCLIWrapper.assert_cli_exists()
        return AptosInstance.start()

    @staticmethod
    def assert_cli_exists():
        if shutil.which(DEFAULT_BINARY) is None:
            raise MissingCLIError()


class MissingCLIError(Exception):
    """The CLI was not found in the expected path."""

    def __init__(self):
        super().__init__("The CLI was not found in the expected path, {DEFAULT_BINARY}")


class AptosInstance:
    """
    A standalone Aptos node running by itself. This still needs a bit of work:
    * a test instance should be loaded into its own port space. Currently they share ports as
      those are not configurable without a config file. As a result, it is possible that two
      test runs may share a single AptosInstance and both successfully complete.
    * Probably need some means to monitor the process in case it stops, as we aren't actively
      monitoring this.
    """

    _node_runner: subprocess.Popen
    _temp_dir: tempfile.TemporaryDirectory
    _output: List[str]
    _error: List[str]

    def __del__(self):
        self.stop()

    def __init__(
        self, node_runner: subprocess.Popen, temp_dir: tempfile.TemporaryDirectory
    ):
        self._node_runner = node_runner
        self._temp_dir = temp_dir

        self._output = []
        self._error = []

        def queue_lines(pipe, target):
            for line in iter(pipe.readline, b""):
                if line == "":
                    continue
                target.append(line)
            pipe.close()

        err_thread = threading.Thread(
            target=queue_lines, args=(node_runner.stderr, self._error)
        )
        err_thread.daemon = True
        err_thread.start()

        out_thread = threading.Thread(
            target=queue_lines, args=(node_runner.stdout, self._output)
        )
        out_thread.daemon = True
        out_thread.start()

    @staticmethod
    def start() -> AptosInstance:
        temp_dir = tempfile.TemporaryDirectory()
        args = [
            DEFAULT_BINARY,
            "node",
            "run-local-testnet",
            "--test-dir",
            str(temp_dir),
            "--with-faucet",
            "--force-restart",
            "--assume-yes",
        ]
        node_runner = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        return AptosInstance(node_runner, temp_dir)

    def stop(self):
        self._node_runner.terminate()
        self._node_runner.wait()
        self._temp_dir.cleanup()

    def errors(self) -> List[str]:
        return self._error

    def output(self) -> List[str]:
        return self._output

    async def wait_until_operational(self) -> bool:
        operational = await self.is_operational()
        start = time.time()
        last = start

        while (
            not self.is_stopped()
            and not operational
            and start + MAXIMUM_WAIT_TIME_SEC > last
        ):
            await asyncio.sleep(0.1)
            operational = await self.is_operational()
            last = time.time()
        return not self.is_stopped()

    async def is_operational(self) -> bool:
        rest_client = RestClient(LOCAL_NODE)
        faucet_client = FaucetClient(LOCAL_NODE, rest_client)

        try:
            await rest_client.chain_id()
            return await faucet_client.healthy()
        except Exception:
            return False
        finally:
            await rest_client.close()

    def is_stopped(self) -> bool:
        return self._node_runner.returncode is not None
