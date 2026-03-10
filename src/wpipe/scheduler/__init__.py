#!/usr/bin/env python
"""
Description
-----------

TODO

How to use
----------

TODO

Utilities
---------
pbsconsumer
    TODO

JobData
    TODO

checkPbsConnection
    TODO

sendJobToPbs
    TODO
"""

import os
import socket
import subprocess
import sys
import time

# from .PbsScheduler import PbsScheduler
from .PbsConsumer import checkPbsConnection, sendJobToPbs
from .PbsConsumer import DEFAULT_PORT as PBS_PORT
from .SlurmConsumer import checkSlurmConnection, sendJobToSlurm
from .SlurmConsumer import DEFAULT_PORT as SLURM_PORT
from .JobData import JobData

__all__ = [
    "pbsconsumer",
    "JobData",
    "checkPbsConnection",
    "sendJobToPbs",
    "slurmconsumer",
    "JobData",
    "checkSlurmConnection",
    "sendJobToSlurm",
]


def _get_head_node(consumer_type: str) -> str:
    """Return the head node hostname.

    Checks, in order:
    1. Env var WPIPE_SLURM_HEAD_NODE (or WPIPE_PBS_HEAD_NODE)
    2. ~/.slurmconsumer/config  (INI format: [slurmconsumer] / head_node = hostname)
    3. RuntimeError with clear instructions if neither is configured
    """
    upper = consumer_type.upper()
    env_var = "WPIPE_{}_HEAD_NODE".format(upper)
    head_node = os.environ.get(env_var)
    if head_node:
        return head_node

    import configparser
    config_file = os.path.expanduser(
        "~/.{}consumer/config".format(consumer_type)
    )
    if os.path.exists(config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        section = "{}consumer".format(consumer_type)
        if config.has_option(section, "head_node"):
            head_node = config.get(section, "head_node").strip()
            if head_node:
                return head_node

    raise RuntimeError(
        "Head node not configured for {}consumer. "
        "Set the {} environment variable to the head/login node hostname, "
        "or create ~/.{}consumer/config with:\n"
        "  [{}consumer]\n"
        "  head_node = <hostname>".format(
            consumer_type, env_var, consumer_type, consumer_type
        )
    )


def _is_on_head_node(head_node: str) -> bool:
    """Return True if currently running on the head node."""
    current = socket.gethostname()
    return current.split(".")[0] == head_node.split(".")[0]


def _check_stale_lock(consumer_type: str) -> bool:
    """Check for a stale address file.

    Returns True if a stale file was found and cleaned up.
    Raises RuntimeError if the consumer is already running.
    """
    from .Utils import read_address_file, remove_address_file

    result = read_address_file(consumer_type)
    if result is None:
        return False

    host, port, _ = result
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2.0)
    connected = s.connect_ex((host, port))
    s.close()

    if connected == 0:
        raise RuntimeError(
            "{}consumer is already running at {}:{}".format(
                consumer_type.capitalize(), host, port
            )
        )

    # Connection refused or timed out — stale file
    print(
        "Stale address file found for {}consumer at {}:{}. "
        "Cleaning up ...".format(consumer_type.capitalize(), host, port)
    )
    remove_address_file(consumer_type)
    return True


def pbsconsumer(which: str):
    connection = checkPbsConnection()
    print(
        "PbsConsumer connection status (0 = running): {}".format(connection)
    )
    if which == "check":
        return print(connection)
    elif which == "start":
        if connection != 0:
            # Check for stale lock; raises if already running
            try:
                _check_stale_lock("pbs")
            except RuntimeError as e:
                print(str(e))
                return

            head_node = _get_head_node("pbs")

            homedir = os.path.expanduser("~/.pbsconsumer")
            os.makedirs(homedir, exist_ok=True)

            consumer_module = "wpipe.scheduler.PbsConsumer"

            if _is_on_head_node(head_node):
                print("Starting PbsConsumer locally on head node ...")
                subprocess.Popen(
                    ["nohup", sys.executable, "-m", consumer_module],
                    cwd=homedir,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
            else:
                print("Starting PbsConsumer on head node {} via SSH ...".format(head_node))
                ssh_cmd = [
                    "ssh",
                    "-o", "BatchMode=yes",
                    "-o", "ConnectTimeout=10",
                    head_node,
                    "nohup {} -m {} </dev/null >/dev/null 2>&1 &".format(
                        sys.executable, consumer_module
                    ),
                ]
                try:
                    result = subprocess.run(ssh_cmd, timeout=15)
                except subprocess.TimeoutExpired:
                    raise RuntimeError(
                        "SSH to {} timed out. "
                        "Is WPIPE_PBS_HEAD_NODE correct? "
                        "Is the head node reachable?".format(head_node)
                    )
                if result.returncode != 0:
                    raise RuntimeError(
                        "SSH to {} failed (returncode {}). "
                        "Is WPIPE_PBS_HEAD_NODE correct? "
                        "Is passwordless SSH configured (ssh-copy-id)?".format(
                            head_node, result.returncode
                        )
                    )

            # Poll until connected (up to 30s)
            deadline = time.time() + 30
            while time.time() < deadline:
                if checkPbsConnection() == 0:
                    print("PbsConsumer is up.")
                    return
                time.sleep(0.5)
            print(
                "WARNING: PbsConsumer did not come up within 30s. "
                "Check ~/.pbsconsumer/ for logs."
            )
        else:
            print("PbsConsumer is already running.")
    else:
        if connection == 0:
            if which == "stop":
                print("Shutting down PbsConsumer ...")
                sendJobToPbs("poisonpill")
            elif which == "log":
                print("Printing current PbsConsumer log ...")
                # TODO
        else:
            print("No PbsConsumer found, nothing to do ...")


def slurmconsumer(which):
    connection = checkSlurmConnection()
    print(
        "SlurmConsumer connection status (0 = running): {}".format(connection)
    )
    if which == "check":
        return print(connection)
    elif which == "start":
        if connection != 0:
            # Check for stale lock; raises if already running
            try:
                _check_stale_lock("slurm")
            except RuntimeError as e:
                print(str(e))
                return

            head_node = _get_head_node("slurm")

            homedir = os.path.expanduser("~/.slurmconsumer")
            os.makedirs(homedir, exist_ok=True)

            consumer_module = "wpipe.scheduler.SlurmConsumer"

            if _is_on_head_node(head_node):
                print("Starting SlurmConsumer locally on head node ...")
                subprocess.Popen(
                    ["nohup", sys.executable, "-m", consumer_module],
                    cwd=homedir,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,
                )
            else:
                print("Starting SlurmConsumer on head node {} via SSH ...".format(head_node))
                ssh_cmd = [
                    "ssh",
                    "-o", "BatchMode=yes",
                    "-o", "ConnectTimeout=10",
                    head_node,
                    "nohup {} -m {} </dev/null >/dev/null 2>&1 &".format(
                        sys.executable, consumer_module
                    ),
                ]
                try:
                    result = subprocess.run(ssh_cmd, timeout=15)
                except subprocess.TimeoutExpired:
                    raise RuntimeError(
                        "SSH to {} timed out. "
                        "Is WPIPE_SLURM_HEAD_NODE correct? "
                        "Is the head node reachable?".format(head_node)
                    )
                if result.returncode != 0:
                    raise RuntimeError(
                        "SSH to {} failed (returncode {}). "
                        "Is WPIPE_SLURM_HEAD_NODE correct? "
                        "Is passwordless SSH configured (ssh-copy-id)?".format(
                            head_node, result.returncode
                        )
                    )

            # Poll until connected (up to 30s)
            deadline = time.time() + 30
            while time.time() < deadline:
                if checkSlurmConnection() == 0:
                    print("SlurmConsumer is up.")
                    return
                time.sleep(0.5)
            print(
                "WARNING: SlurmConsumer did not come up within 30s. "
                "Check ~/.slurmconsumer/ for logs."
            )
        else:
            print("SlurmConsumer is already running.")
    else:
        if connection == 0:
            if which == "stop":
                print("Shutting down SlurmConsumer ...")
                sendJobToSlurm("poisonpill")
            elif which == "log":
                print("Printing current SlurmConsumer log ...")
                # TODO
        else:
            print("No SlurmConsumer found, nothing to do ...")
