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
import subprocess
import time
from datetime import datetime

# from .PbsScheduler import PbsScheduler
from .PbsConsumer import checkPbsConnection, sendJobToPbs
from .PbsConsumer import HOST_MACHINE as PBS_HOST, DEFAULT_PORT as PBS_PORT
from .SlurmConsumer import checkSlurmConnection, sendJobToSlurm
from .SlurmConsumer import HOST_MACHINE as SLURM_HOST, DEFAULT_PORT as SLURM_PORT
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


def _create_consumer_wrapper_script(consumer_type: str, homedir: str) -> str:
    """
    Create a bash wrapper script that monitors consumer process and logs termination.

    Only creates the script if it doesn't exist or content has changed.
    Safe for shared home directories across compute nodes.

    Parameters
    ----------
    consumer_type : str
        Type of consumer ("pbs" or "slurm")
    homedir : str
        Home directory for the consumer (~/.pbsconsumer or ~/.slurmconsumer)

    Returns
    -------
    str
        Path to the wrapper script
    """
    wrapper_script_path = os.path.join(homedir, "run_consumer.sh")
    termination_log = os.path.join(homedir, "termination.log")
    consumer_module = "wpipe.scheduler.{}Consumer".format(
        consumer_type.capitalize()
    )

    wrapper_content = """#!/bin/bash
# Consumer wrapper script - monitors process and logs termination
# Generated automatically - do not edit manually

CONSUMER_MODULE="{consumer_module}"
TERMINATION_LOG="{termination_log}"
PID=$$
PPID=${{PPID}}

# Function to log termination information
log_termination() {{
    local EXIT_CODE=$1
    local SIGNAL_NAME=$2
    local TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

    echo "===== Consumer Termination: $TIMESTAMP =====" >> "$TERMINATION_LOG"
    echo "Exit Code: $EXIT_CODE" >> "$TERMINATION_LOG"
    echo "Signal: $SIGNAL_NAME" >> "$TERMINATION_LOG"
    echo "Consumer PID: $PID" >> "$TERMINATION_LOG"
    echo "Parent PID: $PPID" >> "$TERMINATION_LOG"

    # Try to get parent process info from /proc
    if [ -d "/proc/$PPID" ]; then
        if [ -f "/proc/$PPID/comm" ]; then
            PARENT_NAME=$(cat /proc/$PPID/comm 2>/dev/null || echo "unknown")
            echo "Parent Process Name: $PARENT_NAME" >> "$TERMINATION_LOG"
        fi
        if [ -f "/proc/$PPID/cmdline" ]; then
            PARENT_CMD=$(cat /proc/$PPID/cmdline 2>/dev/null | tr '\\0' ' ' || echo "unknown")
            echo "Parent Command: $PARENT_CMD" >> "$TERMINATION_LOG"
        fi
    fi

    # Get hostname and user info
    echo "Hostname: $(hostname)" >> "$TERMINATION_LOG"
    echo "User: $(whoami)" >> "$TERMINATION_LOG"
    echo "Working Directory: $(pwd)" >> "$TERMINATION_LOG"

    # Determine signal from exit code if applicable
    if [ $EXIT_CODE -gt 128 ] && [ $EXIT_CODE -lt 192 ]; then
        SIGNAL_NUM=$((EXIT_CODE - 128))
        echo "Likely killed by signal number: $SIGNAL_NUM" >> "$TERMINATION_LOG"
    fi

    echo "=============================================" >> "$TERMINATION_LOG"
    echo "" >> "$TERMINATION_LOG"
}}

# Run the consumer and capture exit code
python -m "$CONSUMER_MODULE"
EXIT_CODE=$?

# Determine signal name from exit code
if [ $EXIT_CODE -eq 0 ]; then
    SIGNAL_NAME="Normal Exit"
elif [ $EXIT_CODE -eq 130 ]; then
    SIGNAL_NAME="SIGINT (Ctrl+C)"
elif [ $EXIT_CODE -eq 143 ]; then
    SIGNAL_NAME="SIGTERM"
elif [ $EXIT_CODE -eq 137 ]; then
    SIGNAL_NAME="SIGKILL"
elif [ $EXIT_CODE -eq 129 ]; then
    SIGNAL_NAME="SIGHUP"
elif [ $EXIT_CODE -eq 141 ]; then
    SIGNAL_NAME="SIGPIPE"
elif [ $EXIT_CODE -gt 128 ]; then
    SIGNAL_NUM=$((EXIT_CODE - 128))
    SIGNAL_NAME="Signal $SIGNAL_NUM"
else
    SIGNAL_NAME="Non-zero exit"
fi

# Log termination
log_termination $EXIT_CODE "$SIGNAL_NAME"

# Exit with same code as consumer
exit $EXIT_CODE
""".format(
        consumer_module=consumer_module,
        termination_log=termination_log
    )

    # Only create/update if script doesn't exist or content changed
    should_write = True
    if os.path.exists(wrapper_script_path):
        try:
            with open(wrapper_script_path, "r") as f:
                existing_content = f.read()
            if existing_content == wrapper_content:
                should_write = False
        except (IOError, OSError):
            # If we can't read it, recreate it
            should_write = True

    if should_write:
        with open(wrapper_script_path, "w") as f:
            f.write(wrapper_content)
        # Make script executable
        os.chmod(wrapper_script_path, 0o755)

    return wrapper_script_path


def pbsconsumer(which: str):
    connection = checkPbsConnection()
    print(
        "PbsConsumer connection status to {}:{} (0 = running): {}".format(
            PBS_HOST, PBS_PORT, connection
        )
    )
    if which == "check":
        return print(connection)
    elif which == "start":
        if connection != 0:
            print("Starting PbsConsumer on {}:{} ...".format(PBS_HOST, PBS_PORT))
            homedir = os.path.expanduser("~/.pbsconsumer")
            if not os.path.exists(homedir):
                os.mkdir(homedir)
            elif not os.path.isdir(homedir):
                raise FileExistsError("{} is not a directory".format(homedir))

            # Create wrapper script that monitors consumer and logs termination
            wrapper_script = _create_consumer_wrapper_script("pbs", homedir)

            subprocess.Popen(
                ["nohup", wrapper_script],
                cwd=homedir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True
            )
            while checkPbsConnection() != 0:
                time.sleep(0.1)
        else:
            print(
                "PbsConsumer is already running on {}:{} ...".format(PBS_HOST, PBS_PORT)
            )
    else:
        if connection == 0:
            if which == "stop":
                print(
                    "Shutting down PbsConsumer on {}:{} ...".format(PBS_HOST, PBS_PORT)
                )
                sendJobToPbs("poisonpill")
            elif which == "log":
                print("Printing current PbsConsumer log ...")
                # TODO
        else:
            print(
                "No server found at {}:{}, nothing to do ...".format(PBS_HOST, PBS_PORT)
            )


def slurmconsumer(which):
    connection = checkSlurmConnection()
    print(
        "SlurmConsumer connection status to {}:{} (0 = running): {}".format(
            SLURM_HOST, SLURM_PORT, connection
        )
    )
    if which == "check":
        return print(connection)
    elif which == "start":
        if connection != 0:
            print("Starting SlurmConsumer on {}:{} ...".format(SLURM_HOST, SLURM_PORT))
            homedir = os.path.expanduser("~/.slurmconsumer")
            if not os.path.exists(homedir):
                os.mkdir(homedir)
            elif not os.path.isdir(homedir):
                raise FileExistsError("{} is not a directory".format(homedir))

            # Create wrapper script that monitors consumer and logs termination
            wrapper_script = _create_consumer_wrapper_script("slurm", homedir)

            subprocess.Popen(
                ["nohup", wrapper_script],
                cwd=homedir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True
            )
            while checkSlurmConnection() != 0:
                time.sleep(0.1)
        else:
            print(
                "SlurmConsumer is already running on {}:{} ...".format(
                    SLURM_HOST, SLURM_PORT
                )
            )
    else:
        if connection == 0:
            if which == "stop":
                print(
                    "Shutting down SlurmConsumer on {}:{} ...".format(
                        SLURM_HOST, SLURM_PORT
                    )
                )
                sendJobToSlurm("poisonpill")
            elif which == "log":
                print("Printing current SlurmConsumer log ...")
                # TODO
        else:
            print(
                "No server found at {}:{}, nothing to do ...".format(
                    SLURM_HOST, SLURM_PORT
                )
            )
