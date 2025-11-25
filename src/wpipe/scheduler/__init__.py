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
            subprocess.Popen(
                ["nohup", "python", "-m", "wpipe.scheduler.PbsConsumer"], cwd=homedir
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
            subprocess.Popen(
                ["nohup", "python", "-m", "wpipe.scheduler.SlurmConsumer"], cwd=homedir
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
