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

slurmconsumer
    TODO

JobData
    TODO

checkConsumerConnection
    TODO

sendJobToConsumer
    TODO
"""
import os
import subprocess

from .BaseConsumer import checkConsumerConnection, sendJobToConsumer
from .JobData import JobData

__all__ = ['pbsconsumer', 'slurmconsumer', 'base_consumer', 'checkConsumerConnection', 'sendJobToConsumer', 'JobData']


SUPPORTED_SCHEDULERS = {'Pbs', 'Slurm'}


def pbsconsumer(which):
    connection = checkConsumerConnection()
    if which == 'check':
        return print(connection)
    elif which == 'start':
        if connection != 0:
            print("Starting PbsConsumer ...")
            homedir = os.path.expanduser('~/.pbsconsumer')
            if not os.path.exists(homedir):
                os.mkdir(homedir)
            elif not os.path.isdir(homedir):
                raise FileExistsError("%s is not a directory" % homedir)
            subprocess.Popen(["nohup", "python", "-m", "wpipe.scheduler.PbsConsumer"], cwd=homedir)
            while checkConsumerConnection() != 0:
                pass
        else:
            print("PbsConsumer is already running ...")
    else:
        if connection == 0:
            if which == 'stop':
                print("Shutting down PbsConsumer ...")
                sendJobToConsumer('poisonpill')
            elif which == 'log':
                print("Printing current PbsConsumer log ...")
                # TODO
        else:
            print("No server found, nothing to do ...")

def slurmconsumer(which):
    connection = checkConsumerConnection()
    if which == 'check':
        return print(connection)
    elif which == 'start':
        if connection != 0:
            print("Starting SlurmConsumer ...")
            homedir = os.path.expanduser('~/.slurmconsumer')
            if not os.path.exists(homedir):
                os.mkdir(homedir)
            elif not os.path.isdir(homedir):
                raise FileExistsError("%s is not a directory" % homedir)
            subprocess.Popen(["nohup", "python", "-m", "wpipe.scheduler.SlurmConsumer"], cwd=homedir)
            while checkConsumerConnection() != 0:
                pass
        else:
            print("SlurmConsumer is already running ...")
    else:
        if connection == 0:
            if which == 'stop':
                print("Shutting down SlurmConsumer ...")
                sendJobToConsumer('poisonpill')
            elif which == 'log':
                print("Printing current SlurmConsumer log ...")
                # TODO
        else:
            print("No server found, nothing to do ...")

def base_consumer(which, kind: str):
    if kind not in SUPPORTED_SCHEDULERS:
        raise ValueError(f"Job scheduler '{kind}' is not supported: choose in {SUPPORTED_SCHEDULERS}")
    connection = checkConsumerConnection()
    if which == 'check':
        return print(connection)
    elif which == 'start':
        if connection != 0:
            print(f"Starting {kind}Consumer ...")
            homedir = os.path.expanduser(f"~/.{kind.lower()}consumer")
            if not os.path.exists(homedir):
                os.mkdir(homedir)
            elif not os.path.isdir(homedir):
                raise FileExistsError("%s is not a directory" % homedir)
            subprocess.Popen(["nohup", "python", "-m", f"wpipe.scheduler.{kind}Consumer"], cwd=homedir)
            while checkConsumerConnection() != 0:
                pass
        else:
            print(f"{kind}Consumer is already running ...")
    else:
        if connection == 0:
            if which == 'stop':
                print(f"Shutting down {kind}Consumer ...")
                sendJobToConsumer('poisonpill')
            elif which == 'log':
                print(f"Printing current {kind}Consumer log ...")
                # TODO
        else:
            print("No server found, nothing to do ...")
