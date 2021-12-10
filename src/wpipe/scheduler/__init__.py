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

# from .PbsScheduler import PbsScheduler
from .PbsConsumer import checkPbsConnection, sendJobToPbs
from .JobData import JobData

__all__ = ['pbsconsumer', 'JobData', 'checkPbsConnection', 'sendJobToPbs']


def pbsconsumer(which):
    connection = checkPbsConnection()
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
            while checkPbsConnection() != 0:
                pass
        else:
            print("PbsConsumer is already running ...")
    else:
        if connection == 0:
            if which == 'stop':
                print("Shutting down PbsConsumer ...")
                sendJobToPbs('poisonpill')
            elif which == 'log':
                print("Printing current PbsConsumer log ...")
                # TODO
        else:
            print("No server found, nothing to do ...")
