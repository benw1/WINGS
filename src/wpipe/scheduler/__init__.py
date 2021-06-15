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
PbsScheduler
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

from .PbsScheduler import PbsScheduler
from .PbsConsumer import checkPbsConnection, sendJobToPbs
from .JobData import JobData

__all__ = ['pbsconsumer', 'PbsScheduler', 'JobData', 'checkPbsConnection', 'sendJobToPbs']


def pbsconsumer(which):
    if which == 'start':
        connection = checkPbsConnection()
        if connection != 0:
            print("Starting PbsConsumer ...")
            homedir = os.path.expanduser('~/.pbsconsumer')
            if not os.path.exists(homedir):
                os.mkdir(homedir)
            elif not os.path.isdir(homedir):
                raise FileExistsError("%s is not a directory" % homedir)
            subprocess.Popen(["nohup", "python", "-m", "wpipe.scheduler.PbsConsumer"], cwd=homedir)
        else:
            print("PbsConsumer is already running ...")
    else:
        connection = checkPbsConnection()
        if connection == 0:
            if which == 'stop':
                print("Shutting down PbsConsumer ...")
                sendJobToPbs('poisonpill')
            elif which == 'log':
                print("Printing current PbsConsumer log ...")
                # TODO
        else:
            print("No server found, nothing to do ...")
