#!/usr/bin/env python
"""
Contains the PbsScheduler class definition

Please note that this module is private. The PbsScheduler class is
available in the main ``wpipe`` namespace - use that instead.
"""
from .core import time, subprocess
import asyncio
# import jinja

__all__ = ['PbsScheduler']


class PbsScheduler(object):
    def __init__(self, event, job):
        self.cores = 0
        self.memory = 0

    def submit(self, event, job):
        options = event.options
        try:
            self.cores = options['cores']
        except KeyError:
            self.cores = 1
        try:
            self.memory = options['memory']
        except KeyError:
            self.memory = 100
        task = job.task.executable + ' -j ' + str(job.job_id)
        # Put job into PBS file
        # start async callback of execute method

    def execute(self, event,):
        my_pipe = event.pipeline
        pipedir = my_pipe.pipe_root
        # generate task list and pbs script files
        # pbs_command="qsub "+pbs_file
        subprocess.run(pbs_command, shell=True)
