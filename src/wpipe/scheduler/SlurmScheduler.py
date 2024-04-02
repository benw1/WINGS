#!/usr/bin/env python
"""
Contains the scheduler.SlurmScheduler class definition

Please note that this module is private. The scheduler.SlurmScheduler class is
available in the ``wpipe.scheduler`` namespace - use that instead.
"""
import datetime
import math

from .BaseScheduler import BaseScheduler
from .TemplateFactory import TemplateFactory
import subprocess

__all__ = ['DEFAULT_NODE_MODEL', 'DEFAULT_WALLTIME', 'SlurmScheduler']

DEFAULT_WALLTIME = '48:00:00'
DEFAULT_MEMORY = '50G'
DEFAULT_ACCOUNT = 'astro'
DEFAULT_PARTITION = 'astro'
DEFAULT_NODE_MODEL = 'has'
NODE_CORES_DICT = {'bro': 2 * 14, 'has': 2 * 12, 'ivy': 2 * 10, 'san': 2 * 8}



class SlurmScheduler(BaseScheduler):
    # Keep track of all the instances that might be spawned
    schedulers = list()

    def __init__(self, jobdata):

        super().__init__(
            jobdata.getTime() if jobdata.getTime() is not None else 20)  # passed in value or default timer amount (seconds).
        print("Creating a new scheduler ...")

        self._key = self.SlurmKey(jobdata)
        self._jobList = list()

        SlurmScheduler.schedulers.append(self)  # add this new scheduler to the list

        # run the submit now that the object is created
        self._submitJob(jobdata)

    #######################
    ## Internal Use Only ##
    #######################

    def _submitJob(self, jobdata, listmax=10):
        # TODO: Change to event later

        self._jobList.append(jobdata)

        # Reset the scheduler
       
        if (len(self._jobList) > listmax):
            super().run_it()
        else:
            super().reset()

    def _execute(self):
        print("We do the scheduling now from: " + self._key.getKey())

        now = datetime.datetime.now()
        dt_string = now.strftime("%d-%m-%Y-%H-%M-%S.%f")

        slurmfilename = dt_string + ".slurm"  # name it with the current time
        executables_file = dt_string + ".list"  # name it with the current time

        slurmfilepath = self._jobList[0].getPipelineConfigRoot() + "/" + slurmfilename
        executables_path = self._jobList[0].getPipelineConfigRoot() + "/" + executables_file

        jobFileOutput = self._makeJobList()
        slurmFileOutput = self._makeSlurmFile(executables_path)

        with open(executables_path, 'w') as f:
            f.write(jobFileOutput)
        with open(slurmfilepath, 'w') as f:
            f.write(slurmFileOutput)

        # TODO: Test this out more
        output = subprocess.run("sbatch %s" % (slurmfilepath), shell=True, capture_output=True)
        print("Sbatch output:")
        print(output)

        # remove scheduler from list
        SlurmScheduler.schedulers.remove(self)

    @staticmethod
    def _checkForScheduler(jobdata):
        # This will check for an existing scheduler and return it if it exists
        tempKey = SlurmScheduler.SlurmKey(jobdata)

        for scheduler in SlurmScheduler.schedulers:
            if scheduler._key.equals(tempKey):
                return True, scheduler

        return False, None

    def _makeJobList(self):
        template = TemplateFactory.getJobListTemplate()

        # Make job list into a dictionary to pass to jinja2
        jobsForJinja = list()
        node_cores = NODE_CORES_DICT
        node_model = self._jobList[0].getNodemodel()
        omp_threads = self._jobList[0].getJobOpenMP()
        n_cpus = node_cores[node_model]
        for jobdata in self._jobList:
            jobsForJinja.append(
                {'command': ("export OMP_NUM_THREADS=%d && " % n_cpus if omp_threads else "")
                            + "source ~/.bashrc && conda activate %s &&" % jobdata.getCondaEnv()
                            + jobdata.getTaskExecutable()
                            + ' -p ' + str(jobdata.getPipelineId())
                            + ' -u ' + str(jobdata.getPipelineUserName())
                            + ' -j ' + str(jobdata.getJobId())
                            + bool(jobdata.getVerbose()) * ' -v'})

        output = template.render(jobs=jobsForJinja)
        print()
        print("Jinja commands:")
        print(output)

        return output

    def _makeSlurmFile(self, executablesListPath):

        template = TemplateFactory.getSlurmFileTemplate()

        slurm_account = self._jobList[0].getAccount()
        slurm_partition = self._jobList[0].getPartition()
        node_cores = NODE_CORES_DICT
        node_model = self._jobList[0].getNodemodel()
        omp_threads = self._jobList[0].getJobOpenMP()
        n_jobs = len(self._jobList)
        n_nodes = [math.ceil(n_jobs / node_cores[node_model]), n_jobs][omp_threads]
        n_cpus = node_cores[node_model]
        n_jobs_per_node = [n_cpus, 1][omp_threads]
        omp_threads = ['', 'ompthreads=%d:' % n_cpus][omp_threads]

        # create a dictionary
        slurmDict = {'nnodes': n_nodes,
                   'njobs': n_jobs_per_node,
                   'walltime': self._jobList[0].getWalltime(),
                   'mem' : self._jobList[0].getMemory(),
                   'account' : self._jobList[0].getAccount(),
                   'partition' : self._jobList[0].getPartition(),
                   'jobid' : self._jobList[0].getJobId(),
                   'pipe_root': self._jobList[0].getPipelinePipeRoot(),
                   'executables_list_path': executablesListPath}

        output = template.render(slurm=slurmDict)

        print()
        print("Jinja Slurm File:")
        print(output)
        return output

    ######################
    ### Usable Methods ###
    ######################

    @staticmethod
    def submit(jobdata):
        # If no schedulers exist then create a new one and exit this method
        if len(SlurmScheduler.schedulers) == 0:
            SlurmScheduler(jobdata)
            return

        (hasScheduler, scheduler) = SlurmScheduler._checkForScheduler(jobdata)
        if hasScheduler:  # check for existing schedulers and call submitJob for the retrieved scheduler
            print('Adding job to scheduler with key {} ...'.format(scheduler._key.getKey()))
            scheduler._submitJob(jobdata)
        else:  # No scheduler was found but we need to do the scheduling
            SlurmScheduler(jobdata)

    ####################
    ## Nested Classes ##
    ####################

    # out of site and out of mind
    class SlurmKey(object):

        def __init__(self, jobdata):
            # self._key = jobdata.getTaskName()  # For debugging
            self._key = str(jobdata.getPipelineId()) + jobdata.getTaskName() + jobdata.getNodemodel() +\
                        jobdata.getWalltime() + ['', 'OpenMP'][jobdata.getJobOpenMP()]

        def equals(self, other):
            if self._key == other.getKey():
                return True
            return False

        def getKey(self):
            return self._key
