import datetime

from .BaseScheduler import BaseScheduler
from .TemplateFactory import TemplateFactory
import subprocess


class PbsScheduler(BaseScheduler):
    # Keep track of all the instances that might be spawned
    schedulers = list()

    def __init__(self, jobdata):

        super().__init__(jobdata.getTime() if jobdata.getTime() is not None else 20) # passed in value or default timer amount (seconds).
        print("Creating a new scheduler ...")

        self._key = self.PbsKey(jobdata)
        self._jobList = list()

        PbsScheduler.schedulers.append(self)  # add this new scheduler to the list

        # run the submit now that the object is created
        self._submitJob(jobdata)

    #######################
    ## Internal Use Only ##
    #######################

    def _submitJob(self, jobdata):
        # TODO: Change to event later

        self._jobList.append(jobdata)

        # Reset the scheduler
        super().reset()

    def _execute(self):
        print("We do the scheduling now from: " + self._key.getKey())

        now = datetime.datetime.now()
        dt_string = now.strftime("%d-%m-%Y-%H-%M-%S.%f")

        pbsfilename = dt_string + ".pbs"  # name it with the current time
        executables_file = dt_string + ".list"  # name it with the current time

        pbsfilepath = self._jobList[0].getPipelineConfigRoot() + "/" + pbsfilename
        executables_path = self._jobList[0].getPipelineConfigRoot() + "/" + executables_file

        jobFileOutput = self._makeJobList()
        pbsFileOutput = self._makePbsFile(executables_path)

        with open(executables_path, 'w') as f:
            f.write(jobFileOutput)
        with open(pbsfilepath, 'w') as f:
            f.write(pbsFileOutput)

        # TODO: Test this out more
        output = subprocess.run("qsub %s" % (pbsfilepath), shell=True, capture_output=True)
        print("Qsub output:")
        print(output)

        # remove scheduler from list
        PbsScheduler.schedulers.remove(self)

    @staticmethod
    def _checkForScheduler(jobdata):
        # This will check for an existing scheduler and return it if it exists
        tempKey = PbsScheduler.PbsKey(jobdata)

        for scheduler in PbsScheduler.schedulers:
            if scheduler._key.equals(tempKey):
                return True, scheduler

        return False, None

    def _makeJobList(self):
        template = TemplateFactory.getJobListTemplate()

        # Make job list into a dictionary to pass to jinja2
        jobsForJinja = list()
        for jobdata in self._jobList:
            jobsForJinja.append(
                {'command': jobdata.getTaskExecutable() + ' -p ' + str(jobdata.getPipelineId()) +
                 ' -u ' + str(jobdata.getPipelineUserName()) + ' -j ' + str(jobdata.getJobId()) +
                 bool(jobdata.getVerbose())*' -v'})

        output = template.render(jobs=jobsForJinja)
        print()
        print("Jinja commands:")
        print(output)

        return output

    def _makePbsFile(self, executablesListPath):

        template = TemplateFactory.getPbsFileTemplate()

        # create a dictionary
        pbsDict = {'njobs': len(self._jobList), 'pipe_root': self._jobList[0].getPipelinePipeRoot(),
                   'executables_list_path': executablesListPath}

        output = template.render(pbs=pbsDict)

        print()
        print("Jinja Pbs File:")
        print(output)
        return output

    ######################
    ### Usable Methods ###
    ######################

    @staticmethod
    def submit(jobdata):
        # If no schedulers exist then create a new one and exit this method
        if len(PbsScheduler.schedulers) == 0:
            PbsScheduler(jobdata)
            return

        (hasScheduler, scheduler) = PbsScheduler._checkForScheduler(jobdata)
        if hasScheduler:  # check for existing schedulers and call submitJob for the retrieved scheduler
            print('Adding job to scheduler with key {} ...'.format(scheduler._key.getKey()))
            scheduler._submitJob(jobdata)
        else:  # No scheduler was found but we need to do the scheduling
            PbsScheduler(jobdata)

    ####################
    ## Nested Classes ##
    ####################

    # out of site and out of mind
    class PbsKey(object):

        def __init__(self, jobdata):
            #self._key = jobdata.getTaskName()  # For debugging
            self._key = str(jobdata.getPipelineId()) + jobdata.getTaskName()

        def equals(self, other):
            if self._key == other.getKey():
                return True
            return False

        def getKey(self):
            return self._key
