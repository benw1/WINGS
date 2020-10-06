import datetime

from .BaseScheduler import BaseScheduler
from .TemplateFactory import TemplateFactory
import subprocess


class PbsScheduler(BaseScheduler):
    # Keep track of all the instances that might be spawned
    schedulers = list()

    def __init__(self, job):
        super().__init__()
        print("Creating a new scheduler")

        self._key = self.PbsKey(job)
        self._jobList = list()

        PbsScheduler.schedulers.append(self)  # add this new scheduler to the list

        # run the submit now that the object is created
        self._submitJob(job)

    #######################
    ## Internal Use Only ##
    #######################

    def _submitJob(self, job):
        # TODO: Change to event later
        print("do a reset")

        self._jobList.append(job)

        # Reset the scheduler
        super().reset()

    def _execute(self):
        print("We do the scheduling now from: " + self._key.getKey())

        now = datetime.datetime.now()
        dt_string = now.strftime("%d-%m-%Y-%H-%M-%S.%f")

        pbsfilename = dt_string + ".pbs"  # name it with the current time
        executables_file = dt_string + ".list"  # name it with the current time

        pbsfilepath = self._jobList[0].pipeline.config_root + "/" + pbsfilename
        executables_path = self._jobList[0].pipeline.config_root + "/" + executables_file

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
    def _checkForScheduler(job):
        # This will check for an existing scheduler and return it if it exists
        tempKey = PbsScheduler.PbsKey(job)

        for scheduler in PbsScheduler.schedulers:
            if scheduler._key.equals(tempKey):
                return True, scheduler

        return False, None

    def _makeJobList(self):
        template = TemplateFactory.getJobListTemplate()

        # Make job list into a dictionary to pass to jinja2
        jobsForJinja = list()
        for job in self._jobList:
            jobsForJinja.append(
                {'command': job.task.executable + ' -p ' + str(job.pipeline_id) +
                 ' -u ' + str(job.pipeline.user_name) + ' -j ' + str(job.job_id)})

        output = template.render(jobs=jobsForJinja)
        print()
        print("Jinja commands:")
        print(output)

        return output

    def _makePbsFile(self, exectuablesListPath):

        # template = jinjaEnv.get_template('PbsFile.jinja')
        template = TemplateFactory.getPbsFileTemplate()

        # create a dictionary
        pbsDict = {'njobs': len(self._jobList), 'pipe_root': self._jobList[0].pipeline.pipe_root,
                   'executables_list_path': exectuablesListPath}

        output = template.render(pbs=pbsDict)

        print()
        print("Jinja Pbs File:")
        print(output)
        return output

    ######################
    ### Usable Methods ###
    ######################

    @staticmethod
    def submit(job):
        # If no schedulers exist then create a new one and exit this method
        if len(PbsScheduler.schedulers) == 0:
            PbsScheduler(job)
            return

        (hasScheduler, scheduler) = PbsScheduler._checkForScheduler(job)
        if hasScheduler:  # check for existing schedulers and call submitJob for the retrieved scheduler
            print("A scheduler with those attributes exists")
            scheduler._submitJob(job)
        else:  # No scheduler was found but we need to do the scheduling
            PbsScheduler(job)

    ####################
    ## Nested Classes ##
    ####################

    # out of site and out of mind
    class PbsKey(object):

        def __init__(self, job):
            # TODO: Make this into a dictionary later
            # TODO: Change this to event type
            self._key = str(job.pipeline.pipeline_id) + job.task.name
            print("This is our key: " + self._key)

        def equals(self, other):
            if self._key == other.getKey():
                return True
            return False

        def getKey(self):
            return self._key
