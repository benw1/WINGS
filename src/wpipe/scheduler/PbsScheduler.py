from .BaseScheduler import BaseScheduler
import os
from datetime import datetime

class PbsScheduler(BaseScheduler):

    # Keep track of all the instances that might be spawned
    schedulers = list()

    def __init__(self, job):
        super().__init__()
        print("Check schedulers and Create a new scheduler if necessary")
        #self.key = self.task.name
        self.key = PbsKey(schedulers,job)
        test_new = self.key.equals(self)
        if test_new == 0:
           PbsScheduler.schedulers.append(self) # add this new scheduler to the list
        if test_new > 0:
           pass
        # run the submit now that the object is created
        self._submit(scheduler)
        # run the submit now that the object is created
        self._submitJob(job)
    class PbsKey:
        def __init__(self,schedulers,event)
            self.key = self.pipeline.pipeline_id+job.task.name
        def equals(self):
            check = 0
            for key in self.schedulers:
                if self.schedulers[key] == self.key
                   check = 1
            return(check)
 

    #######################
    ## Internal Use Only ##
    #######################

    def _submit(self,scheduler):
        # TODO: Probably need a pass in variable
        print("do a reset")
        scheduler.jobList.append(self.task.name+" -j "+"self.job.job_id)
        # TODO: Probably need a pass in variable

        # TODO: submit list of jobs


        # Reset the scheduler
        super().reset()


    def _execute(self):
        print("We do the scheduling now from: " + self.key)
        #Put pbs and list of tasks in the configuration root
        pbsfilepath = setup_pbs(self)
        qsub_command = "qsub "+pbsfilepath
        pbs_id = os.popen(qsub_command) 
        ##Need to keep track of the pbs job id.  Where can we put this?

        # TODO: Use PbsKey to better cleanup the now unused scheduler
        # remove scheduler from list
        PbsScheduler.schedulers.remove(self)

    def setup_pbs(self):
        # TODO: Ben put more white space in.
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y-%H-%M-%S.%f")
        pbsfilename = dt_string+".pbs #name it with the current time
        executables_file = dt_string+".list #name it with the current time
        pbsfilepath = self.pipeline.config_root+"/"+pbsfilename
        executabls_path = self.pipeline.config_root+"/"+executables_file
        njobs = len(self.jobs)
        pbsfile = open(pbsfilepath,"w")
        pbsfile.write("#PBS -S /bin/bash\n")
        pbsfile.write("#PBS -j oe\n")
        pbsfile.write("#PBS -l select=1:ncpus=10:model=has\n")
        pbsfile.write("#PBS -W group_list=s1692\n")
        pbsfile.write("#PBS -l walltime=24:00:00\n")
        pbsfile.write("source ~/.bashrc\n")
        pbsfile.write("cd ",self.pipeline.pipe_root\n)
        pbsfile.write("parallel --jobs",str(njobs),"--sshloginfile $PBS_NODEFILE --workdir $PWD < ",executables_list_path,"\n")
        pbsfile.close()

        executables_list = open(executables_path,"w")

        for job in self.jobs:
            executable = job.task.name
            jobid = job.job_id 
            executables_list.write(str(executable)," -j ",jobid)
        executables_list.close()

        return(pbsfilepath)        

    @staticmethod
    def _checkForScheduler(event):
        # This will check for an existing scheduler and return it if it exists
        tempKey = PbsKey(event)
        for scheduler in PbsScheduler.schedulers:
            if (scheduler.key.equals(tempKey)): # TODO: Make sure this works properly with the PbsKey
                return (True, scheduler)
        return (False, None)

    ######################
    ### Usable Methods ###
    ######################

    @staticmethod
    def submit(event, job):

        # If no schedulers exist then create a new one and exit this method
        if (len(PbsScheduler.schedulers) == 0):
            PbsScheduler(event)
            return

        (hasScheduler, scheduler) = PbsScheduler._checkForScheduler(event)
        if (hasScheduler): # check for existing schedulers and call submitJob for the retrieved scheduler
            print("A scheduler with those attributes exists")
            scheduler._submitJob(job)
        else: # No scheduler was found but we need to do the scheduling
            PbsScheduler(event)

    ####################
    ## Nested Classes ##
    ####################

    # out of site and out of mind
    class PbsKey(object):

        def __init__(self, event):
            self.key = self.pipeline.pipeline_id+job.task.name

        def equals(self):
            ## TODO: Make thie return True or False
            check = 0
            for key in self.schedulers:
                if self.schedulers[key] == self.key:
                   check = 1
            return(check)
