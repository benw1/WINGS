from .BaseScheduler import BaseScheduler
import os
from datetime import datetime

class PbsScheduler(BaseScheduler):

    # Keep track of all the instances that might be spawned
    schedulers = list()

    def __init__(self, job):
        super().__init__()
        print("Create a new scheduler")
        self.key = self.task.name
        #self.key = PbsKey(schedulers,job)
        test_new = self.key.equals(self)
        PbsScheduler.schedulers.append(self) # add this new scheduler to the list

        # run the submit now that the object is created
        self._submit()
    ### Not sure we need this, since we can probaby define the key in the PbsScheduler object...
    class PbsKey:
        def __init__(self,schedulers,event)
            self.key = job.task.name
        return 
    def equals(self):
        check = 0
        for key in self.schedulers:
            if self.schedulers[key] == self.key
               check = 1
        return(check)
 
            
    #######################
    ## Internal Use Only ##
    #######################

    def _submit(self):
        # TODO: Probably need a pass in variable
        print("do a reset")
        super().reset()


    def _execute(self):
        print("We do the scheduling now from: " + self.key)
        #Put pbs and list of tasks in the configuration root
        now = datetime.now()
        dt_string = now.strftime("%d-%m-%Y-%H-%M-%S.%f")
        pbsfilename = dt_string+".pbs #name it with the current time
        executables_file = dt_string+".list #name it with the current time
        pbsfilepath = self.event.target.conf_root+pbsfilename
        executabls_path = self.event.target.conf_root+executables_file
        njobs = len(self.jobs)
        pbsfile = open(pbsfilepath,"w")
        pbsfile.write("#PBS -S /bin/bash\n")
        pbsfile.write("#PBS -j oe\n")
        pbsfile.write("#PBS -l select=1:ncpus=10:model=has\n")
        pbsfile.write("#PBS -W group_list=s1692\n")
        pbsfile.write("#PBS -l walltime=24:00:00\n")
        pbsfile.write("source ~/.bashrc\n")
        pbsfile.write("cd ",self.event.target.proc_root\n)
        pbsfile.write("parallel --jobs",str(njobs),"--sshloginfile $PBS_NODEFILE --workdir $PWD < ",executables_list_path,"\n")
        pbsfile.close()
        executables_list = open(executables_path,"w")
        for job in self.jobs:
            executable = job.task.name
            jobid = job.job_id 
            executables_list.write(str(executable)," -j ",jobid)
        executables_list.close()
        qsub_command = "qsub "+pbsfilepath
        pbs_id = os.popen(qsub_command) 
        ##Need to keep track of the pbs job id.  Where can we put this?
        # remove scheduler from list
        PbsScheduler.schedulers.remove(self)


    @staticmethod
    def _checkForScheduler(key):
        for scheduler in PbsScheduler.schedulers:
            if (scheduler.key == key):
                return (True, scheduler)
        return (False, None)

    ######################
    ### Usable Methods ###
    ######################

    @staticmethod
    def submit(event, job):
        # Check if any scheduler exists.
        print(event)
        print(job)
        if (len(PbsScheduler.schedulers) == 0):  # !self._checkScheduler(stuff)):
            PbsScheduler(event)
            return

        (hasScheduler, scheduler) = PbsScheduler._checkForScheduler(key)
        if (hasScheduler): # check schedulers and submit to one
            print("A scheduler with those attributes exists")
            scheduler._submit()
        else: # No scheduler was found but we need to do the scheduling
            PbsScheduler(event)
