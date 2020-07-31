from .BaseScheduler import BaseScheduler
from jinja2 import Environment, PackageLoader, FileSystemLoader, select_autoescape

class PbsScheduler(BaseScheduler):

    # Keep track of all the instances that might be spawned
    schedulers = list()

    def __init__(self, job):
        super().__init__()
        print("creating a new scheduler")
        #self.key = self.task.name

        self._key = self.PbsKey(job)
        self._jobList = list()

        PbsScheduler.schedulers.append(self) # add this new scheduler to the list

        # run the submit now that the object is created
        self._submitJob(job)


    #######################
    ## Internal Use Only ##
    #######################

    def _submitJob(self, job):
        # TODO: Probably need a pass in variable
        print("do a reset")

        # TODO: submit list of jobs
        # self.jobList.append(self.task.name+" -j "+"self.job.job_id+"\n")
        self._jobList.append(job)

        # Reset the scheduler
        super().reset()

    def _execute(self):
        print("We do the scheduling now from: " + self._key.getKey())
        #Put pbs and list of tasks in the configuration root
        # pbsfilepath = setup_pbs(self)
        # qsub_command = "qsub "+pbsfilepath
        # pbs_id = os.popen(qsub_command)
        # ##Need to keep track of the pbs job id.  Where can we put this?
        #
        for job in self._jobList:
            print(job.task.executable, '-p', str(job.pipeline_id), '-u', str(job.pipeline.user_name),
            '-j', str(job.job_id))

        # Load the jinja file
        env = Environment(loader=FileSystemLoader("/home/tristan/workspace/WINGS/src/wpipe/templates"))

        self._makeJobList(env)
        # TODO: Use PbsKey to better cleanup the now unused scheduler
        # remove scheduler from list
        PbsScheduler.schedulers.remove(self)

    # def _setup_pbs(self):
    #     # TODO: Ben put more white space in.
    #     now = datetime.now()
    #     dt_string = now.strftime("%d-%m-%Y-%H-%M-%S.%f")
    #     pbsfilename = dt_string+".pbs #name it with the current time
    #     executables_file = dt_string+".list #name it with the current time
    #     pbsfilepath = self.pipeline.config_root+"/"+pbsfilename
    #     executabls_path = self.pipeline.config_root+"/"+executables_file
    #     njobs = len(self.jobs)
    #     pbsfile = open(pbsfilepath,"w")
    #     pbsfile.write("#PBS -S /bin/bash\n")
    #     pbsfile.write("#PBS -j oe\n")
    #     pbsfile.write("#PBS -l select=1:ncpus=10:model=has\n")
    #     pbsfile.write("#PBS -W group_list=s1692\n")
    #     pbsfile.write("#PBS -l walltime=24:00:00\n")
    #     pbsfile.write("source ~/.bashrc\n")
    #     pbsfile.write("cd ",self.pipeline.pipe_root\n)
    #     pbsfile.write("parallel --jobs",str(njobs),"--sshloginfile $PBS_NODEFILE --workdir $PWD < ",executables_list_path,"\n")
    #     pbsfile.close()
    #
    #     executables_list = open(executables_path,"w")
    #
    #
    #     #NEED TO DETERMINE HOW TO GET THIS LIST TO BE JUST THE ONE FOR THIS SCHEDULER, AND NOT ALL JOBS FOR ALL SCHEDULERS
    #
    #     for job in self.jobList:
    #         executables_list.write(job)
    #     executables_list.close()
    #
    #     return(pbsfilepath)

    @staticmethod
    def _checkForScheduler(job):
        # This will check for an existing scheduler and return it if it exists
        tempKey = PbsScheduler.PbsKey(job)
        for scheduler in PbsScheduler.schedulers:
            if (scheduler._key.equals(tempKey)): # TODO: Make sure this works properly with the PbsKey
                print("We found an existing scheduler")
                return (True, scheduler)
        return (False, None)

    def _makeJobList(self, jinjaEnv):

        print(jinjaEnv.list_templates())
        template = env.get_template('SchedulerJobList.jinja')

        # Make job list into a dictionary to pass to jinja2
        jobsForJinja = list()
        for job in self._jobList:
            jobsForJinja.append({'command' : job.task.executable + ' -p ' + str(job.pipeline_id) + ' -u ' + str(job.pipeline.user_name) +
            ' -j ' + str(job.job_id)})

        output = template.render(jobs=jobsForJinja)
        print()
        print("Jinja commands:")
        print(output)

        return output


    ######################
    ### Usable Methods ###
    ######################

    @staticmethod
    def submit(event):
        job = event.parent_job
        # If no schedulers exist then create a new one and exit this method
        if (len(PbsScheduler.schedulers) == 0):
            PbsScheduler(job)
            return

        (hasScheduler, scheduler) = PbsScheduler._checkForScheduler(job)
        if (hasScheduler): # check for existing schedulers and call submitJob for the retrieved scheduler
            print("A scheduler with those attributes exists")
            scheduler._submitJob(job)
        else: # No scheduler was found but we need to do the scheduling
            PbsScheduler(job)

    ####################
    ## Nested Classes ##
    ####################

    # out of site and out of mind
    class PbsKey(object):

        def __init__(self, job):
            # TODO: Make this into a dictionary later
            self._key = str(job.pipeline.pipeline_id) + job.task.name
            print("This is our key: " + self._key)

        def equals(self, otherPbsKey):
            # TODO: Make thie return True or False
            if self._key == otherPbsKey.getKey():
                return True
            return False

        def getKey(self):
            return self._key