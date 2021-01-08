from .. import si


# This class makes it so we can pickle and unpickle the info we need for the pbs scheduler for a job
class JobData:
    def __init__(self, job):
        # Attributes for PbsKey
        self._task_name = job.task.name

        # Attributes for files
        self._pipeline_pipe_root = job.pipeline.pipe_root
        self._pipeline_config_root = job.pipeline.config_root

        # Attributes for command to execute
        self._task_executable = job.task.executable
        self._pipeline_id = job.pipeline_id
        self._pipeline_username = job.pipeline.user_name
        self._job_id = job.job_id
        self._verbose = si.core.verbose

        print(job.firing_event.options)
        event_options = job.firing_event.options
        try:
            self._job_time = event_options['job_time']
        except KeyError:
            self._job_time = None


    # These are required
    def validate(self):
        errors = ""
        if self._task_name is None:
            errors += "Job for scheduler has no task name\n"
        if self._pipeline_pipe_root is None:
            errors += "Job for scheduler has no pipe root\n"
        if self._pipeline_config_root is None:
            errors += "Job for scheduler has no pipeline configuration root\n"
        if self._task_executable is None:
            errors += "Job for scheduler has no task executable\n"
        if self._pipeline_id is None:
            errors += "Job for scheduler has no pipeline id\n"
        if self._pipeline_username is None:
            errors += "Job for scheduler has no pipeline username\n"
        if self._job_id is None:
            errors += "Job for scheduler has no job id\n"
        return errors

    def getTaskName(self):
        return self._task_name

    def getPipelinePipeRoot(self):
        return self._pipeline_pipe_root

    def getPipelineConfigRoot(self):
        return self._pipeline_config_root

    def getTaskExecutable(self):
        return self._task_executable

    def getPipelineId(self):
        return self._pipeline_id

    def getPipelineUserName(self):
        return self._pipeline_username

    def getJobId(self):
        return self._job_id

    def getVerbose(self):
        return self._verbose

    def getTime(self):
        return self._job_time

    def setTime(self, time):
        self._job_time = time

    # For pretty printing or logging
    def toString(self):
        string = 'JobData:\n'
        string += '\tTask Name: {}\n'.format(self.getTaskName())
        string += '\tPipeline Pipe Root: {}\n'.format(self.getPipelinePipeRoot())
        string += '\tPipeline Config Root: {}\n'.format(self.getPipelineConfigRoot())
        string += '\tTask Executable: {}\n'.format(self.getTaskExecutable())
        string += '\tPipeline ID: {}\n'.format(self.getPipelineId())
        string += '\tPipeline Username: {}\n'.format(self.getPipelineUserName())
        string += '\tJob ID: {}\n'.format(self.getJobId())
        string += '\tSet verbosity to: {}\n'.format(self.getVerbose())
        if self.getTime() is not None:
            string += '\tJob Time: {}\n'.format(self.getTime())
        return string