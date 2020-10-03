# This class makes it so we can pickle and unpickle the info we need for the pbs scheduler for a job
# TODO: Add some sort of enforcement of attributes
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