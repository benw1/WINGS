import pandas as pd
import numpy as np
import time

from pipebackbone import Configuration, Event, Node, Store, Options, Task
from pipebackbone.wpipe import update_time

class Job():
    def __init__(self, state='any', event_id=0,
                 task=Task().new(),
                 config=Configuration().new(),
                 node=Node().new()):
        self.state = np.array([str(state)])
        self.job_id = np.array([int(0)])
        self.event_id = np.array([int(event_id)])
        self.task_id = np.array([int(task.task_id)])
        self.config_id = np.array([int(config.config_id)])
        self.node_id = np.array([int(node.node_id)])
        self.pipeline_id = np.array([int(config.pipeline_id)])
        self.starttime = pd.to_datetime(time.time(), unit='s')
        self.endtime = pd.to_datetime(time.time(), unit='s')
        self.timestamp = pd.to_datetime(time.time(), unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = update_time((pd.MultiIndex.from_arrays(arrays=
                                                           [np.array([int(self.pipeline_id)]),
                                                            np.array([int(self.task_id)]),
                                                            np.array([int(self.config_id)]),
                                                            np.array([int(self.event_id)]),
                                                            np.array([int(self.job_id)])],
                                                           names=(
                                                           'pipelineID', 'taskID', 'configID', 'eventID', 'jobID'))))
        _df.endtime = _df.timestamp.copy()
        return _df

    def create(self, options={'completed': 0}, ret_opt=False, store=Store()):
        _df = store.create('jobs', 'job_id', self)
        _opt = Options(options).create('job', int(_df.job_id), store=store)
        if ret_opt:
            return _df, _opt
        else:
            return _df

    def get(job_id, store=Store()):
        x = store.select('jobs', 'job_id==' + str(job_id))
        return x.loc[x.index.values[0]]

    def getEvent(job,
                 name='any', value='0', jargs='0',
                 options={'any': 0}, store=Store()):
        return Event(name, value, jargs, job).create(options=options, store=store)
