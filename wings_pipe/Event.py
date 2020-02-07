import pandas as pd
import numpy as np

from wings_pipe import Job, Options, Store
from wings_pipe.wpipe import update_time

class Event():
    def __init__(self, name='', value='', jargs='', job=Job().new()):
        self.job_id = np.array([int(job.job_id)])
        self.jargs = np.array([str(jargs)])
        self.name = np.array([str(name)])
        self.value = np.array([str(value)])
        self.event_id = np.array([int(0)])
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.event_id
        return update_time(_df)

    def create(self, options={'any': 0}, ret_opt=False, store=Store()):
        _df = store.create('events', 'event_id', self)
        _opt = Options(options).create('event', int(_df.event_id), store=store)
        if ret_opt:
            return _df, _opt
        else:
            return _df

    def get(event_id, store=Store()):
        return store.select('events').loc[int(event_id)]

    def run_complete(event_id=0, store=Store()):
        event = Event.get(int(event_id))
        job_id = int(event.job_id)
        jobOpt = Options.get('job', int(job_id))
        jobOpt['completed'] = int(jobOpt['completed']) + 1
        return store.update('options', Options(jobOpt).new('job', job_id))