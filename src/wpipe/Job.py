from .core import *
from .Store import Store
from .Node import Node, SQLNode
from .Owner import SQLOwner
from .Configuration import Configuration, SQLConfiguration
from .Task import Task, SQLTask


class Job:
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

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = update_time((pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                                                                   np.array([int(self.task_id)]),
                                                                   np.array([int(self.config_id)]),
                                                                   np.array([int(self.event_id)]),
                                                                   np.array([int(self.job_id)])],
                                                           names=('pipelineID', 'taskID', 'configID',
                                                                  'eventID', 'jobID'))))
        _df.endtime = _df.timestamp.copy()
        return _df

    def create(self, options={'completed': 0}, ret_opt=False, store=Store()):
        from . import Options
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
        from . import Event
        return Event(name, value, jargs, job).create(options=options, store=store)


class SQLJob(SQLOwner):
    def __init__(self, config, task=None, event=None, node=None,
                 state='any', options={}):
        super().__init__()
        try:
            self._job = si.session.query(si.Job). \
                filter_by(config_id=config.config_id). \
                filter_by(node_id=node.node_id)
            if task is not None:
                self._job = self._job. \
                    filter_by(task_id=task.task_id)
            if event is not None:
                self._job = self._job. \
                    filter_by(firing_event_id=event.event_id)
            if node is not None:
                self._job = self._job. \
                    filter_by(node_id=node.node_id)
            self._job = self._job.one()
            self._child_events = []
            from .Event import SQLEvent
            for child_event in self._job.child_events:
                if hasattr(child_event, '_wpipe_object'):
                    self._child_events.append(child_event._wpipe_object)
                else:
                    self._child_events.append(SQLEvent(self, child_event.name))
        except si.orm.exc.NoResultFound:
            self._job = si.Job(state=state)
            config._config.jobs.append(self._job)
            if task is not None:
                task._task.jobs.append(self._job)
            if event is not None:
                event._event.fired_jobs.append(self._job)
            if node is not None:
                node._node.jobs.append(self._job)
            self._job.starttime = datetime.datetime.utcnow()
            self._job.endtime = datetime.datetime.utcnow()
            self._child_events = []
        self._owner = self._job
        self.options = options
        self._job.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def state(self):
        si.session.commit()
        return self._job.state

    @property
    def job_id(self):
        si.session.commit()
        return self._job.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._job.timestamp

    @property
    def starttime(self):
        si.session.commit()
        return self._job.starttime

    @property
    def endtime(self):
        si.session.commit()
        return self._job.endtime

    @property
    def task_id(self):
        si.session.commit()
        return self._job.task_id

    @property
    def node_id(self):
        si.session.commit()
        return self._job.node_id

    @property
    def config_id(self):
        si.session.commit()
        return self._job.config_id

    @property
    def pipeline_id(self):
        si.session.commit()
        return self._job.config.pipeline_id

    @property
    def firing_event_id(self):
        si.session.commit()
        return self._job.firing_event_id

    @property
    def child_events(self):
        return self._child_events

    def create_event(self, name, **kwargs):
        from .Event import SQLEvent
        si.session.commit()
        self._child_events.append(SQLEvent(self, name, **kwargs))
        return self._child_events[-1]
