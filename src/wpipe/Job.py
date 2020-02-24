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
    def __init__(self, task, node, config, state='any',
                 options={}):
        super().__init__()
        try:
            self._job = si.session.query(si.Job). \
                filter_by(task_id=task.task_id). \
                filter_by(node_id=node.node_id). \
                filter_by(config_id=config.config_id).one()
        except si.orm.exc.NoResultFound:
            self._job = si.Job(state=state)
            task._task.jobs.append(self._job)
            node._node.jobs.append(self._job)
            config._config.jobs.append(self._job)
            self._job.starttime = datetime.datetime.utcnow()
            self._job.endtime = datetime.datetime.utcnow()
        self._owner = self._job
        self.options = options
        self._job.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def state(self):
        return self._job.state

    @property
    def job_id(self):
        return self._job.id

    @property
    def timestamp(self):
        return self._job.timestamp

    @property
    def starttime(self):
        return self._job.starttime

    @property
    def endtime(self):
        return self._job.endtime

    @property
    def task_id(self):
        return self._job.task_id

    @property
    def node_id(self):
        return self._job.node_id

    @property
    def config_id(self):
        return self._job.config_id

    @property
    def pipeline_id(self):
        return self._job.config.pipeline_id

    @property
    def event_id(self):
        return self._job.event.id
