from .core import *
from .Store import Store
from .Pipeline import Pipeline, SQLPipeline


class Task:
    def __init__(self, name='any',
                 pipeline=Pipeline().new(),
                 nruns=0, run_time=0,
                 is_exclusive=0):
        self.name = np.array([str(name)])
        self.pipeline_id = np.array([int(pipeline.pipeline_id)])
        self.task_id = np.array([int(0)])
        self.nruns = np.array([int(nruns)])
        self.run_time = np.array([float(run_time)])
        self.is_exclusive = np.array([bool(is_exclusive)])
        self.timestamp = pd.to_datetime(time.time(), unit='s')

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                                                      np.array([int(self.task_id)])], names=('pipelineID', 'taskID'))
        return update_time(_df)

    def create(self, store=Store()):
        _df = store.create('tasks', 'task_id', self)
        return _df

    def add_mask(task, source='any', name='any', value='0', store=Store()):
        from . import Mask
        return Mask(task, source, name, value).create(store=store)

    def get(task_id, store=Store()):
        x = store.select('tasks', 'task_id==' + str(task_id))
        return x.loc[x.index.values[0]]


class SQLTask:
    def __init__(self, pipeline, name,
                 nruns=0, run_time=0,
                 is_exclusive=0):
        try:
            self._task = si.session.query(si.Task). \
                filter_by(pipeline_id=pipeline.pipeline_id). \
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            self._task = si.Task(name=name,
                                 nruns=nruns,
                                 run_time=run_time,
                                 is_exclusive=is_exclusive)
            pipeline._pipeline.tasks.append(self._task)
        self._task.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        return self._task.name

    @name.setter
    def name(self, name):
        self._task.name = name
        self._task.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def task_id(self):
        return self._task.id

    @property
    def timestamp(self):
        return self._task.timestamp

    @property
    def nruns(self):
        return self._task.nruns

    @property
    def run_time(self):
        return self._task.run_time

    @property
    def is_exclusive(self):
        return self._task.is_exclusive

    @property
    def pipeline_id(self):
        return self._task.pipeline_id

    @property
    def jobs(self):
        return list(map(lambda job: job.name, self._task.jobs))
