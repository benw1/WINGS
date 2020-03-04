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
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object
        cls._task = args[0] if len(args) else None
        if not isinstance(cls._task, si.Task):
            # gathering construction arguments
            pipeline = kwargs.get('pipeline', args[0] if len(args) else None)
            name = kwargs.get('name', args[1] if len(args) > 1 else None)
            nruns = kwargs.get('nruns', 0)
            run_time = kwargs.get('run_time', 0)
            is_exclusive = kwargs.get('is_exclusive', 0)
            # querying the database for existing row or create
            try:
                cls._task = si.session.query(si.Task). \
                    filter_by(pipeline_id=pipeline.pipeline_id). \
                    filter_by(name=name).one()
            except si.orm.exc.NoResultFound:
                cls._task = si.Task(name=name,
                                    nruns=nruns,
                                    run_time=run_time,
                                    is_exclusive=is_exclusive)
                pipeline._pipeline.tasks.append(cls._task)
            # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Task', __name__)
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_masks_proxy'):
            self._masks_proxy = ChildrenProxy(self._task, 'masks', 'Mask', __name__)
        if not hasattr(self, '_jobs_proxy'):
            self._jobs_proxy = ChildrenProxy(self._task, 'jobs', 'Job', __name__,
                                             child_attr='id')
        self._task.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        si.session.commit()
        return self._task.name

    @name.setter
    def name(self, name):
        self._task.name = name
        self._task.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def task_id(self):
        si.session.commit()
        return self._task.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._task.timestamp

    @property
    def nruns(self):
        si.session.commit()
        return self._task.nruns

    @property
    def run_time(self):
        si.session.commit()
        return self._task.run_time

    @property
    def is_exclusive(self):
        si.session.commit()
        return self._task.is_exclusive

    @property
    def pipeline_id(self):
        si.session.commit()
        return self._task.pipeline_id

    @property
    def masks(self):
        return self._masks_proxy

    @property
    def jobs(self):
        return self._jobs_proxy
