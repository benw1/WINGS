from .core import *
from .Store import Store
from .Pipeline import Pipeline


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
        # checking if given argument is sqlintf object or existing id
        cls._task = args[0] if len(args) else None
        if not isinstance(cls._task, si.Task):
            id = kwargs.get('id', cls._task)
            if isinstance(id, int):
                cls._task = si.session.query(si.Task).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=4)
                pipeline = kwargs.get('pipeline', wpargs.get('Pipeline', None))
                name = kwargs.get('name', args[0])
                nruns = kwargs.get('nruns', 0 if args[1] is None else args[1])
                run_time = kwargs.get('run_time', 0 if args[2] is None else args[2])
                is_exclusive = kwargs.get('is_exclusive', 0 if args[3] is None else args[3])
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
        wpipe_to_sqlintf_connection(cls, 'Task')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_masks_proxy'):
            self._masks_proxy = ChildrenProxy(self._task, 'masks', 'Mask')
        if not hasattr(self, '_jobs_proxy'):
            self._jobs_proxy = ChildrenProxy(self._task, 'jobs', 'Job',
                                             child_attr='id')
        self._task.timestamp = datetime.datetime.utcnow()
        self.register()
        si.session.commit()

    @property
    def parents(self):
        return self.pipeline

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
    def pipeline(self):
        if hasattr(self._task.pipeline, '_wpipe_object'):
            return self._task.pipeline._wpipe_object
        else:
            from .Pipeline import SQLPipeline
            return SQLPipeline(self._task.pipeline)

    @property
    def masks(self):
        return self._masks_proxy

    @property
    def jobs(self):
        return self._jobs_proxy

    def mask(self, *args, **kwargs):
        from .Mask import SQLMask
        return SQLMask(self, *args, **kwargs)

    def job(self, *args, **kwargs):
        from .Job import SQLJob
        return SQLJob(self, *args, **kwargs)

    def register(self):
        _temp = __import__(os.path.basename(self.pipeline.software_root) + '.' + self.name.replace('.py', ''),
                           fromlist=[''])
        if hasattr(_temp, 'register'):
            _temp.register(self)
        else:
            warnings.warn("Task " + self.pipeline.software_root + '/' + self.name +
                          " cannot be registered: no 'register' function")
