from .core import *


class Task:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._task = args[0] if len(args) else None
        if not isinstance(cls._task, si.Task):
            keyid = kwargs.get('id', cls._task)
            if isinstance(keyid, int):
                cls._task = si.session.query(si.Task).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=4)
                pipeline = kwargs.get('pipeline', wpargs.get('Pipeline', None))
                base, name = os.path.split(clean_path(kwargs.get('path', args[0])))
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
                    if base != pipeline.software_root:
                        shutil.copy2(base+'/'+name, pipeline.software_root+'/')
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
        si.session.commit()

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Task).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

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
            from .Pipeline import Pipeline
            return Pipeline(self._task.pipeline)

    @property
    def masks(self):
        return self._masks_proxy

    @property
    def jobs(self):
        return self._jobs_proxy

    def mask(self, *args, **kwargs):
        from .Mask import Mask
        return Mask(self, *args, **kwargs)

    def job(self, *args, **kwargs):
        from .Job import Job
        return Job(self, *args, **kwargs)

    def register(self):
        _temp = __import__(os.path.basename(self.pipeline.software_root) + '.' + self.name.replace('.py', ''),
                           fromlist=[''])
        if hasattr(_temp, 'register'):
            _temp.register(self)
        else:
            warnings.warn("Task " + self.pipeline.software_root + '/' + self.name +
                          " cannot be registered: no 'register' function")
