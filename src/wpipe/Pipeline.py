from .core import *
from .Store import Store
from .User import User


class Pipeline:
    def __init__(self, user=User().new(), name='any', software_root='',
                 data_root='', pipe_root='', config_root='',
                 description=''):
        self.name = np.array([str(name)])
        self.user_name = np.array([str(user['name'])])
        self.user_id = np.array([int(user.user_id)])
        self.pipeline_id = np.array([int(0)])
        self.software_root = np.array([str(software_root)])
        self.data_root = np.array([str(data_root)])
        self.pipe_root = np.array([str(pipe_root)])
        self.config_root = np.array([str(config_root)])
        self.description = np.array([str(description)])
        self.timestamp = pd.to_datetime(time.time(), unit='s')

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.pipeline_id
        return update_time(_df)

    def create(self, store=Store()):
        _df = store.create('pipelines', 'pipeline_id', self)
        return _df

    def get(pipeline_id, store=Store()):
        return store.select('pipelines').loc[int(pipeline_id)]


class SQLPipeline:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._pipeline = args[0] if len(args) else as_int(PARSER.parse_known_args()[0].pipeline)
        if not isinstance(cls._pipeline, si.Pipeline):
            id = kwargs.get('id', cls._pipeline)
            if isinstance(id, int):
                cls._pipeline = si.session.query(si.Pipeline).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=7)
                from . import DefaultUser
                user = kwargs.get('user', wpargs.get('User', DefaultUser))
                pipe_root = clean_path(kwargs.get('pipe_root',
                                                  PARSER.parse_known_args()[0].pipeline if args[0] is None
                                                  else args[0]))
                name = kwargs.get('name', os.path.basename(pipe_root) if args[1] is None else args[1])
                software_root = clean_path(kwargs.get('software_root',
                                                      'build' if args[2] is None else args[2]),
                                           root=pipe_root)
                data_root = clean_path(kwargs.get('data_root',
                                                  'data' if args[3] is None else args[3]),
                                       root=pipe_root)
                config_root = clean_path(kwargs.get('config_root',
                                                    'config' if args[4] is None else args[4]),
                                         root=pipe_root)
                description = kwargs.get('description',
                                         '' if args[5] is None else args[5])
                # querying the database for existing row or create
                try:
                    cls._pipeline = si.session.query(si.Pipeline). \
                        filter_by(user_id=user.user_id). \
                        filter_by(software_root=software_root).one()
                except si.orm.exc.NoResultFound:
                    cls._pipeline = si.Pipeline(name=name,
                                                pipe_root=pipe_root,
                                                software_root=software_root,
                                                data_root=data_root,
                                                config_root=config_root,
                                                description=description)
                    user._user.pipelines.append(cls._pipeline)
                    if not os.path.isdir(cls._pipeline.software_root):
                        os.mkdir(cls._pipeline.software_root)
                    if not os.path.isfile(cls._pipeline.software_root+'/__init__.py'):
                        with open(cls._pipeline.software_root+'/__init__.py', 'w') as file:
                            file.write("def register(task):\n    return")
                    if not os.path.isdir(cls._pipeline.data_root):
                        os.mkdir(cls._pipeline.data_root)
                    if not os.path.isdir(cls._pipeline.config_root):
                        os.mkdir(cls._pipeline.config_root)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Pipeline')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if self.pipe_root not in map(os.path.abspath, os.sys.path):
            os.sys.path.insert(0, self.pipe_root)
        if not hasattr(self, '_targets_proxy'):
            self._targets_proxy = ChildrenProxy(self._pipeline, 'targets', 'Target')
        if not hasattr(self, '_tasks_proxy'):
            self._tasks_proxy = ChildrenProxy(self._pipeline, 'tasks', 'Task')
        if not hasattr(self, '_dummy_task'):
            self._dummy_task = self.task('__init__.py')
        if not hasattr(self, '_dummy_job'):
            self._dummy_job = self.dummy_task.job()
        self._pipeline.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Pipeline).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        return self.user

    @property
    def name(self):
        si.session.commit()
        return self._pipeline.name

    @name.setter
    def name(self, name):
        self._pipeline.name = name
        self._pipeline.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def pipeline_id(self):
        si.session.commit()
        return self._pipeline.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._pipeline.timestamp

    @property
    def pipe_root(self):
        si.session.commit()
        return self._pipeline.pipe_root

    @property
    def software_root(self):
        si.session.commit()
        return self._pipeline.software_root

    @property
    def data_root(self):
        si.session.commit()
        return self._pipeline.data_root

    @property
    def config_root(self):
        si.session.commit()
        return self._pipeline.config_root

    @property
    def description(self):
        si.session.commit()
        return self._pipeline.description

    @description.setter
    def description(self, description):
        self._pipeline.description = description
        self._pipeline.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def user_id(self):
        si.session.commit()
        return self._pipeline.user_id

    @property
    def user_name(self):
        si.session.commit()
        return self._pipeline.user.name

    @property
    def user(self):
        if hasattr(self._pipeline.user, '_wpipe_object'):
            return self._pipeline.user._wpipe_object
        else:
            from .User import SQLUser
            return SQLUser(self._pipeline.user)

    @property
    def targets(self):
        return self._targets_proxy

    @property
    def tasks(self):
        return self._tasks_proxy

    @property
    def dummy_task(self):
        return self._dummy_task

    @property
    def dummy_job(self):
        return self._dummy_job

    def target(self, *args, **kwargs):
        from .Target import SQLTarget
        return SQLTarget(self, *args, **kwargs)

    def task(self, *args, **kwargs):
        from .Task import SQLTask
        return SQLTask(self, *args, **kwargs)

    def to_json(self, *args, **kwargs):
        si.session.commit()
        return pd.DataFrame(dict((('' if attr != 'id' else 'pipeline_') + attr, getattr(self._pipeline, attr))
                                 for attr in dir(self._pipeline) if attr[0] != '_'
                                 and type(getattr(self._pipeline, attr)).__module__.split('.')[0]
                                 not in ['sqlalchemy', 'wpipe']),
                            index=[0]).to_json(*args, **kwargs)

    def attach_tasks(self, tasks_path):
        tasks_path = clean_path(tasks_path)
        if tasks_path is not None:
            for task_path in os.listdir(tasks_path):
                shutil.copy2(tasks_path+'/'+task_path, self.software_root)
        _temp = [self.task(task_path).register()
                 for task_path in os.listdir(self.software_root)
                 if os.path.isfile(self.software_root+'/'+task_path)
                 and os.access(self.software_root+'/'+task_path, os.X_OK)]

    def attach_targets(self, data_dir, config_file=None):
        data_dir = clean_path(data_dir)
        if data_dir is not None:
            for target_file in os.listdir(data_dir):
                shutil.copy2(data_dir+'/'+target_file, self.data_root)
        _temp = [self.target(target_file).configure_target(config_file)
                 for target_file in os.listdir(self.data_root)
                 if os.path.isfile(self.data_root+'/'+target_file)
                 and os.access(self.data_root+'/'+target_file, os.R_OK)]

    def run_pipeline(self):
        self.dummy_job.child_event('__init__').fire()
