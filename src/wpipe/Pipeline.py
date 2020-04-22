from .core import *


class Pipeline:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._pipeline = args[0] if len(args) else as_int(PARSER.parse_known_args()[0].pipeline)
        if not isinstance(cls._pipeline, si.Pipeline):
            keyid = kwargs.get('id', cls._pipeline)
            if isinstance(keyid, int):
                cls._pipeline = si.session.query(si.Pipeline).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=8)
                from . import DefaultUser
                user = kwargs.get('user', wpargs.get('User', DefaultUser))
                pipe_root = clean_path(kwargs.get('pipe_root',
                                                  PARSER.parse_known_args()[0].pipeline if args[0] is None
                                                  else args[0]))
                name = kwargs.get('name', os.path.basename(pipe_root) if args[1] is None else args[1])
                software_root = clean_path(kwargs.get('software_root',
                                                      'build' if args[2] is None else args[2]),
                                           root=pipe_root)
                input_root = clean_path(kwargs.get('input_root',
                                                   'input' if args[3] is None else args[3]),
                                        root=pipe_root)
                data_root = clean_path(kwargs.get('data_root',
                                                  'data' if args[4] is None else args[4]),
                                       root=pipe_root)
                config_root = clean_path(kwargs.get('config_root',
                                                    'config' if args[5] is None else args[5]),
                                         root=pipe_root)
                description = kwargs.get('description',
                                         '' if args[6] is None else args[6])
                # querying the database for existing row or create
                try:
                    cls._pipeline = si.session.query(si.Pipeline). \
                        filter_by(user_id=user.user_id). \
                        filter_by(software_root=software_root).one()
                except si.orm.exc.NoResultFound:
                    cls._pipeline = si.Pipeline(name=name,
                                                pipe_root=pipe_root,
                                                software_root=software_root,
                                                input_root=input_root,
                                                data_root=data_root,
                                                config_root=config_root,
                                                description=description)
                    user._user.pipelines.append(cls._pipeline)
                    if not os.path.isdir(cls._pipeline.software_root):
                        os.mkdir(cls._pipeline.software_root)
                    if not os.path.isfile(cls._pipeline.software_root+'/__init__.py'):
                        with open(cls._pipeline.software_root+'/__init__.py', 'w') as file:
                            file.write("def register(task):\n    return")
                    if not os.path.isdir(cls._pipeline.input_root):
                        os.mkdir(cls._pipeline.input_root)
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
        if not hasattr(self, '_inputs_proxy'):
            self._inputs_proxy = ChildrenProxy(self._pipeline, 'inputs', 'Input')
        if not hasattr(self, '_tasks_proxy'):
            self._tasks_proxy = ChildrenProxy(self._pipeline, 'tasks', 'Task')
        if not hasattr(self, '_dummy_task'):
            self._dummy_task = self.task(self.software_root+'/__init__.py')
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
    def input_root(self):
        si.session.commit()
        return self._pipeline.input_root

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
            from .User import User
            return User(self._pipeline.user)

    @property
    def inputs(self):
        return self._inputs_proxy

    @property
    def tasks(self):
        return self._tasks_proxy

    @property
    def dummy_task(self):
        return self._dummy_task

    @property
    def dummy_job(self):
        return self._dummy_job

    def input(self, *args, **kwargs):
        from .Input import Input
        return Input(self, *args, **kwargs)

    def task(self, *args, **kwargs):
        from .Task import Task
        return Task(self, *args, **kwargs)

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
            for task_path in glob.glob(tasks_path+'/*'):
                if os.path.isfile(task_path) and os.access(task_path, os.X_OK):
                    self.task(task_path).register()

    def attach_inputs(self, inputs_path, config_file=None):
        inputs_path = clean_path(inputs_path)
        if inputs_path is not None:
            for input_path in glob.glob(inputs_path+'/*'):
                if os.access(inputs_path, os.R_OK):
                    self.input(input_path).make_config(config_file)

    def run_pipeline(self):
        self.dummy_job.child_event('__init__').fire()
