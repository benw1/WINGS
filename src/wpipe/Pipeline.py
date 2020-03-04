from .core import *
from .Store import Store
from .User import User, SQLUser


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
        # checking if given argument is sqlintf object
        cls._pipeline = args[0] if len(args) else None
        if not isinstance(cls._pipeline, si.Pipeline):
            # gathering construction arguments
            user = kwargs.get('user', args[0] if len(args) else None)
            name = kwargs.get('name', args[1] if len(args) > 1 else None)
            software_root = kwargs.get('software_root', 'build')
            data_root = kwargs.get('data_root', 'data')
            pipe_root = kwargs.get('pipe_root', '')
            config_root = kwargs.get('config_root', 'config')
            description = kwargs.get('description', '')
            # querying the database for existing row or create
            try:
                cls._pipeline = si.session.query(si.Pipeline). \
                    filter_by(user_id=user.user_id). \
                    filter_by(name=name).one()
            except si.orm.exc.NoResultFound:
                temp = []
                for path in [pipe_root, software_root, data_root, config_root]:
                    if path[:1] != '/' or path == '':
                        path = os.getcwd() + ['', '/'][bool(path)] + path
                    temp.append(path)
                pipe_root, software_root, data_root, config_root = temp
                cls._pipeline = si.Pipeline(name=name,
                                            software_root=software_root,
                                            data_root=data_root,
                                            pipe_root=pipe_root,
                                            config_root=config_root,
                                            description=description)
                user._user.pipelines.append(cls._pipeline)
                if not os.path.isdir(cls._pipeline.software_root):
                    os.mkdir(cls._pipeline.software_root)
                if not os.path.isdir(cls._pipeline.data_root):
                    os.mkdir(cls._pipeline.data_root)
                if not os.path.isdir(cls._pipeline.config_root):
                    os.mkdir(cls._pipeline.config_root)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Pipeline', __name__)
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_targets_proxy'):
            self._targets_proxy = ChildrenProxy(self._pipeline, 'targets', 'Target', __name__)
        if not hasattr(self, '_tasks_proxy'):
            self._tasks_proxy = ChildrenProxy(self._pipeline, 'tasks', 'Task', __name__)
        if not hasattr(self, '_dummy_task'):
            from .Task import SQLTask
            self._dummy_task = SQLTask(self, 'dummy')
        self._pipeline.timestamp = datetime.datetime.utcnow()
        si.session.commit()

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
    def software_root(self):
        si.session.commit()
        return self._pipeline.software_root

    @property
    def data_root(self):
        si.session.commit()
        return self._pipeline.data_root

    @property
    def pipe_root(self):
        si.session.commit()
        return self._pipeline.pipe_root

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
    def targets(self):
        return self._targets_proxy

    @property
    def tasks(self):
        return self._tasks_proxy

    @property
    def dummy_task(self):
        return self._dummy_task
