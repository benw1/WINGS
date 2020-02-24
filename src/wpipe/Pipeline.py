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
    def __init__(self, user, name, software_root='build',
                 data_root='data', pipe_root='',
                 config_root='config', description=''):
        try:
            self._pipeline = si.session.query(si.Pipeline). \
                filter_by(user_id=user.user_id). \
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            self._pipeline = si.Pipeline(name=name,
                                         software_root=software_root,
                                         data_root=data_root,
                                         pipe_root=pipe_root,
                                         config_root=config_root,
                                         description=description)
            user._user.pipelines.append(self._pipeline)
            if not os.path.isdir(self._pipeline.software_root):
                os.mkdir(self._pipeline.software_root)
            if not os.path.isdir(self._pipeline.data_root):
                os.mkdir(self._pipeline.data_root)
            if not os.path.isdir(self._pipeline.config_root):
                os.mkdir(self._pipeline.config_root)
        self._pipeline.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        return self._pipeline.name

    @name.setter
    def name(self, name):
        self._pipeline.name = name
        self._pipeline.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def pipeline_id(self):
        return self._pipeline.id

    @property
    def timestamp(self):
        return self._pipeline.timestamp

    @property
    def software_root(self):
        return self._pipeline.software_root

    @property
    def data_root(self):
        return self._pipeline.data_root

    @property
    def pipe_root(self):
        return self._pipeline.pipe_root

    @property
    def config_root(self):
        return self._pipeline.config_root

    @property
    def description(self):
        return self._pipeline.description

    @description.setter
    def description(self, description):
        self._pipeline.description = description
        self._pipeline.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def user_id(self):
        return self._pipeline.user_id

    @property
    def user_name(self):
        return self._pipeline.user.name

    @property
    def targets(self):
        return list(map(lambda target: target.name, self._pipeline.targets))

    @property
    def tasks(self):
        return list(map(lambda task: task.name, self._pipeline.tasks))
