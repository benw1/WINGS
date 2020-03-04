from .core import *
from .Store import Store
from .Pipeline import Pipeline, SQLPipeline
from .Owner import SQLOwner


class Target:
    def __init__(self, name='any',
                 pipeline=Pipeline().new()):
        self.name = np.array([str(name)])
        self.pipeline_id = np.array([int(pipeline.pipeline_id)])
        self.target_id = np.array([int(0)])
        myPipe = Pipeline.get(self.pipeline_id)
        self.relativepath = np.array([str(myPipe.data_root) + '/' + str(self.name[0])])
        self.timestamp = pd.to_datetime(time.time(), unit='s')

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                                                      np.array([int(self.target_id)])],
                                              names=('pipelineID', 'targetID'))
        return update_time(_df)

    def create(self, options={'any': 0}, ret_opt=False, create_dir=False, store=Store()):
        from . import Options
        _df = store.create('targets', 'target_id', self)
        _opt = Options(options).create('target', int(_df.target_id), store=store)

        if create_dir:
            _t = subprocess.run(['mkdir', '-p', str(self.relativepath[0])], stdout=subprocess.PIPE)

        if ret_opt:
            return _df, _opt
        else:
            return _df

    def get(target_id, store=Store()):
        x = store.select('targets', 'target_id==' + str(target_id))
        return x.loc[x.index.values[0]]


class SQLTarget(SQLOwner):
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object
        cls._target = args[0] if len(args) else None
        if not isinstance(cls._target, si.Target):
            # gathering construction arguments
            pipeline = kwargs.get('pipeline', args[0] if len(args) else None)
            name = kwargs.get('name', args[1] if len(args) > 1 else None)
            # querying the database for existing row or create
            try:
                cls._target = si.session.query(si.Target). \
                    filter_by(pipeline_id=pipeline.pipeline_id). \
                    filter_by(name=name).one()
            except si.orm.exc.NoResultFound:
                cls._target = si.Target(name=name,
                                        relativepath=pipeline.data_root+'/'+name)
                pipeline._pipeline.targets.append(cls._target)
                if not os.path.isdir(cls._target.relativepath):
                    os.mkdir(cls._target.relativepath)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Target', __name__)
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_configurations_proxy'):
            self._configurations_proxy = ChildrenProxy(self._target, 'configurations', 'Configuration', __name__)
        if not hasattr(self, '_owner'):
            super().__init__()
            self._owner = self._target
        self.options = kwargs.get('options', {})
        self._target.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        si.session.commit()
        return self._target.name

    @name.setter
    def name(self, name):
        self._target.name = name
        self._target.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def target_id(self):
        si.session.commit()
        return self._target.id

    @property
    def relativepath(self):
        si.session.commit()
        return self._target.relativepath

    @property
    def pipeline_id(self):
        si.session.commit()
        return self._target.pipeline_id

    @property
    def configurations(self):
        return self._configurations_proxy
