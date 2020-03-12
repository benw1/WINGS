from .core import *
from .Store import Store
from .Pipeline import Pipeline
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
        # checking if given argument is sqlintf object or existing id
        cls._target = args[0] if len(args) else None
        if not isinstance(cls._target, si.Target):
            id = kwargs.get('id', cls._target)
            if isinstance(id, int):
                cls._target = si.session.query(si.Target).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=1)
                pipeline = kwargs.get('pipeline', wpargs.get('Pipeline', None))
                name = kwargs.get('name', args[0])
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
        wpipe_to_sqlintf_connection(cls, 'Target')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_configurations_proxy'):
            self._configurations_proxy = ChildrenProxy(self._target, 'configurations', 'Configuration')
        if not hasattr(self, '_owner'):
            self._owner = self._target
        super(SQLTarget, self).__init__(kwargs.get('options', {}))

    @property
    def parents(self):
        return self.pipeline

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
    def pipeline(self):
        if hasattr(self._target.pipeline, '_wpipe_object'):
            return self._target.pipeline._wpipe_object
        else:
            from .Pipeline import SQLPipeline
            return SQLPipeline(self._target.pipeline)

    @property
    def configurations(self):
        return self._configurations_proxy

    def configuration(self, *args, **kwargs):
        from .Configuration import SQLConfiguration
        return SQLConfiguration(self, *args, **kwargs)
