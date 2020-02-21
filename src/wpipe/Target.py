from .core import *
from .Store import Store
from .Pipeline import Pipeline, SQLPipeline


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


class SQLTarget:
    def __init__(self, pipeline, name):
        try:
            self._target = si.session.query(si.Target). \
                filter_by(pipeline_id=pipeline.pipeline_id). \
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            self._target = si.Target(name=name,
                                     relativepath=pipeline.data_root+'/'+name)
            pipeline._pipeline.targets.append(self._target)
            # _opt = Options(options).create('target', int(_df.target_id), store=store)
            if not os.path.isdir(self._target.relativepath):
                os.mkdir(self._target.relativepath)
        self._target.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        return self._target.name

    @name.setter
    def name(self, name):
        self._target.name = name
        self._target.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def target_id(self):
        return self._target.id

    @property
    def timestamp(self):
        return self._target.timestamp

    @property
    def relativepath(self):
        return self._target.relativepath

    @property
    def pipeline_id(self):
        return self._target.pipeline_id

    @property
    def configurations(self):
        return list(map(lambda configuration: configuration.name, self._target.configurations))
