from .core import *
from .Store import Store
from .Target import Target, SQLTarget


class Configuration:
    def __init__(self, name='', description='',
                 target=Target().new()):
        self.name = np.array([str(name)])
        self.relativepath = np.array([str(target.relativepath[0])])
        self.logpath = np.array([str(target.relativepath[0]) + '/log_' + str(name)])
        self.confpath = np.array([str(target.relativepath[0]) + '/conf_' + str(name)])
        self.rawpath = np.array([str(target.relativepath[0]) + '/raw_' + str(name)])
        self.procpath = np.array([str(target.relativepath[0]) + '/proc_' + str(name)])
        self.target_id = np.array([int(target.target_id)])
        self.pipeline_id = np.array([int(target.pipeline_id)])
        self.config_id = np.array([int(0)])
        self.description = np.array([str(description)])
        self.timestamp = pd.to_datetime(time.time(), unit='s')

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                                                      np.array([int(self.target_id)]), np.array([int(self.config_id)])],
                                              names=('pipelineID', 'targetID', 'configID'))
        return update_time(_df)

    def create(self, params={'any': 0}, create_dir=False, ret_opt=False, store=Store()):
        from . import Parameters
        _df = store.create('configurations', 'config_id', self)
        _params = Parameters(params).create(_df, store=store)

        if create_dir:
            for _path in [self.rawpath[0], self.confpath[0], self.procpath[0], self.logpath[0]]:
                _t = subprocess.run(['mkdir', '-p', str(_path)], stdout=subprocess.PIPE)

        if ret_opt:
            return _df, _params
        else:
            return _df

    def get(config_id, store=Store()):
        x = store.select('configurations', 'config_id==' + str(config_id))
        return x.loc[x.index.values[0]]


class SQLConfiguration:
    def __init__(self, target, name='', description=''):
        try:
            self._config = si.session.query(si.Target). \
                filter_by(target_id=target.target_id). \
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            self._config = si.Configuration(name=name,
                                            relativepath=target.relativepath,
                                            logpath=target.relativepath+'/log_'+name,
                                            confpath=target.relativepath+'/conf_'+name,
                                            rawpath=target.relativepath+'/raw_'+name,
                                            procpath=target.relativepath+'/proc_'+name,
                                            description=description)
            target._config.configurations.append(self._config)
            # _params = Parameters(params).create(_df, store=store)
            if not os.path.isdir(self._config.logpath):
                os.mkdir(self._config.logpath)
            if not os.path.isdir(self._config.confpath):
                os.mkdir(self._config.confpath)
            if not os.path.isdir(self._config.rawpath):
                os.mkdir(self._config.rawpath)
            if not os.path.isdir(self._config.procpath):
                os.mkdir(self._config.procpath)
        self._config.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        return self._config.name

    @name.setter
    def name(self, name):
        self._config.name = name
        self._config.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def config_id(self):
        return self._config.id

    @property
    def timestamp(self):
        return self._config.timestamp

    @property
    def relativepath(self):
        return self._config.relativepath

    @property
    def logpath(self):
        return self._config.logpath

    @property
    def confpath(self):
        return self._config.confpath

    @property
    def rawpath(self):
        return self._config.rawpath

    @property
    def procpath(self):
        return self._config.procpath

    @property
    def description(self):
        return self._config.description

    @description.setter
    def description(self, description):
        self._config.description = description
        self._config.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def target_id(self):
        return self._config.target_id

    @property
    def pipeline_id(self):
        return self._config._target.pipeline_id

    @property
    def jobs(self):
        return list(map(lambda job: job.name, self._config.jobs))
