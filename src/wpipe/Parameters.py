from .core import *
from .Store import Store
from .Configuration import Configuration, SQLConfiguration


# What is this for?
class Parameters:
    def __init__(self, params={'any': 0}):
        self.__dict__ = params

    def new(self, config=Configuration().new()):
        name = np.array(list(self.__dict__.keys()))
        value = np.array(list(self.__dict__.values()))
        _df = pd.DataFrame(data=np.array([name, value]).T,
                           columns=['name', 'value']).sort_values('name')

        _config_id = np.repeat(int(config.config_id), len(name))
        _target_id = np.repeat(int(config.target_id), len(name))
        _pipeline_id = np.repeat(int(config.pipeline_id), len(name))
        arrays = [_pipeline_id, _target_id, _config_id]
        _df.index = pd.MultiIndex.from_arrays(arrays,
                                              names=('pipelineID', 'targetID', 'configID'))
        return _df

    def create(self, config=Configuration().new(), store=Store()):
        _df = self.new(config)
        with pd.HDFStore(str(store.path), 'r+') as myStore:
            myStore.append('parameters', _df, min_itemsize=fmin_itemsize(_df))
        return _df

    def getParam(config_id=0, store=Store()):
        config_id = int(config_id)
        config = Configuration.get(int(config_id))
        target_id = int(config.target_id)
        pipeline_id = int(config.pipeline_id)
        x = store.select('parameters').loc[pipeline_id, target_id, config_id]
        if x.shape == (2,):
            return dict(zip([x['name']], [x['value']]))
        else:
            return dict(zip(x['name'].values, x['value'].values))

    def addParam(config_id, key, value, store=Store()):
        config_id = int(config_id)
        _config = Configuration.get(config_id)
        _params = Parameters.getParam(config_id)
        _params[key] = value
        return store.update('parameters', Parameters(_params).new(_config))


class SQLParameter:
    def __init__(self, config, name, value):
        try:
            self._parameter = si.session.query(si.Parameter). \
                filter_by(config_id=config.config_id). \
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            self._parameter = si.Parameter(name=name,
                                           value=str(value))
            config._config.parameters.append(self._parameter)
        self._parameter.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        return self._parameter.name

    @name.setter
    def name(self, name):
        self._parameter.name = name
        self._parameter.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def parameter_id(self):
        return self._parameter.id

    @property
    def timestamp(self):
        return self._parameter.timestamp

    @property
    def value(self):
        return self._parameter.value

    @value.setter
    def value(self, value):
        self._parameter.value = value
        self._parameter.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def config_id(self):
        return self._parameter.config_id
