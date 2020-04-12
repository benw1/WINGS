from .core import *
from .Store import Store
from .Target import Target


# from .Parameters import SQLParameter # this will work if only
#                                        importing working with SQL,
#                                        right now it can't work
#                                        due to the existing class


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
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._configuration = args[0] if len(args) else None
        if not isinstance(cls._configuration, si.Configuration):
            id = kwargs.get('id', cls._configuration)
            if isinstance(id, int):
                cls._configuration = si.session.query(si.Configuration).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=2)
                target = kwargs.get('target', wpargs.get('Target', None))
                name = kwargs.get('name', args[0])
                description = kwargs.get('description', '' if args[1] is None else args[1])
                # querying the database for existing row or create
                try:
                    cls._configuration = si.session.query(si.Configuration). \
                        filter_by(target_id=target.target_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._configuration = si.Configuration(name=name,
                                                          datapath=target.datapath,
                                                          logpath=target.datapath + '/log_' + name,
                                                          confpath=target.datapath + '/conf_' + name,
                                                          rawpath=target.dataraws,
                                                          procpath=target.datapath + '/proc_' + name,
                                                          description=description)
                    target._target.configurations.append(cls._configuration)
                    if not os.path.isdir(cls._configuration.logpath):
                        os.mkdir(cls._configuration.logpath)
                    if not os.path.isdir(cls._configuration.confpath):
                        os.mkdir(cls._configuration.confpath)
                    if not os.path.isdir(cls._configuration.rawpath):
                        os.mkdir(cls._configuration.rawpath)
                    if not os.path.isdir(cls._configuration.procpath):
                        os.mkdir(cls._configuration.procpath)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Configuration')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_dataproducts_proxy'):
            self._dataproducts_proxy = ChildrenProxy(self._configuration, 'dataproducts', 'DataProduct',
                                                     child_attr='filename')
        if not hasattr(self, '_parameters_proxy'):
            self._parameters_proxy = DictLikeChildrenProxy(self._configuration, 'parameters', 'Parameter')
        self.parameters = kwargs.get('parameters', {})
        if not hasattr(self, '_jobs_proxy'):
            self._jobs_proxy = ChildrenProxy(self._configuration, 'jobs', 'Job',
                                             child_attr='id')
        if not hasattr(self, '_raw_dp'):
            self._raw_dp = self.dataproduct(filename=self._configuration.target.name, relativepath=self.rawpath,
                                            group='raw')
        self._configuration.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Configuration).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        return self.target

    @property
    def name(self):
        si.session.commit()
        return self._configuration.name

    @name.setter
    def name(self, name):
        self._configuration.name = name
        self._configuration.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def config_id(self):
        si.session.commit()
        return self._configuration.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._configuration.timestamp

    @property
    def datapath(self):
        si.session.commit()
        return self._configuration.datapath

    @property
    def logpath(self):
        si.session.commit()
        return self._configuration.logpath

    @property
    def confpath(self):
        si.session.commit()
        return self._configuration.confpath

    @property
    def rawpath(self):
        si.session.commit()
        return self._configuration.rawpath

    @property
    def procpath(self):
        si.session.commit()
        return self._configuration.procpath

    @property
    def description(self):
        si.session.commit()
        return self._configuration.description

    @description.setter
    def description(self, description):
        self._configuration.description = description
        self._configuration.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def target_id(self):
        si.session.commit()
        return self._configuration.target_id

    @property
    def target(self):
        if hasattr(self._configuration.target, '_wpipe_object'):
            return self._configuration.target._wpipe_object
        else:
            from .Target import SQLTarget
            return SQLTarget(self._configuration.target)

    @property
    def pipeline_id(self):
        return self.target.pipeline_id

    @property
    def pipeline(self):
        return self.target.pipeline

    @property
    def dummy_task(self):
        return self.pipeline.dummy_task

    @property
    def parameters(self):
        return self._parameters_proxy

    @parameters.setter
    def parameters(self, parameters):
        for key, value in parameters.items():
            self.parameter(name=key, value=value)

    @property
    def dataproducts(self):
        return self._dataproducts_proxy

    @property
    def raw_dataproduct(self):
        return self._raw_dp

    @property
    def jobs(self):
        return self._jobs_proxy

    def parameter(self, *args, **kwargs):
        from .Parameters import SQLParameter
        return SQLParameter(self, *args, **kwargs)

    def dataproduct(self, *args, **kwargs):
        from .DataProduct import SQLDataProduct
        return SQLDataProduct(self, *args, **kwargs)

    def job(self, *args, **kwargs):
        from .Job import SQLJob
        return SQLJob(self, *args, **kwargs)
