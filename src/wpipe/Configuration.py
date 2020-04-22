from .core import *
from .DPOwner import DPOwner


class Configuration(DPOwner):
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._configuration = args[0] if len(args) else None
        if not isinstance(cls._configuration, si.Configuration):
            keyid = kwargs.get('id', cls._configuration)
            if isinstance(keyid, int):
                cls._configuration = si.session.query(si.Configuration).filter_by(id=keyid).one()
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
                                                          confpath=target.datapath + '/conf_' + name,
                                                          rawpath=target.datapath + '/raw_' + name,
                                                          logpath=target.datapath + '/log_' + name,
                                                          procpath=target.datapath + '/proc_' + name,
                                                          description=description)
                    target._target.configurations.append(cls._configuration)
                    if not os.path.isdir(cls._configuration.confpath):
                        os.mkdir(cls._configuration.confpath)
                    confdp = target.input.dataproduct(filename=name+'.conf', group='conf')
                    confdp.symlink(cls._configuration.confpath, dpowner=cls._configuration, group='conf')
                    if not os.path.isdir(cls._configuration.rawpath):
                        os.mkdir(cls._configuration.rawpath)
                    for rawdp in target.input.rawdataproducts:
                        rawdp.symlink(cls._configuration.rawpath, dpowner=cls._configuration, group='raw')
                    if not os.path.isdir(cls._configuration.logpath):
                        os.mkdir(cls._configuration.logpath)
                    if not os.path.isdir(cls._configuration.procpath):
                        os.mkdir(cls._configuration.procpath)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Configuration')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_parameters_proxy'):
            self._parameters_proxy = DictLikeChildrenProxy(self._configuration, 'parameters', 'Parameter')
        self.parameters = kwargs.get('parameters', {})
        if not hasattr(self, '_jobs_proxy'):
            self._jobs_proxy = ChildrenProxy(self._configuration, 'jobs', 'Job',
                                             child_attr='id')
        if not hasattr(self, '_dpowner'):
            self._dpowner = self._configuration
        super(Configuration, self).__init__()

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
    def datapath(self):
        si.session.commit()
        return self._configuration.datapath

    @property
    def confpath(self):
        si.session.commit()
        return self._configuration.confpath

    @property
    def rawpath(self):
        si.session.commit()
        return self._configuration.rawpath

    @property
    def logpath(self):
        si.session.commit()
        return self._configuration.logpath

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
            from .Target import Target
            return Target(self._configuration.target)

    @property
    def input_id(self):
        return self.target.input_id

    @property
    def input(self):
        return self.target.input

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
    def jobs(self):
        return self._jobs_proxy

    def parameter(self, *args, **kwargs):
        from .Parameter import Parameter
        return Parameter(self, *args, **kwargs)

    def job(self, *args, **kwargs):
        from .Job import Job
        return Job(self, *args, **kwargs)
