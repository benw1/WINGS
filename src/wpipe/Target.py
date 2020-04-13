from .core import *
from .Owner import SQLOwner


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
                    filebase, fileext = os.path.splitext(name)
                    cls._target = si.Target(name=name,
                                            datapath=pipeline.data_root+'/'+filebase+'_data',
                                            dataraws=pipeline.data_root+'/'+filebase+'_data/raw')
                    pipeline._pipeline.targets.append(cls._target)
                    if not os.path.isdir(cls._target.datapath):
                        os.mkdir(cls._target.datapath)
                    if not os.path.isdir(cls._target.dataraws):
                        os.mkdir(cls._target.dataraws)
                shutil.move(pipeline.data_root+'/'+name, cls._target.dataraws+'/'+name)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Target')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_configurations_proxy'):
            self._configurations_proxy = ChildrenProxy(self._target, 'configurations', 'Configuration')
        if not hasattr(self, '_owner'):
            self._owner = self._target
        if not hasattr(self, '_default_conf'):
            self._default_conf = self.configuration('default')
        super(SQLTarget, self).__init__(kwargs.get('options', {}))

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Target).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

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
    def datapath(self):
        si.session.commit()
        return self._target.datapath

    @property
    def dataraws(self):
        si.session.commit()
        return self._target.dataraws

    @property
    def configpath(self):
        si.session.commit()
        return self._target.configpath

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

    @property
    def default_conf(self):
        return self._default_conf

    def configuration(self, *args, **kwargs):
        from .Configuration import SQLConfiguration
        return SQLConfiguration(self, *args, **kwargs)

    def configure_target(self, config_file, default=True):
        if config_file is not None:
            conf_filename = config_file.split('/')[-1]
            if default:
                config = self.default_conf
            else:
                config = self.configuration(conf_filename.split('.')[0])
            shutil.copy2(config_file, config.confpath)
            config.parameters = json.load(open(config.confpath+'/'+conf_filename))[0]
            _dp = config.dataproduct(filename=conf_filename, relativepath=config.confpath, group='conf')
