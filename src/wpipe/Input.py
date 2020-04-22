from .core import *
from .DPOwner import DPOwner


class Input(DPOwner):
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._input = args[0] if len(args) else None
        if not isinstance(cls._input, si.Input):
            keyid = kwargs.get('id', cls._input)
            if isinstance(keyid, int):
                cls._input = si.session.query(si.Input).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=1)
                pipeline = kwargs.get('pipeline', wpargs.get('Pipeline', None))
                base, name = os.path.split(clean_path(kwargs.get('path', args[0])))
                # querying the database for existing row or create
                try:
                    cls._input = si.session.query(si.Input). \
                        filter_by(pipeline_id=pipeline.pipeline_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._input = si.Input(name=name,
                                          rawspath=pipeline.input_root+'/'+name,
                                          confpath=pipeline.config_root+'/'+name)
                    pipeline._pipeline.inputs.append(cls._input)
                    if not os.path.isdir(cls._input.rawspath):
                        os.mkdir(cls._input.rawspath)
                    cls._copy_data(base+'/'+name)
                    if not os.path.isdir(cls._input.confpath):
                        os.mkdir(cls._input.confpath)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Input')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_targets_proxy'):
            self._targets_proxy = ChildrenProxy(self._input, 'targets', 'Target')
        if not hasattr(self, '_dpowner'):
            self._dpowner = self._input
        super(Input, self).__init__()
        self._verify_raws()

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Input).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @classmethod
    def _copy_data(cls, path):
        if hasattr(cls, '_input'):
            if hasattr(cls._input, 'rawspath'):
                shutil.copy2(path, cls._input.rawspath + '/')

    def _verify_raws(self):
        for filename in glob.glob(self.rawspath+'/*'):
            if os.path.splitext(filename)[-1] == '.conf':
                self.make_config(filename)
                os.remove(filename)
            else:
                base, name = os.path.split(filename)
                self.dataproduct(filename=name, relativepath=base, group='raw')

    @property
    def parents(self):
        return self.pipeline

    @property
    def name(self):
        si.session.commit()
        return self._input.name

    @name.setter
    def name(self, name):
        self._input.name = name
        self._input.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def input_id(self):
        si.session.commit()
        return self._input.id

    @property
    def rawspath(self):
        si.session.commit()
        return self._input.rawspath

    @property
    def confpath(self):
        si.session.commit()
        return self._input.confpath

    @property
    def pipeline_id(self):
        si.session.commit()
        return self._input.pipeline_id

    @property
    def pipeline(self):
        if hasattr(self._input.pipeline, '_wpipe_object'):
            return self._input.pipeline._wpipe_object
        else:
            from .Pipeline import Pipeline
            return Pipeline(self._input.pipeline)

    @property
    def targets(self):
        return self._targets_proxy

    def target(self, *args, **kwargs):
        from .Target import Target
        return Target(self, *args, **kwargs)

    def make_config(self, config_file):
        config_file = clean_path(config_file)
        if config_file is not None:
            shutil.copy2(config_file, self.confpath)
            self.dataproduct(filename=os.path.split(config_file)[-1], relativepath=self.confpath, group='conf')
