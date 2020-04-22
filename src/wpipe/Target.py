from .core import *
from .OptOwner import OptOwner


class Target(OptOwner):
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._target = args[0] if len(args) else None
        if not isinstance(cls._target, si.Target):
            keyid = kwargs.get('id', cls._target)
            if isinstance(keyid, int):
                cls._target = si.session.query(si.Target).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=1)
                input = kwargs.get('input', wpargs.get('Input', None))
                name = kwargs.get('name', input.name if args[0] is None else args[0])
                # querying the database for existing row or create
                try:
                    cls._target = si.session.query(si.Target). \
                        filter_by(input_id=input.input_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._target = si.Target(name=name,
                                            datapath=input.pipeline.data_root+'/'+name,
                                            dataraws=input.rawspath)
                    input._input.targets.append(cls._target)
                    if not os.path.isdir(cls._target.datapath):
                        os.mkdir(cls._target.datapath)
                    if not os.path.isdir(cls._target.dataraws):
                        os.mkdir(cls._target.dataraws)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Target')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_configurations_proxy'):
            self._configurations_proxy = ChildrenProxy(self._target, 'configurations', 'Configuration')
        if not hasattr(self, '_optowner'):
            self._optowner = self._target
        if not hasattr(self, '_default_conf'):
            self._default_conf = self.configuration('default')
        self.configure_target()
        super(Target, self).__init__(kwargs.get('options', {}))

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Target).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        return self.input

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
    def input_id(self):
        si.session.commit()
        return self._target.input_id

    @property
    def input(self):
        if hasattr(self._target.input, '_wpipe_object'):
            return self._target.input._wpipe_object
        else:
            from .Input import Input
            return Input(self._target.input)

    @property
    def pipeline_id(self):
        return self.input.pipeline_id

    @property
    def pipeline(self):
        return self.input.pipeline

    @property
    def configurations(self):
        return self._configurations_proxy

    @property
    def default_conf(self):
        return self._default_conf

    def configuration(self, *args, **kwargs):
        from .Configuration import Configuration
        return Configuration(self, *args, **kwargs)

    def configure_target(self):
        for confdp in self.input.confdataproducts:
            self.configuration(os.path.splitext(confdp.filename)[0],
                               parameters=json.load(open(confdp.relativepath+'/'+confdp.filename))[0])
