from .core import *


class SQLParameter:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._parameter = args[0] if len(args) else None
        if not isinstance(cls._parameter, si.Parameter):
            id = kwargs.get('id', cls._parameter)
            if isinstance(id, int):
                cls._parameter = si.session.query(si.Parameter).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=2)
                config = kwargs.get('config', wpargs.get('Configuration', None))
                name = kwargs.get('name', args[0])
                value = kwargs.get('value', args[1])
                # querying the database for existing row or create
                try:
                    cls._parameter = si.session.query(si.Parameter). \
                        filter_by(config_id=config.config_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._parameter = si.Parameter(name=name,
                                                  value=str(value))
                    config._configuration.parameters.append(cls._parameter)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Parameter')
        return cls._inst

    def __init__(self, config, name, value):
        self._parameter.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Parameter).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        return self.config

    @property
    def name(self):
        si.session.commit()
        return self._parameter.name

    @name.setter
    def name(self, name):
        self._parameter.name = name
        self._parameter.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def parameter_id(self):
        si.session.commit()
        return self._parameter.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._parameter.timestamp

    @property
    def value(self):
        si.session.commit()
        return self._parameter.value

    @value.setter
    def value(self, value):
        self._parameter.value = value
        self._parameter.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def config_id(self):
        si.session.commit()
        return self._parameter.config_id

    @property
    def config(self):
        if hasattr(self._parameter.config, '_wpipe_object'):
            return self._parameter.config._wpipe_object
        else:
            from .Configuration import SQLConfiguration
            return SQLConfiguration(self._parameter.config)
