from .core import *


class Option:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._option = args[0] if len(args) else None
        if not isinstance(cls._option, si.Option):
            keyid = kwargs.get('id', cls._option)
            if isinstance(keyid, int):
                cls._option = si.session.query(si.Option).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=2)
                list(wpargs.__setitem__('Owner', wpargs[key]) for key in list(wpargs.keys())[::-1]
                     if (key in map(lambda obj: obj.__name__, si.Owner.__subclasses__())))
                owner = kwargs.get('owner', wpargs.get('Owner', None))
                name = kwargs.get('name', args[0])
                value = kwargs.get('value', args[1])
                # querying the database for existing row or create
                try:
                    cls._option = si.session.query(si.Option). \
                        filter_by(owner_id=owner.owner_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._option = si.Option(name=name,
                                            value=str(value))
                    owner._owner.options.append(cls._option)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Option')
        return cls._inst

    def __init__(self, owner, name, value):
        self._option.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Option).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def name(self):
        si.session.commit()
        return self._option.name

    @name.setter
    def name(self, name):
        self._option.name = name
        self._option.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def option_id(self):
        si.session.commit()
        return self._option.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._option.timestamp

    @property
    def value(self):
        si.session.commit()
        return self._option.value

    @value.setter
    def value(self, value):
        self._option.value = value
        self._option.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def owner(self):
        si.session.commit()
        return self._option.type

    @property
    def owner_id(self):
        si.session.commit()
        return self._option.owner_id
