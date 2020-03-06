from .core import *
from .Store import Store


# What is this for?
class Options:
    def __init__(self, opts={'any': 0}):
        self.__dict__ = opts

    def new(self, owner=str('any' + 250 * ' '), owner_id=0):
        name = np.array(list(self.__dict__.keys()))
        value = np.array(list(self.__dict__.values()))
        _df = pd.DataFrame(data=np.array([name, value]).T,
                           columns=['name', 'value']).sort_values('name')
        owner = np.repeat(str(owner), len(name))
        owner_id = np.repeat(int(owner_id), len(name))
        arrays = [owner, owner_id]
        _df.index = pd.MultiIndex.from_arrays(arrays, names=('owner', 'owner_id'))
        return _df

    def create(self, owner='any', owner_id=0, store=Store()):
        _df = self.new(owner, owner_id)
        with pd.HDFStore(str(store.path), 'r+') as myStore:
            myStore.append('options', _df, min_itemsize=fmin_itemsize(_df),
                           complevel=9, complib='blosc:blosclz')
        return _df

    def get(owner, owner_id, store=Store()):
        x = store.select('options').loc[str(owner)].loc[int(owner_id)]
        if x.shape == (2,):
            return dict(zip([x['name']], [x['value']]))
        else:
            return dict(zip(x['name'].values, x['value'].values))

    def addOption(owner, owner_id, key, value, store=Store()):
        _opt = Options.get(owner, int(owner_id))
        _opt[key] = value
        return store.update('options', Options(_opt).new(owner, int(owner_id)))


class SQLOption:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._option = args[0] if len(args) else None
        if not isinstance(cls._option, si.Option):
            id = kwargs.get('id', cls._option)
            if isinstance(id, int):
                cls._option = si.session.query(si.Option).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args = wpargs_from_args(*args)
                list(wpargs.__setitem__('Owner', wpargs[key]) for key in list(wpargs.keys())[::-1]
                     if (key in map(lambda obj: obj.__name__, si.Owner.__subclasses__())))
                owner = wpargs.get('Owner', kwargs.get('owner', None))
                name = args[0] if len(args) else kwargs.get('name', None)
                value = args[1] if len(args) > 1 else kwargs.get('value', None)
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
        wpipe_to_sqlintf_connection(cls, 'Option', __name__)
        return cls._inst

    def __init__(self, owner, name, value):
        self._option.timestamp = datetime.datetime.utcnow()
        si.session.commit()

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
