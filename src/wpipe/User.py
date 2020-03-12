from .core import *
from .Store import Store


class User:
    def __init__(self, name='any'):
        self.name = np.array([str(name)], dtype='<U20')
        self.user_id = np.array([int(0)])
        self.timestamp = pd.to_datetime(time.time(), unit='s')

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.user_id
        return update_time(_df)

    def create(self, store=Store()):
        return store.create('users', 'user_id', self)

    def get(user_name, store=Store()):
        x = store.select('users', 'name=="' + str(user_name) + '"')
        return x.loc[x.index.values[0]]


class SQLUser:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._user = args[0] if len(args) else None
        if not isinstance(cls._user, si.User):
            id = kwargs.get('id', cls._user)
            if isinstance(id, int):
                cls._user = si.session.query(si.User).filter_by(id=id).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=1)
                name = kwargs.get('name', PARSER.parse_known_args()[0].user_name if args[0] is None else args[0])
                # querying the database for existing row or create
                try:
                    cls._user = si.session.query(si.User). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._user = si.User(name=name)
                    si.session.add(cls._user)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'User')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_pipelines_proxy'):
            self._pipelines_proxy = ChildrenProxy(self._user, 'pipelines', 'Pipeline')
        self._user.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def parents(self):
        return

    @property
    def name(self):
        si.session.commit()
        return self._user.name

    @name.setter
    def name(self, name):
        self._user.name = name
        self._user.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def user_id(self):
        si.session.commit()
        return self._user.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._user.timestamp

    @property
    def pipelines(self):
        return self._pipelines_proxy

    def pipeline(self, *args, **kwargs):
        from .Pipeline import SQLPipeline
        return SQLPipeline(self, *args, **kwargs)