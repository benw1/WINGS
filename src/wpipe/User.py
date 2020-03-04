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
        # checking if given argument is sqlintf object
        cls._user = args[0] if len(args) else None
        if not isinstance(cls._user, si.User):
            # gathering construction arguments
            name = kwargs.get('name', args[0] if len(args) else None)
            # querying the database for existing row or create
            try:
                cls._user = si.session.query(si.User). \
                    filter_by(name=name).one()
            except si.orm.exc.NoResultFound:
                cls._user = si.User(name=name)
                si.session.add(cls._user)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'User', __name__)
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_pipelines_proxy'):
            self._pipelines_proxy = ChildrenProxy(self._user, 'pipelines', 'Pipeline', __name__)
        self._user.timestamp = datetime.datetime.utcnow()
        si.session.commit()

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
