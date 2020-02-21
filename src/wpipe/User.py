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
    def __init__(self, name):
        try:
            self._user = si.session.query(si.User).\
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            self._user = si.User(name=name)
            si.session.add(self._user)
        self._user.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        return self._user.name

    @name.setter
    def name(self, name):
        self._user.name = name
        self._user.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def user_id(self):
        return self._user.id

    @property
    def timestamp(self):
        return self._user.timestamp

    @property
    def pipelines(self):
        return list(map(lambda pipeline: pipeline.name, self._user.pipelines))
