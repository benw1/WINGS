from .core import *
from .Store import Store

class User():
    def __init__(self,name='any'):
        self.name = np.array([str(name)],dtype='<U20')
        self.user_id = np.array([int(0)])
        self.timestamp = pd.to_datetime(time.time(),unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.user_id
        return update_time(_df)

    def create(self, store=Store()):
        return store.create('users','user_id',self)

    def get(user_name, store=Store()):
        x = store.select('users','name=="'+str(user_name)+'"')
        return x.loc[x.index.values[0]]