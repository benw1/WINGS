from .core import *
from .Store import Store

class Node():
    def __init__(self, name='any', int_ip='', ext_ip=''):
        self.name = np.array([str(name)])
        self.node_id = np.array([int(0)])
        self.int_ip = np.array([str(int_ip)])
        self.ext_ip = np.array([str(ext_ip)])
        self.timestamp = pd.to_datetime(time.time(), unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.node_id
        return update_time(_df)

    def create(self, store=Store()):
        return store.create('nodes', 'node_id', self)

    def get(node_id, store=Store()):
        return store.select('nodes').loc[int(node_id)]