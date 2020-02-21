from .core import *
from .Store import Store


class Node:
    def __init__(self, name='any', int_ip='', ext_ip=''):
        self.name = np.array([str(name)])
        self.node_id = np.array([int(0)])
        self.int_ip = np.array([str(int_ip)])
        self.ext_ip = np.array([str(ext_ip)])
        self.timestamp = pd.to_datetime(time.time(), unit='s')

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.node_id
        return update_time(_df)

    def create(self, store=Store()):
        return store.create('nodes', 'node_id', self)

    def get(node_id, store=Store()):
        return store.select('nodes').loc[int(node_id)]


class SQLNode:
    def __init__(self, name, int_ip='', ext_ip=''):
        try:
            self._node = si.session.query(si.Node). \
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            self._node = si.Node(name=name,
                                 int_ip=int_ip,
                                 ext_ip=ext_ip)
            si.session.add(self._node)
        self._node.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        return self._node.name

    @name.setter
    def name(self, name):
        self._node.name = name
        self._node.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def node_id(self):
        return self._node.id

    @property
    def timestamp(self):
        return self._node.timestamp

    @property
    def int_ip(self):
        return self._node.int_ip

    @property
    def ext_ip(self):
        return self._node.ext_ip

    @property
    def jobs(self):
        return list(map(lambda job: job.name, self._node.jobs))
