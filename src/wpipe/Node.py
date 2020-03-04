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
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object
        cls._node = args[0] if len(args) else None
        if not isinstance(cls._node, si.Node):
            # gathering construction arguments
            name = kwargs.get('name', args[0] if len(args) else None)
            int_ip = kwargs.get('int_ip', '')
            ext_ip = kwargs.get('ext_ip', '')
            # querying the database for existing row or create
            try:
                cls._node = si.session.query(si.Node). \
                    filter_by(name=name).one()
            except si.orm.exc.NoResultFound:
                cls._node = si.Node(name=name,
                                    int_ip=int_ip,
                                    ext_ip=ext_ip)
                si.session.add(cls._node)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Node', __name__)
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_jobs_proxy'):
            self._jobs_proxy = ChildrenProxy(self._node, 'jobs', 'Job', __name__,
                                             child_attr='id')
        self._node.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def name(self):
        si.session.commit()
        return self._node.name

    @name.setter
    def name(self, name):
        self._node.name = name
        self._node.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def node_id(self):
        si.session.commit()
        return self._node.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._node.timestamp

    @property
    def int_ip(self):
        si.session.commit()
        return self._node.int_ip

    @property
    def ext_ip(self):
        si.session.commit()
        return self._node.ext_ip

    @property
    def jobs(self):
        return self._jobs_proxy
