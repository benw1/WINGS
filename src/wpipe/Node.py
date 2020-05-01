from .core import *
import socket


class Node:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._node = args[0] if len(args) else None
        if not isinstance(cls._node, si.Node):
            keyid = kwargs.get('id', cls._node)
            if isinstance(keyid, int):
                cls._node = si.session.query(si.Node).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=3)
                name = kwargs.get('name', socket.gethostname() if args[0] is None else args[0])
                int_ip = kwargs.get('int_ip', '' if args[1] is None else args[1])
                ext_ip = kwargs.get('ext_ip', '' if args[2] is None else args[2])
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
        wpipe_to_sqlintf_connection(cls, 'Node')
        return cls._inst

    def __init__(self, *args, **kwargs):
        if not hasattr(self, '_jobs_proxy'):
            self._jobs_proxy = ChildrenProxy(self._node, 'jobs', 'Job',
                                             child_attr='id')
        self._node.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Node).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        return

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

    def job(self, *args, **kwargs):
        from .Job import Job
        return Job(self, *args, **kwargs)
