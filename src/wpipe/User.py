from .core import *


class User:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._user = args[0] if len(args) else None
        if not isinstance(cls._user, si.User):
            keyid = kwargs.get('id', cls._user)
            if isinstance(keyid, int):
                cls._user = si.session.query(si.User).filter_by(id=keyid).one()
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

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.User).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

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
        from .Pipeline import Pipeline
        return Pipeline(self, *args, **kwargs)
