from .core import *


class Mask:
    def __new__(cls, *args, **kwargs):
        # checking if given argument is sqlintf object or existing id
        cls._mask = args[0] if len(args) else None
        if not isinstance(cls._mask, si.Mask):
            keyid = kwargs.get('id', cls._mask)
            if isinstance(keyid, int):
                cls._mask = si.session.query(si.Mask).filter_by(id=keyid).one()
            else:
                # gathering construction arguments
                wpargs, args, kwargs = initialize_args(args, kwargs, nargs=3)
                task = kwargs.get('task', wpargs.get('Task', None))
                name = kwargs.get('name', args[0])
                source = kwargs.get('source', '' if args[1] is None else args[1])
                value = kwargs.get('value', '' if args[2] is None else args[2])
                # querying the database for existing row or create
                try:
                    cls._mask = si.session.query(si.Mask). \
                        filter_by(task_id=task.task_id). \
                        filter_by(name=name).one()
                except si.orm.exc.NoResultFound:
                    cls._mask = si.Mask(name=name,
                                        source=source,
                                        value=value)
                    task._task.masks.append(cls._mask)
        # verifying if instance already exists and return
        wpipe_to_sqlintf_connection(cls, 'Mask')
        return cls._inst

    def __init__(self, *args, **kwargs):
        self._mask.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @classmethod
    def select(cls, **kwargs):
        cls._temp = si.session.query(si.Mask).filter_by(**kwargs)
        return list(map(cls, cls._temp.all()))

    @property
    def parents(self):
        return self.task

    @property
    def name(self):
        si.session.commit()
        return self._mask.name

    @name.setter
    def name(self, name):
        self._mask.name = name
        self._mask.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def mask_id(self):
        si.session.commit()
        return self._mask.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._mask.timestamp

    @property
    def source(self):
        si.session.commit()
        return self._mask.source

    @property
    def value(self):
        si.session.commit()
        return self._mask.value

    @property
    def task_id(self):
        si.session.commit()
        return self._mask.task_id

    @property
    def task(self):
        if hasattr(self._mask.task, '_wpipe_object'):
            return self._mask.task._wpipe_object
        else:
            from .Task import Task
            return Task(self._mask.task)
