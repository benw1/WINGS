from .core import *
from .Store import Store
from .Task import Task, SQLTask

class Mask():
    def __init__(self, task=Task().new(), source='', name='', value=''):
        self.source = np.array([str(source)])
        self.name = np.array([str(name)])
        self.value = np.array([str(value)])
        self.task_id = np.array([int(task.task_id)])
        self.mask_id = np.array([int(0)])
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.mask_id
        return update_time(_df)

    def create(self, store=Store()):
        return store.create('masks', 'mask_id', self)

    def get(mask_id, store=Store()):
        return store.select('masks').loc[int(mask_id)]


class SQLMask:
    def __init__(self, task, name, source='', value=''):
        try:
            self._mask = si.session.query(si.Mask). \
                filter_by(task_id=task.task_id). \
                filter_by(name=name).one()
        except si.orm.exc.NoResultFound:
            self._mask = si.Mask(name=name,
                                 source=source,
                                 value=value)
            task._task.masks.append(self._mask)
        self._mask.timestamp = datetime.datetime.utcnow()
        si.session.commit()

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
