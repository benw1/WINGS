from .core import *
from .Store import Store
from .Task import Task

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
