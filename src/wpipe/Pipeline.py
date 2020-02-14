from .core import *
from .Store import Store
from .User import User

class Pipeline():
    def __init__(self, user=User().new(), name='any', software_root='',
                 data_root='', pipe_root='', config_root='',
                 description=''):
        self.name = np.array([str(name)])
        self.user_name = np.array([str(user['name'])])
        self.user_id = np.array([int(user.user_id)])
        self.pipeline_id = np.array([int(0)])
        self.software_root = np.array([str(software_root)])
        self.data_root = np.array([str(data_root)])
        self.pipe_root = np.array([str(pipe_root)])
        self.config_root = np.array([str(config_root)])
        self.description = np.array([str(description)])
        self.timestamp = pd.to_datetime(time.time(), unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = _df.pipeline_id
        return update_time(_df)

    def create(self, store=Store()):
        _df = store.create('pipelines', 'pipeline_id', self)
        return _df

    def get(pipeline_id, store=Store()):
        return store.select('pipelines').loc[int(pipeline_id)]