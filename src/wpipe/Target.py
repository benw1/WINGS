from .core import *
from .Store import Store
from .Pipeline import Pipeline

class Target():
    def __init__(self, name='any',
                 pipeline=Pipeline().new()):
        self.name = np.array([str(name)])
        self.pipeline_id = np.array([int(pipeline.pipeline_id)])
        self.target_id = np.array([int(0)])
        myPipe = Pipeline.get(self.pipeline_id)
        self.relativepath = np.array([str(myPipe.data_root) + '/' + str(self.name[0])])
        self.timestamp = pd.to_datetime(time.time(), unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                                                      np.array([int(self.target_id)])],
                                              names=('pipelineID', 'targetID'))
        return update_time(_df)

    def create(self, options={'any': 0}, ret_opt=False, create_dir=False, store=Store()):
        from . import Options
        _df = store.create('targets', 'target_id', self)
        _opt = Options(options).create('target', int(_df.target_id), store=store)

        if create_dir:
            _t = subprocess.run(['mkdir', '-p', str(self.relativepath[0])], stdout=subprocess.PIPE)

        if ret_opt:
            return _df, _opt
        else:
            return _df

    def get(target_id, store=Store()):
        x = store.select('targets', 'target_id==' + str(target_id))
        return x.loc[x.index.values[0]]
