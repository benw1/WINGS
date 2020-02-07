import pandas as pd
import numpy as np
import time

from wings_pipe import Parameters, Store, Target
from wings_pipe.wpipe import update_time

class Configuration():
    def __init__(self, name='', description='',
                 target=Target().new()):
        self.name = np.array([str(name)])
        self.relativepath = np.array([str(target.relativepath[0])])
        self.logpath = np.array([str(target.relativepath[0]) + '/log_' + str(name)])
        self.confpath = np.array([str(target.relativepath[0]) + '/conf_' + str(name)])
        self.rawpath = np.array([str(target.relativepath[0]) + '/raw_' + str(name)])
        self.procpath = np.array([str(target.relativepath[0]) + '/proc_' + str(name)])
        self.target_id = np.array([int(target.target_id)])
        self.pipeline_id = np.array([int(target.pipeline_id)])
        self.config_id = np.array([int(0)])
        self.description = np.array([str(description)])
        self.timestamp = pd.to_datetime(time.time(), unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                                                      np.array([int(self.target_id)]), np.array([int(self.config_id)])],
                                              names=('pipelineID', 'targetID', 'configID'))
        return update_time(_df)

    def create(self, params={'any': 0}, create_dir=False, ret_opt=False, store=Store()):
        _df = store.create('configurations', 'config_id', self)
        _params = Parameters(params).create(_df, store=store)

        if create_dir:
            for _path in [self.rawpath[0], self.confpath[0], self.procpath[0], self.logpath[0]]:
                _t = subprocess.run(['mkdir', '-p', str(_path)], stdout=subprocess.PIPE)

        if ret_opt:
            return _df, _params
        else:
            return _df

    def get(config_id, store=Store()):
        x = store.select('configurations', 'config_id==' + str(config_id))
        return x.loc[x.index.values[0]]
