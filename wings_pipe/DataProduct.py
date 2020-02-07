import pandas as pd
import numpy as np
import time

from wings_pipe import Configuration
from wings_pipe import Store
from wings_pipe import Options
from wings_pipe.wpipe import update_time

class DataProduct():
    def __init__(self, filename='any', relativepath='', group='',
                 configuration=Configuration().new(),
                 data_type='', subtype='', filtername='',
                 ra=0, dec=0, pointing_angle=0):
        self.config_id = np.array([int(configuration.config_id)])
        self.target_id = np.array([int(configuration.target_id)])
        self.pipeline_id = np.array([int(configuration.pipeline_id)])
        self.dp_id = np.array([int(0)])

        self.filename = np.array([str(filename)])
        self.relativepath = np.array([str(relativepath)])

        _suffix = ' '
        if '.' in filename:
            _suffix = filename.split('.')[-1]
        if _suffix not in ['fits', 'txt', 'head', 'cl',
                           'py', 'pyc', 'pl', 'phot', 'png', 'jpg', 'ps',
                           'gz', 'dat', 'lst', 'sh']:
            _suffix = 'other'
        self.suffix = np.array([str(_suffix)])

        if not (data_type): data_type = _suffix
        self.data_type = np.array([str(data_type)])
        self.subtype = np.array([str(subtype)])

        if group not in ['proc', 'conf', 'log', 'raw']:
            group = 'other'
        self.group = np.array([str('other')])

        self.filtername = np.array([str(filtername)])
        self.ra = np.array([float(ra)])
        self.dec = np.array([float(dec)])
        self.pointing_angle = np.array([float(pointing_angle)])
        # self.tags = Options(tags) # meant to break
        self.timestamp = pd.to_datetime(time.time(), unit='s')
        return None

    def new(self):
        _df = pd.DataFrame.from_dict(self.__dict__)
        _df.index = pd.MultiIndex.from_arrays(arrays=[np.array([int(self.pipeline_id)]),
                                                      np.array([int(self.target_id)]), np.array([int(self.config_id)]),
                                                      np.array([int(self.dp_id)])],
                                              names=('pipelineID', 'targetID', 'configID', 'dpID'))
        return update_time(_df)

    def create(self, options={'any': 0}, ret_opt=False, store=Store()):
        _df = store.create('data_products', 'dp_id', self)
        _opt = Options(options).create('data_product', int(_df.dp_id), store=store)
        if ret_opt:
            return _df, _opt
        else:
            return _df

    def get(dp_id, store=Store()):
        x = store.select('data_products', 'dp_id==' + str(dp_id))
        return x.loc[x.index.values[0]]