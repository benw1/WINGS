#! /usr/bin/env python
import time, datetime, subprocess, os
import numpy as np
import pandas as pd
from . import sqlintf as si

pd.set_option('io.hdf.default_format', 'table')

try:
    path_to_store = os.path.dirname(__file__)+'/h5data/wpipe_store.h5'
except NameError:
    path_to_store = 'src/wpipe/h5data/wpipe_store.h5'

def update_time(x):
    x.timestamp = pd.to_datetime(time.time(), unit='s')
    return x

def increment(df, x):
    df[x] = int(df[x]) + 1
    return df


def fmin_itemsize(x):
    min_itemsize = {}
    for k, _dt in dict(x.dtypes).items():
        if _dt is np.dtype('O'):
            min_itemsize[k] = int(256)
    return min_itemsize

