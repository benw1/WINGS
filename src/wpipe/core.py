#! /usr/bin/env python
import time, datetime, subprocess, os
import numpy as np
import pandas as pd
from . import sqlintf as si

pd.set_option('io.hdf.default_format', 'table')

try:
    path_to_store = os.path.dirname(__file__) + '/h5data/wpipe_store.h5'
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


def wpipe_to_sqlintf_connection(cls, cls_name, __name__):
    cls_attr = '_' + cls_name.lower()
    if hasattr(getattr(cls, cls_attr), '_wpipe_object'):
        cls._inst = getattr(cls, cls_attr)._wpipe_object
    else:
        cls._inst = super(getattr(os.sys.modules[__name__], 'SQL' + cls_name), cls).__new__(cls)
        getattr(cls, cls_attr)._wpipe_object = cls._inst
        setattr(cls._inst, cls_attr, getattr(cls, cls_attr))


class ChildrenProxy:
    def __init__(self, parent, children_attr, cls_name, __name__, child_attr='name'):
        self._parent = parent
        self._children_attr = children_attr
        self._cls_name = cls_name
        self._name = __name__
        self._child_attr = child_attr

    def __repr__(self):
        return 'Children(' + ', '.join(
            map(lambda child: self._cls_name + '(' + repr(getattr(child, self._child_attr)) + ')',
                self.children)) + ')'

    def __getitem__(self, item):
        if hasattr(self.children[item], '_wpipe_object'):
            return self.children[item]._wpipe_object
        else:
            return getattr(os.sys.modules[self._name], 'SQL' + self._cls_name)(self.children[item])

    @property
    def children(self):
        return getattr(self._parent, self._children_attr)
