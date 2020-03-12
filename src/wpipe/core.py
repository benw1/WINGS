#! /usr/bin/env python
import time
import datetime
import subprocess
import os
import shutil
import json
import warnings
import numpy as np
import pandas as pd
from . import sqlintf as si

PARSER = si.PARSER
PARSER.add_argument('--user', '-u', dest='user_name', type=str,
                    default=os.environ['WPIPE_USER'] if 'WPIPE_USER' in os.environ.keys()
                    else warnings.warn("Set environment variable $WPIPE_USER to associate a default username"),
                    help='Name of user creating this pipeline')

if os.getcwd() not in map(os.path.abspath, os.sys.path):
    os.sys.path.insert(0, os.getcwd())

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


def clean_path(path, root=''):
    path = os.path.expandvars(os.path.expanduser(path))
    root = os.path.expandvars(os.path.expanduser(root))
    return os.path.abspath([root+'/', ''][os.path.isabs(path) or not(os.path.isabs(root))]+path)


def key_wpipe_separator(obj):
    return type(obj).__module__.split('.')[0] != 'wpipe'


def initialize_args(args, kwargs, nargs):
    wpargs = sorted(args, key=key_wpipe_separator)
    args = list(wpargs.pop() for i in range(len(wpargs)) if key_wpipe_separator(wpargs[-1]))[::-1]
    wpargs = dict((type(wparg).__name__.replace('SQL', ''), wparg) for wparg in wpargs)
    kwargs = dict((key,item) for key,item in kwargs.items() if item is not None)
    args += max(nargs-len(args), 0)*[None]
    return wpargs, args, kwargs


def wpipe_to_sqlintf_connection(cls, cls_name):
    cls_attr = '_' + cls_name.lower()
    if hasattr(getattr(cls, cls_attr), '_wpipe_object'):
        cls._inst = getattr(cls, cls_attr)._wpipe_object
    else:
        cls._inst = super(getattr(os.sys.modules['wpipe'], 'SQL' + cls_name), cls).__new__(cls)
        getattr(cls, cls_attr)._wpipe_object = cls._inst
        setattr(cls._inst, cls_attr, getattr(cls, cls_attr))


class ChildrenProxy:
    def __init__(self, parent, children_attr, cls_name, child_attr='name'):
        self._parent = parent
        self._children_attr = children_attr
        self._cls_name = cls_name
        self._child_attr = child_attr

    def __repr__(self):
        si.session.commit()
        return 'Children(' + ', '.join(
            map(lambda child: self._cls_name + '(' + repr(getattr(child, self._child_attr)) + ')',
                self.children)) + ')'

    def __len__(self):
        si.session.commit()
        return len(self.children)

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __getitem__(self, item):
        si.session.commit()
        if hasattr(self.children[item], '_wpipe_object'):
            return self.children[item]._wpipe_object
        else:
            return getattr(os.sys.modules['wpipe'], 'SQL' + self._cls_name)(self.children[item])

    @property
    def children(self):
        return getattr(self._parent, self._children_attr)


class DictLikeChildrenProxy(ChildrenProxy):
    def __init__(self, parent, children_attr, cls_name, child_attr='name', child_value='value'):
        super(DictLikeChildrenProxy, self).__init__(parent, children_attr, cls_name, child_attr)
        self._child_value = child_value

    def __repr__(self):
        si.session.commit()
        return dict(map(lambda child: (getattr(child, self._child_attr), getattr(child, self._child_value)),
                        self.children))
