#!/usr/bin/env python
"""
Contains the core import statements and developing tools of Wpipe

Please note that this module is private. All functions and objects
are available in the main ``wpipe`` namespace - use that instead.
"""
import importlib
import os
import sys
import types
import datetime
import time
import subprocess
import glob
import shutil
import warnings
import json
import ast
import atexit

import numpy as np
import pandas as pd

from . import sqlintf as si

__all__ = ['importlib', 'os', 'sys', 'types', 'datetime', 'time', 'subprocess',
           'glob', 'shutil', 'warnings', 'json', 'ast', 'atexit', 'np', 'pd',
           'si', 'PARSER', 'as_int', 'try_scalar', 'clean_path', 'split_path',
           'remove_path', 'key_wpipe_separator', 'initialize_args',
           'wpipe_to_sqlintf_connection', 'return_dict_of_attrs', 'to_json',
           'ChildrenProxy', 'DictLikeChildrenProxy']

PARSER = si.PARSER  # argparse.ArgumentParser()
PARSER.add_argument('--user', '-u', dest='user_name', type=str,
                    default=os.environ['WPIPE_USER'] if 'WPIPE_USER' in os.environ.keys()
                    else [warnings.warn("Set environment variable $WPIPE_USER to associate a default username"),
                          'default'][1],
                    help='Name of user - default to WPIPE_USER environment variable')
PARSER.add_argument('--pipeline', '-p', dest='pipeline', type=str, default=os.getcwd(),
                    help='Path or ID of pipeline - default to current working directory')
PARSER.add_argument('--job', '-j', dest='job_id', type=int, help='ID of this job')
PARSER.add_argument('--event', '-e', dest='event_id', type=int, help='ID of the firing event of this job')

# if os.getcwd() not in map(os.path.abspath, sys.path):
#     sys.path.insert(0, os.getcwd())

pd.set_option('io.hdf.default_format', 'table')


def as_int(string):
    """
    Returns given string parameter as integer, or return as string.

    Parameters
    ----------
    string : string
        Input string parameter.

    Returns
    -------
    value : int or string
        Integer conversion from string or string itself.
    """
    try:
        return int(string)
    except (ValueError, TypeError):
        return string


def try_scalar(string):
    """
    Returns given string parameter as scalar value, or return as string.

    Parameters
    ----------
    string : string
        Input string parameter.

    Returns
    -------
    value
        Scalar conversion from string or string itself.
    """
    try:
        return ast.literal_eval(string)
    except (ValueError, NameError, SyntaxError):
        return string


def clean_path(path, root=''):
    """
    Returns given path in absolute format expanding variables and user flags.

    Parameters
    ----------
    path : string
        Path to clean.
    root : string
        Alternative root for converting relative path.

    Returns
    -------
    out : string
        Converted path
    """
    if path is not None:
        path = os.path.expandvars(os.path.expanduser(path))
        root = os.path.expandvars(os.path.expanduser(root))
        return os.path.abspath([root + '/', ''][os.path.isabs(path) or not (os.path.isabs(root))] + path)


def split_path(path):
    """
    Returns split path between its base, filename and file extension.

    Parameters
    ----------
    path : string
        Path to split.

    Returns
    -------
    base : string
        Base of the path.
    name : string
        Filename of the path.
    ext : string
        File extension of the path.
    """
    path, ext = os.path.splitext(path)
    base, name = os.path.split(path)
    return base, name, ext


def remove_path(path, hard=False):
    """
    Remove file or directory located at path.

    Parameters
    ----------
    path : string
        Path where file or directory is located
    hard : boolean
        Flag to proceed to a hard tree removal of a directory - defaults to
        False.
    """
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.isdir(path):
        if hard:
            shutil.rmtree(path)
        else:
            try:
                os.rmdir(path)
            except OSError:
                warnings.warn("Cannot remove directory %s : directory is not empty." % path)


def key_wpipe_separator(obj):
    """
    Returns true if given object is a Wpipe object.

    Parameters
    ----------
    obj
        Input object.

    Returns
    -------
    out : bool
        True if obj is a Wpipe object, False otherwise.
    """
    return type(obj).__module__.split('.')[0] != 'wpipe'


def initialize_args(args, kwargs, nargs):
    """
    Special function for Wpipe object constructor args and kwargs.

    Parameters
    ----------
    args : tuple
        Tuple of constructor args.
    kwargs : dict
        Dictionary of constructor kwargs.
    nargs : int
        Maximum total number of (args, kwargs).

    Returns
    -------
    wpargs : dict
        Special kwargs for Wpipe objects.
    args : tuple
        Input args depleted of its Wpipe objects and appended with None
        entries, up to length nargs. TODO
    kwargs : dict
        Input kwargs depleted of its Wpipe objects.
    """
    wpargs = sorted(args, key=key_wpipe_separator)
    args = list(wpargs.pop() for _i in range(len(wpargs)) if key_wpipe_separator(wpargs[-1]))[::-1]
    wpargs = dict((type(wparg).__name__, wparg) for wparg in wpargs)
    kwargs = dict((key, item) for key, item in kwargs.items() if item is not None)
    args += max(nargs - len(args), 0) * [None]
    return wpargs, args, kwargs


def wpipe_to_sqlintf_connection(cls, cls_name):
    """
    Special function for Wpipe object constructor instantiation in __new__.

    Parameters
    ----------
    cls
        Instance in __new__.
    cls_name : string
        Name of the class of the Wpipe object to be instantiated.
    """
    cls_attr = '_' + cls_name.lower()
    if hasattr(getattr(cls, cls_attr), '_wpipe_object'):
        cls._inst = getattr(cls, cls_attr)._wpipe_object
    else:
        cls._inst = super(getattr(sys.modules['wpipe'], cls_name), cls).__new__(cls)
        getattr(cls, cls_attr)._wpipe_object = cls._inst
        setattr(cls._inst, cls_attr, getattr(cls, cls_attr))


def return_dict_of_attrs(obj):
    """
    Returns dictionary of attributes of object that are not private or None,
    and which top namespace is not sqlalchemy or wpipe.

    Parameters
    ----------
    obj
        Input object.

    Returns
    -------
    attrs : dict
        Dictionary of attributes of obj.
    """
    si.commit()
    return dict((attr, getattr(obj, attr))
                for attr in dir(obj) if attr[0] != '_'
                and getattr(obj, attr) is not None
                and type(getattr(obj, attr)).__module__.split('.')[0]
                not in ['sqlalchemy', 'wpipe'])


def to_json(obj, *args, **kwargs):
    """
    Convert the object dictionary of attributes to a JSON string.

    Parameters
    ----------
    obj
        Input object.
    args, kwargs
        Refer to :meth:`pandas.DataFrame.to_json` for parameters
    """
    si.commit()
    pd.DataFrame(return_dict_of_attrs(obj),
                 index=[0]).to_json(*args, **kwargs)


class ChildrenProxy:
    """
        Proxy to access children of a Wpipe object.

        Parameters
        ----------
        parent : sqlintf object
            Parent sqlintf object of Wpipe object associated to this proxy.
        children_attr : string
            Attribute of the sqlintf object returning its children.
        cls_name : string
            Name of the class of the children.
        child_attr : string
            Child attribute that distinguishes the children from one another.
    """

    def __init__(self, parent, children_attr, cls_name, child_attr='name'):
        self._parent = parent
        self._children_attr = children_attr
        self._cls_name = cls_name
        self._child_attr = child_attr

    def __repr__(self):
        si.commit()
        return 'Children(' + ', '.join(
            map(lambda child: self._cls_name + '(' + repr(getattr(child, self._child_attr)) + ')',
                self.children)) + ')'

    def __len__(self):
        si.commit()
        return len(self.children)

    def __iter__(self):
        self.n = 0
        self.len = len(self)  # CURRENTLY NEEDED FOR DELETION ISSUES
        return self

    def __next__(self):
        self.n -= self.len - len(self)  # CURRENTLY NEEDED FOR DELETION ISSUES
        self.len = len(self)  # CURRENTLY NEEDED FOR DELETION ISSUES
        if 0 <= self.n < self.len:
            result = self[self.n]
            self.n += 1
            return result
        else:
            del self.n, self.len
            raise StopIteration

    def __getitem__(self, item):
        if np.ndim(item) == 0:
            si.commit()
            if hasattr(self.children[item], '_wpipe_object'):
                return self.children[item]._wpipe_object
            else:
                return getattr(sys.modules['wpipe'], self._cls_name)(self.children[item])
        else:
            return np.array([self[i] for i in range(len(self))])[item].tolist()

    def __getattr__(self, item):
        if hasattr(getattr(sys.modules['wpipe'], self._cls_name), item):
            return np.array([getattr(self[i], item) for i in range(len(self))])

    @property
    def children(self):
        return getattr(self._parent, self._children_attr)


class DictLikeChildrenProxy(ChildrenProxy):
    """
        Proxy to access children of a Wpipe object in a dictionary format.

        Parameters
        ----------
        parent : sqlintf object
            Parent sqlintf object of Wpipe object associated to this proxy.
        children_attr : string
            Attribute of the sqlintf object returning its children.
        cls_name : string
            Name of the class of the children.
        child_attr : string
            Child attribute that distinguishes the children from one another.
        child_value : string
            Child attribute that returns its stored value.
    """

    def __init__(self, parent, children_attr, cls_name, child_attr='name', child_value='value'):
        super(DictLikeChildrenProxy, self).__init__(parent, children_attr, cls_name, child_attr)
        self._child_value = child_value

    def __repr__(self):
        si.commit()
        return repr(dict(self._items))

    def __getitem__(self, item):
        si.commit()
        if isinstance(item, int):
            return super(DictLikeChildrenProxy, self).__getitem__(item)
        _temp = self._items
        try:
            key = val = None
            while key != item:
                key, val = next(_temp)
            return try_scalar(val)
        except StopIteration:
            raise KeyError(item)

    def __setitem__(self, item, value):
        si.commit()
        _temp = self._items
        try:
            key = None
            count = -1
            while key != item:
                key, val = next(_temp)
                count += 1
            child = self.children[count]
            setattr(child, self._child_value, value)
        except StopIteration:
            raise KeyError(item)

    @property
    def _items(self):
        return map(lambda child: (getattr(child, self._child_attr), getattr(child, self._child_value)), self.children)
