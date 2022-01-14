#!/usr/bin/env python
"""
Contains the core import statements and developing tools of Wpipe

Please note that this module is private. All functions and objects
are available in the main ``wpipe`` namespace - use that instead.
"""
import importlib
import contextlib
import functools
import os
import sys
import types
import datetime
import time
import subprocess
import logging
import glob
import shutil
import warnings
import json
import ast
import atexit

import numpy as np
import pandas as pd

from . import sqlintf as si

__all__ = ['importlib', 'contextlib', 'os', 'sys', 'types', 'datetime',
           'time', 'subprocess', 'logging', 'glob', 'shutil', 'warnings',
           'json', 'ast', 'atexit', 'np', 'pd', 'si', 'PARSER', 'as_int',
           'clean_path', 'split_path', 'remove_path',
           'make_yield_session_if_not_cached', 'make_query_rtn_upd',
           'key_wpipe_separator', 'initialize_args',
           'wpipe_to_sqlintf_connection', 'in_session',
           'return_dict_of_attrs', 'to_json']

PARSER = si.PARSER
PARSER.add_argument('--user', '-u', dest='user_name', type=str,
                    default=os.environ['WPIPE_USER'] if 'WPIPE_USER' in os.environ.keys()
                    else [warnings.warn("Set environment variable $WPIPE_USER if you wish to define your username"),
                          os.environ['USER'] if 'USER' in os.environ.keys()
                          else 'default'][1],
                    help='Name of user - default to WPIPE_USER environment variable')
PARSER.add_argument('--pipeline', '-p', dest='pipeline', type=str, default=os.getcwd(),
                    help='Path or ID of pipeline - default to current working directory')
PARSER.add_argument('--job', '-j', dest='job_id', type=int, help='ID of this job')
PARSER.add_argument('--event', '-e', dest='event_id', type=int, help='ID of the firing event of this job')

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


def remove_path(*paths, hard=False):
    """
    Remove file or directory located at path.

    Parameters
    ----------
    paths : string or array_like of strings
        Paths where file or directories are located
    hard : boolean
        Flag to proceed to a hard tree removal of a directory - defaults to
        False.
    """
    if len(paths):
        if len(paths) == 1 and hasattr(paths[0], '__len__') and not isinstance(paths[0], str):
            paths = paths[0]
        for path in paths:
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
    else:
        raise TypeError('remove_path expected at least 1 arguments, get 0')


def make_yield_session_if_not_cached(keyid_attr, uniq_attrs, class_low):
    def yield_session_if_not_cached(cls, kind, loc):
        if kind not in ['keyid', 'args']:
            raise KeyError(kind)
        try:
            cls._inst = cls.__cache__.set_index({'keyid': keyid_attr, 'args': uniq_attrs}[kind]).loc[loc][class_low]
            setattr(cls, '_%s' % class_low, getattr(cls._inst, '_%s' % class_low))
        except KeyError:
            for session in si.begin_session():
                with session as session:
                    yield session
                    if hasattr(cls, '_to_cache'):
                        session.add(getattr(cls, '_%s' % class_low))
                        cls._to_cache[keyid_attr] = getattr(cls, '_%s' % class_low).id
                        for attr in uniq_attrs:
                            cls._to_cache[attr] = getattr(getattr(cls, '_%s' % class_low), attr)
    return yield_session_if_not_cached


def make_query_rtn_upd(class_low, keyid_attr, uniq_attrs):
    def query_return_and_update_cached_row(self, value_attr):
        _sqlintf = getattr(self, '_%s' % class_low)
        value = getattr(_sqlintf, value_attr)
        temp = self.__cache__[class_low] == self
        # SOMEHOW, THIS BLOC BELOW IS NECESSARY
        if temp.sum() == 0:
            keyid = _sqlintf
            temp = self.__cache__[keyid_attr] == keyid
        if temp.sum() == 0:
            _to_cache = {keyid_attr: keyid}
            for attr in uniq_attrs:
                _to_cache[attr] = getattr(_sqlintf, attr)
            _to_cache[class_low] = self
            self.__class__.__cache__.loc[len(self.__cache__)] = _to_cache
        ########################################
        elif (getattr(self.__cache__[temp], value_attr) != value).iloc[0]:
            self.__cache__.loc[temp, value_attr] = value
        return value
    return query_return_and_update_cached_row


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
    if not hasattr(cls, '_inst'):
        cls_attr = '_' + cls_name.lower()
        if hasattr(getattr(cls, cls_attr), '_wpipe_object'):
            cls._inst = getattr(cls, cls_attr)._wpipe_object
        else:
            cls._inst = super(getattr(sys.modules['wpipe'], cls_name), cls).__new__(cls)
            getattr(cls, cls_attr)._wpipe_object = cls._inst
            setattr(cls._inst, cls_attr, getattr(cls, cls_attr))
            setattr(cls._inst, '_session', None)


def in_session(si_attr, generator=False, **local_kw):
    """
    Returns a decorator that places the modified function in a begin_session
    context manager statement. The modified must be defined around a first
    positional argument which object will hold the associated BeginSession
    object as attribute '_session'. Also, it adds to the corresponding Session
    the object that is hold in the attribute designated by si_attr of that
    same object in first positional argument of the modified function.

    Parameters
    ----------
    si_attr : string
        Name of the attribute where to find the object to add to the Session.

    generator : boolean
        TODO

    local_kw : dict
        TODO

    Returns
    -------
    decor :
        Decorator to be used.
    """

    def decor(func):
        @functools.wraps(func)
        def wrapper(self_cls, *args, **kwargs):
            for session in si.begin_session(**local_kw):
                with session as session:
                    # swap session in self_cls '_session' attribute if exists
                    if hasattr(self_cls, '_session'):
                        _temp = self_cls._session
                        self_cls._session = session
                    # attempt adding existing sqlintf instance if possible otherwise query and replace
                    try:
                        session.add(getattr(self_cls, si_attr))
                    except si.exc.InvalidRequestError:
                        warnings.warn("FIXING ENCOUNTERED BROKEN INSTANCE")
                        broken_instance = getattr(self_cls, si_attr)
                        setattr(self_cls, si_attr, session.query(broken_instance.__class__).
                                filter_by(id=broken_instance._sa_instance_state.key[1][0]).one())
                        if hasattr(broken_instance, '_wpipe_object'):
                            setattr(getattr(self_cls, si_attr), '_wpipe_object', getattr(broken_instance, '_wpipe_object'))
                    # execute wrapped function
                    output = func(self_cls, *args, **kwargs)
                    # return given session in self_cls '_session' attribute if exists
                    if hasattr(self_cls, '_session'):
                        self_cls._session = _temp
                    return output

        return wrapper

    def decor_gene(func):
        @functools.wraps(func)
        def wrapper(self_cls, *args, **kwargs):
            for session in si.begin_session(**local_kw):
                with session as session:
                    # swap session in self_cls '_session' attribute if exists
                    if hasattr(self_cls, '_session'):
                        _temp = self_cls._session
                        self_cls._session = session
                    # attempt adding existing sqlintf instance if possible otherwise query and replace
                    try:
                        session.add(getattr(self_cls, si_attr))
                    except si.exc.InvalidRequestError:
                        warnings.warn("FIXING ENCOUNTERED BROKEN INSTANCE")
                        broken_instance = getattr(self_cls, si_attr)
                        setattr(self_cls, si_attr, session.query(broken_instance.__class__).
                                filter_by(id=broken_instance._sa_instance_state.key[1][0]).one())
                        if hasattr(broken_instance, '_wpipe_object'):
                            setattr(getattr(self_cls, si_attr), '_wpipe_object', getattr(broken_instance, '_wpipe_object'))
                    # execute wrapped function
                    output = yield from func(self_cls, *args, **kwargs)
                    # return given session in self_cls '_session' attribute if exists
                    if hasattr(self_cls, '_session'):
                        self_cls._session = _temp
                    return output

        return wrapper

    if generator:
        return decor_gene
    else:
        return decor


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
    for session in si.begin_session():
        with session as session:
            session.add(obj)
            session.refresh(obj)
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
    pd.DataFrame(return_dict_of_attrs(obj),
                 index=[0]).to_json(*args, **kwargs)
