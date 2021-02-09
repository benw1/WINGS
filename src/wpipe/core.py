#!/usr/bin/env python
"""
Contains the core import statements and developing tools of Wpipe

Please note that this module is private. All functions and objects
are available in the main ``wpipe`` namespace - use that instead.
"""
import importlib
import contextlib
import os
import sys
import types
import itertools
import numbers
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

__all__ = ['importlib', 'os', 'sys', 'types', 'datetime', 'time', 'subprocess',
           'logging', 'glob', 'shutil', 'warnings', 'json', 'ast', 'atexit',
           'np', 'pd', 'si', 'PARSER', 'as_int', 'try_scalar', 'clean_path',
           'split_path', 'remove_path', 'key_wpipe_separator',
           'initialize_args', 'wpipe_to_sqlintf_connection', 'in_session',
           'return_dict_of_attrs', 'to_json',
           'BaseProxy', 'ChildrenProxy', 'DictLikeChildrenProxy']

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
        setattr(cls._inst, '_session', None)


def in_session(si_attr, **local_kw):
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

    local_kw : dict
        TODO

    Returns
    -------
    decor :
        Decorator to be used.
    """
    def decor(func):
        def wrapper(self_cls, *args, **kwargs):
            with si.begin_session(**local_kw) as session:
                _temp = self_cls._session
                self_cls._session = session
                session.add(getattr(self_cls, si_attr))
                output = func(self_cls, *args, **kwargs)
                self_cls._session = _temp
                return output

        return wrapper

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
    with si.begin_session() as session:
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


class BaseProxy:
    """
        Parent class of all proxy classes

        Parameters
        ----------
        parents : sqlintf.Base object
            TODO
        attr_name : string
            TODO
        try_scalar : boolean
            TODO

        Attributes
        ----------
        parents : sqlintf.Base object
            TODO
        parent_id : int
            TODO
        attr_name : string
            TODO
        try_scalar : boolean
            TODO
    """
    def __new__(cls, *args, **kwargs):
        if cls is BaseProxy:
            proxy = getattr(kwargs.pop('parent', None),
                            kwargs.pop('attr_name', ''))
            if kwargs.pop('try_scalar', False):
                proxy = try_scalar(proxy)
            if isinstance(proxy, str) or isinstance(proxy, numbers.Number):
                cls = StrNumProxy
            elif isinstance(proxy, datetime.datetime):
                cls = DatetimeProxy
            else:
                raise ValueError("Invalid proxy type %s" % type(proxy))
            args = [proxy]
            return cls.__new__(cls, *args, *kwargs)
        return super().__new__(cls, *args, *kwargs)

    def __init__(self, *args, **kwargs):
        self._parent = kwargs.pop('parent', None)
        self._parent_id = int(self.parent.id)
        self._attr_name = kwargs.pop('attr_name', '')
        self._try_scalar = kwargs.pop('try_scalar', False)
        self._session = None

    @property
    def parent(self):
        """
        TODO
        """
        return self._parent

    @property
    def parent_id(self):
        """
        TODO
        """
        return self._parent_id

    @property
    def attr_name(self):
        """
        TODO
        """
        return self._attr_name

    @property
    def try_scalar(self):
        """
        TODO
        """
        return self._try_scalar

    @in_session('parent')
    def _augmented_assign(self, operator, other):
        """
        TODO
        """
        for retry in self._session.retrying_nested():
            with retry:
                _temp = retry.retry_state.query(self.parent.__class__).with_for_update(). \
                    filter_by(id=self.parent_id).one()
                _result = getattr(
                    [lambda x: x, try_scalar][self._try_scalar](getattr(_temp, self.attr_name)),
                    operator)(other)
                if _result is not NotImplemented:
                    setattr(_temp, self.attr_name, _result)
                    _temp = BaseProxy(parent=self.parent,
                                      attr_name=self.attr_name,
                                      try_scalar=self.try_scalar)
                retry.retry_state.commit()
        if _result is NotImplemented:
            raise TypeError("unsupported operand type(s) for augmented assignment")
        else:
            return _temp


class StrNumProxy(BaseProxy):
    """
        Class inherited from BaseProxy and parenting the StringProxy and
        NumberProxy classes. Supports the augmented assignment methods
        __iadd__, __imul__ and __imod__.
    """
    def __new__(cls, *args, **kwargs):
        if cls is StrNumProxy:
            proxy = args[0]
            if isinstance(proxy, str):
                cls = StringProxy
            elif isinstance(proxy, numbers.Number):
                cls = NumberProxy
            else:
                raise ValueError("Invalid proxy type %s" % type(proxy))
            return cls.__new__(cls, *args, *kwargs)
        return super().__new__(cls, *args, *kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

    def __iadd__(self, other):  #
        return self._augmented_assign('__add__', other)

    def __imul__(self, other):  #
        return self._augmented_assign('__mul__', other)

    def __imod__(self, other):
        return self._augmented_assign('__mod__', other)


class StringProxy(StrNumProxy, str):
    """
        Class inherited from StrNumProxy meant to proxy entries from the
        database that represent string values. Behaves as a string object.
    """
    pass


class NumberProxy(StrNumProxy):
    """
        Class inherited from StrNumProxy and parenting the IntProxy and
        FloatProxy classes. Supports the augmented assignment methods
        __isub__, __ifloordiv__, __idiv__, __itruediv__, __ipow__, __iand__,
        __ior__ and __ixor__.
    """
    def __new__(cls, *args, **kwargs):
        if cls is NumberProxy:
            proxy = args[0]
            if isinstance(proxy, int):
                cls = IntProxy
            elif isinstance(proxy, float):
                cls = FloatProxy
            else:
                raise ValueError("Invalid proxy type %s" % type(proxy))
            return cls.__new__(cls, *args, *kwargs)
        return super().__new__(cls, *args, *kwargs)

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

    def __isub__(self, other):
        return self._augmented_assign('__sub__', other)

    def __ifloordiv__(self, other):
        return self._augmented_assign('__floordiv__', other)

    def __idiv__(self, other):
        return self._augmented_assign('__div__', other)

    def __itruediv__(self, other):
        return self._augmented_assign('__truediv__', other)

    def __ipow__(self, other):
        return self._augmented_assign('__pow__', other)

    def __iand__(self, other):
        return self._augmented_assign('__and__', other)

    def __ior__(self, other):
        return self._augmented_assign('__or__', other)

    def __ixor__(self, other):
        return self._augmented_assign('__xor__', other)


class IntProxy(NumberProxy, int):
    """
        Class inherited from NumberProxy meant to proxy entries from the
        database that represent integer values. Supports the augmented
        assignment methods __ilshift__ and __irshift__. Behaves as an integer
        object.
    """
    def __ilshift__(self, other):
        return self._augmented_assign('__lshift__', other)

    def __irshift__(self, other):
        return self._augmented_assign('__lrhift__', other)


class FloatProxy(NumberProxy, float):
    """
        Class inherited from NumberProxy meant to proxy entries from the
        database that represent float values. Behaves as a float object.
    """
    pass


class DatetimeProxy(BaseProxy, datetime.datetime):  # TODO: __add__ and __sub__ with timedelta objects
    """
        Class inherited from BaseProxy meant to proxy entries from the
        database that represent datetime.datetime values. Behaves as a
        datetime.datetime object.
    """
    def __new__(cls, *args, **kwargs):
        if cls is DatetimeProxy:
            args = [
                args[0].year,
                args[0].month,
                args[0].day,
                args[0].hour,
                args[0].minute,
                args[0].second,
                args[0].microsecond,
                args[0].tzinfo
            ]
        return super().__new__(cls, *args, *kwargs)


class ChildrenProxy:  # TODO: Generalize proxy object with the BaseProxy
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
        self._work_with_sqlintf = 0
        self._session = None

    def __repr__(self):
        self._refresh()
        return 'Children(' + ', '.join(
            map(lambda child: self._cls_name + '(' + repr(getattr(child, self._child_attr)) + ')',
                self.children)) + ')'

    @in_session('_parent')
    def __len__(self):
        self._refresh()
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

    @in_session('_parent')
    def __getitem__(self, item):
        if np.ndim(item) == 0:
            if self._work_with_sqlintf == 0:
                self._refresh()
                if hasattr(self.children[item], '_wpipe_object'):  # TODO: Unnecessary
                    return self.children[item]._wpipe_object
                else:
                    return getattr(sys.modules['wpipe'], self._cls_name)(self.children[item])
            else:
                return self.children[item]
        else:
            return [self[i] for i in np.arange(len(self))[item]]

    @in_session('_parent')
    def __getattr__(self, item):
        if hasattr(getattr(sys.modules['wpipe'], self._cls_name), item):
            self._refresh()
            with self._with_sqlintf():
                return np.array([getattr(self[i], item) for i in range(len(self))])

    @property
    def children(self):
        return getattr(self._parent, self._children_attr)

    @in_session('_parent')
    def delete(self):
        while len(self):
            self[0].delete()

    @contextlib.contextmanager
    def _with_sqlintf(self):
        self._work_with_sqlintf += 1
        try:
            yield
        finally:
            self._work_with_sqlintf -= 1

    @in_session('_parent')
    def _refresh(self, **kwargs):
        if self._work_with_sqlintf == 0:
            self._session.refresh(self._parent, **kwargs)
            for child in self.children:
                self._session.refresh(child, **kwargs)


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
        self._refresh()
        return repr(dict(self._items))

    @in_session('_parent')
    def __getitem__(self, item):
        if isinstance(item, int):
            return super(DictLikeChildrenProxy, self).__getitem__(item)
        self._refresh()
        _temp = self._keys_children
        try:
            key = child = None
            while key != item:
                key, child = next(_temp)
            return BaseProxy(parent=child,
                             attr_name=self._child_value,
                             try_scalar=True)
        except StopIteration:
            raiseerror = True
        if raiseerror:
            raise KeyError(item)

    @in_session('_parent')
    def __setitem__(self, item, value):
        if not isinstance(value, BaseProxy):
            self._refresh()
            _temp = self._keys_children
            try:
                key = None
                count = -1
                while key != item:
                    key, child = next(_temp)
                    count += 1
                child = self.children[count]
                setattr(child, self._child_value, value)
                self._session.commit()
            except StopIteration:
                _temp = getattr(sys.modules['wpipe'], self._cls_name)(
                    getattr(sys.modules['wpipe'], self._parent.__class__.__name__)(self._parent),
                    **{self._child_attr: item, self._child_value: value}
                )

    @property
    def _keys_children(self):
        return map(lambda child: (getattr(child, self._child_attr), child), self.children)

    @property
    def _items(self):
        return itertools.starmap(lambda key, child: (key, getattr(child, self._child_value)), self._keys_children)
