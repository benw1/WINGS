#!/usr/bin/env python
"""
Contains the core import statements and developing tools of proxies

Please note that this module is private. All functions and objects
are available in the main ``wpipe.proxies`` namespace - use that instead.
"""
import ast
import contextlib
import sys
import itertools
import numbers
import datetime

import numpy as np

from ..core import si, in_session

__all__ = ['contextlib', 'sys', 'itertools', 'numbers', 'datetime', 'np',
           'si', 'in_session', 'try_scalar']

_try_scalar_nan_dict = {'nan': float('NaN'),
                        'inf': float('Inf'),
                        '-inf': -float('Inf')}


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
        return _try_scalar_nan_dict.get(string.lower() if isinstance(string, str) else None, string)
