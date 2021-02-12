#!/usr/bin/env python
"""
Contains the StringProxy class definition

Please note that this module is private and should only be used internally to
the schedulers subpackage.
"""
from .StrNumProxy import StrNumProxy

__all__ = ['StringProxy']


class StringProxy(StrNumProxy, str):
    """
        Class inherited from StrNumProxy meant to proxy entries from the
        database that represent string values. Behaves as a string object.
    """
    pass
