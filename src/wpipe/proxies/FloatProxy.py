#!/usr/bin/env python
"""
Contains the FloatProxy class definition

Please note that this module is private and should only be used internally to
the schedulers subpackage.
"""
from .NumberProxy import NumberProxy

__all__ = ['FloatProxy']


class FloatProxy(NumberProxy, float):
    """
        Class inherited from NumberProxy meant to proxy entries from the
        database that represent float values. Behaves as a float object.
    """
    pass
