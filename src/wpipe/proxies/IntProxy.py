#!/usr/bin/env python
"""
Contains the IntProxy class definition

Please note that this module is private and should only be used internally to
the schedulers subpackage.
"""
from .NumberProxy import NumberProxy

__all__ = ['IntProxy']


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
