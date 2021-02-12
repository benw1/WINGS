#!/usr/bin/env python
"""
Contains the StrNumProxy class definition

Please note that this module is private and should only be used internally to
the schedulers subpackage.
"""
from .core import numbers
from .BaseProxy import BaseProxy

__all__ = ['StrNumProxy']


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
                from . import StringProxy
                cls = StringProxy
            elif isinstance(proxy, numbers.Number):
                from . import NumberProxy
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
