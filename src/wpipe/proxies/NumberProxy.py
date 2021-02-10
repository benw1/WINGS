#!/usr/bin/env python
"""
Contains the NumberProxy class definition

Please note that this module is private and should only be used internally to
the schedulers subpackage.
"""
from .StrNumProxy import StrNumProxy

__all__ = ['NumberProxy']


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
                from . import IntProxy
                cls = IntProxy
            elif isinstance(proxy, float):
                from . import FloatProxy
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
