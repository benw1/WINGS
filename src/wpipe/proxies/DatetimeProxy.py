#!/usr/bin/env python
"""
Contains the DatetimeProxy class definition

Please note that this module is private and should only be used internally to
the schedulers subpackage.
"""
from .core import datetime
from .BaseProxy import BaseProxy

__all__ = ['DatetimeProxy']


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
