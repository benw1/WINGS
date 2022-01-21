#!/usr/bin/env python
"""
Contains the BaseScheduler abstract class definition for all inherited
scheduler classes

Please note that this module is private and should only be used internally to
the schedulers subpackage.
"""
import abc
import threading


class BaseScheduler(abc.ABC):
    """
    Inherit this abstract class for a non-blocking timer.  Call reset() to
    reset the timer.

    Override the abstract method _execute for the timer to call.
    """

    def __init__(self, timer=20):
        self._timer = timer
        self._reset = False

        threading.Timer(self._timer, self._middleman).start()

    def _middleman(self):
        if self._reset:
            self._reset = False
            threading.Timer(self._timer, self._middleman).start()
        else:
            self._execute()

    @abc.abstractmethod
    def _execute(self):
        pass

    def reset(self):
        self._reset = True
