#!/usr/bin/env python
"""
Contains the OptOwner class definition

Please note that this module is private. The OptOwnwer class is not meant to
be used by itself, but through its inherited classes Target, Job, Event and
DataProduct.
"""
from .core import *
from .Option import Option


class OptOwner:
    """
        The OptOwner class is a special base class from which are inherited
        the 4 classes Target, Job, Event and DataProduct to give them the
        capability to parent options. Please refer to their respective
        documentation for specific instructions.
    """
    def __init__(self, options):
        if not hasattr(self, '_optowner'):
            self._optowner = si.OptOwner()
        if not hasattr(self, '_options_proxy'):
            self._options_proxy = DictLikeChildrenProxy(self._optowner, 'options', 'Option')
        self.options = options
        self._optowner.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def optowner_id(self):
        si.session.commit()
        return self._optowner.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._optowner.timestamp

    @property
    def options(self):
        return self._options_proxy

    @options.setter
    def options(self, options):
        for key, value in options.items():
            self.option(name=key, value=value)

    def option(self, *args, **kwargs):
        return Option(self, *args, **kwargs)
