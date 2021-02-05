#!/usr/bin/env python
"""
Contains the OptOwner class definition

Please note that this module is private. The OptOwnwer class is not meant to
be used by itself, but through its inherited classes Target, Job, Event and
DataProduct.
"""
from .core import datetime, si
from .core import DictLikeChildrenProxy
from .core import in_session
from .core import split_path
from .Option import Option

__all__ = ['OptOwner']


def _in_session(**local_kw):
    return in_session('_%s' %  split_path(__file__)[1].lower(), **local_kw)


class OptOwner:
    """
        Represents an option owner.

        The OptOwner class is a special base class from which are inherited
        the 4 classes Target, Job, Event and DataProduct to give them the
        capability to parent options. Please refer to their respective
        documentation for specific instructions.
    """
    @_in_session()
    def __init__(self, options):
        if not hasattr(self, '_optowner'):
            self._optowner = si.OptOwner()
        if not hasattr(self, '_options_proxy'):
            self._options_proxy = DictLikeChildrenProxy(self._optowner, 'options', 'Option')
        self.options = options
        self._optowner.timestamp = datetime.datetime.utcnow()
        self._session.commit()

    @property
    @_in_session()
    def optowner_id(self):
        """
        int: Points to attribute target_id/dp_id/job_id/event_id depending on
        type of optowner.
        """
        return self._optowner.id

    @property
    @_in_session()
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        self._session.refresh(self._optowner)
        return self._optowner.timestamp

    @property
    def options(self):
        """
        :obj:`core.DictLikeChildrenProxy`: Dictionary of Option objects owned
        by the optowner.
        """
        return self._options_proxy

    @options.setter
    def options(self, options):
        for key, value in options.items():
            self.option(name=key, value=value)

    def option(self, *args, **kwargs):
        """
        Returns an option owned by the optowner.

        Parameters
        ----------
        kwargs
            Refer to :class:`Option` for parameters.

        Returns
        -------
        option : :obj:`Option`
            Option corresponding to given kwargs.
        """
        return Option(self, *args, **kwargs)

    def delete(self):
        """
        Delete corresponding row from the database.
        """
        self.options.delete()
        si.delete(self._optowner)
