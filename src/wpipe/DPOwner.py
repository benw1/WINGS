#!/usr/bin/env python
"""
Contains the DPOwner class definition

Please note that this module is private. The DPOwnwer class is not meant to
be used by itself, but through its inherited classes Pipeline, Input and
Configuration.
"""
from .core import datetime, si
from .core import in_session
from .core import split_path
from .proxies import ChildrenProxy

__all__ = ['DPOwner']

CLASS_LOW = split_path(__file__)[1].lower()


def _in_session(**local_kw):
    return in_session('_%s' % CLASS_LOW, **local_kw)


class DPOwner:
    """
        Represents a dataproduct owner.

        The DPOwner class is a special base class from which are inherited
        the 3 classes Pipeline, Input and Configuration to give them the
        capability to parent dataproducts. Please refer to their respective
        documentation for specific instructions.
    """
    # @_in_session()
    def __init__(self):
        if not hasattr(self, '_dpowner'):
            self._dpowner = si.DPOwner()
        if not hasattr(self, '_dataproducts_proxy'):
            self._dataproducts_proxy = ChildrenProxy(self._dpowner, 'dataproducts', 'DataProduct',
                                                     child_attr='filename')
        # self.update_timestamp()

    @property
    @_in_session()
    def dpowner_id(self):
        """
        int: Points to attribute pipeline_id/input_id/config_id depending on
        type of dpowner.
        """
        return self._dpowner.id

    @property
    @_in_session()
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        self._session.refresh(self._dpowner)
        return self._dpowner.timestamp

    @property
    def rawdataproducts(self):
        """
        list of :obj:`DataProduct`: List of owned DataProduct objects
        corresponding to raw data files.
        """
        return self.dataproducts_of_group('raw')

    @property
    def confdataproducts(self):
        """
        list of :obj:`DataProduct`: List of owned DataProduct objects
        corresponding to configuration files.
        """
        return self.dataproducts_of_group('conf')

    @property
    def logdataproducts(self):
        """
        list of :obj:`DataProduct`: List of owned DataProduct objects
        corresponding to logging files.
        """
        return self.dataproducts_of_group('log')

    @property
    def procdataproducts(self):
        """
        list of :obj:`DataProduct`: List of owned DataProduct objects
        corresponding to processed data files.
        """
        return self.dataproducts_of_group('proc')

    @property
    def dataproducts(self):
        """
        :obj:`core.ChildrenProxy`: List of owned DataProduct objects.
        """
        return self._dataproducts_proxy

    def dataproducts_of_group(self, group):
        """
        Returns a list of owned DataProduct object with given group.

        Parameters
        ----------
        group : str
            Group to filter owned dataproducts.

        Returns
        -------
        dataproducts : list of :obj:`DataProduct`
            Filtered list of dataproducts.
        """
        with self.dataproducts.hold_structure():
            return self.dataproducts[self.dataproducts.group == group]

    def dataproduct(self, *args, **kwargs):
        """
        Returns a dataproduct owned by the dpowner.

        Parameters
        ----------
        kwargs
            Refer to :class:`DataProduct` for parameters.

        Returns
        -------
        dataproduct : :obj:`DataProduct`
            DataProduct corresponding to given kwargs.
        """
        from .DataProduct import DataProduct
        return DataProduct(self, *args, **kwargs)

    @_in_session()
    def update_timestamp(self):
        """

        """
        self._dpowner.timestamp = datetime.datetime.utcnow()
        self._session.commit()

    def delete(self, *predeletes):
        """
        Delete corresponding row from the database.
        """
        self.dataproducts.delete()
        if predeletes:
            for predel in predeletes:
                predel()
        si.delete(self._dpowner)
