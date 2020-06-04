#!/usr/bin/env python
"""
Contains the DPOwner class definition

Please note that this module is private. The DPOwnwer class is not meant to
be used by itself, but through its inherited classes Input and Configurations.
"""
from .core import datetime, si
from .core import ChildrenProxy

__all__ = ['DPOwner']


class DPOwner:
    """
        Represents a dataproduct owner.

        The DPOwner class is a special base class from which are inherited
        the 2 classes Input and Configuration to give them the capability
        to parent dataproducts. Please refer to their respective documentation
        for specific instructions.
    """
    def __init__(self):
        if not hasattr(self, '_dpowner'):
            self._dpowner = si.DPOwner()
        if not hasattr(self, '_dataproducts_proxy'):
            self._dataproducts_proxy = ChildrenProxy(self._dpowner, 'dataproducts', 'DataProduct',
                                                     child_attr='filename')
        self._dpowner.timestamp = datetime.datetime.utcnow()
        si.session.commit()

    @property
    def dpowner_id(self):
        """
        int: Points to attribute input_id/config_id depending on type of
        dpowner.
        """
        return self._dpowner.id

    @property
    def timestamp(self):
        """
        :obj:`datetime.datetime`: Timestamp of last access to table row.
        """
        si.session.commit()
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
