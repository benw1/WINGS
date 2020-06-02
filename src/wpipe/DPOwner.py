#!/usr/bin/env python
"""
Contains the DPOwner class definition

Please note that this module is private. The DPOwnwer class is not meant to
be used by itself, but through its inherited classes Input and Configurations.
"""
from .core import datetime, si
from .core import ChildrenProxy


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
        si.session.commit()
        return self._dpowner.id

    @property
    def timestamp(self):
        si.session.commit()
        return self._dpowner.timestamp

    @property
    def rawdataproducts(self):
        return self.dataproducts_of_group('raw')

    @property
    def confdataproducts(self):
        return self.dataproducts_of_group('conf')

    @property
    def logdataproducts(self):
        return self.dataproducts_of_group('log')

    @property
    def procdataproducts(self):
        return self.dataproducts_of_group('proc')

    @property
    def dataproducts(self):
        return self._dataproducts_proxy

    def dataproducts_of_group(self, group):
        return self.dataproducts[self.dataproducts.group == group]

    def dataproduct(self, *args, **kwargs):
        from .DataProduct import DataProduct
        return DataProduct(self, *args, **kwargs)
