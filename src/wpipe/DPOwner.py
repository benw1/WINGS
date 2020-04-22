from .core import *


class DPOwner:
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
