#!/usr/bin/env python
"""
Contains the sqlintf.DPOwner class definition

Please note that this module is private. The sqlintf.DPOwner class is
available in the ``wpipe.sqlintf`` namespace - use that instead.
"""
from .core import sa, orm, Base

__all__ = ['DPOwner']


class DPOwner(Base):
    """
        A DPOwner object represents a row of the `dpowners` table.

        DO NOT USE CONSTRUCTOR: constructing a DPOwner object adds a new row
        to the database: USE INSTEAD ITS WPIPE COUNTERPART.
    """
    __tablename__ = 'dpowners'
    id = sa.Column(sa.Integer, primary_key=True)
    timestamp = sa.Column(sa.TIMESTAMP)
    type = sa.Column(sa.String(256))
    dataproducts = orm.relationship("DataProduct", back_populates="dpowner")
    __mapper_args__ = {
        'polymorphic_identity': 'dpowner',
        'polymorphic_on': type
    }

